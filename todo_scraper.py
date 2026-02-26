import asyncio
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from db import Database
from ai_analyzer import EventAnalyzer
from image_generator import EventImageGenerator
from venue_enricher import VenueEnricher
from event_dedup import EventDedup
from label_cache import LabelCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import httpx
import os
from datetime import datetime

async def download_image(url: str, dest_dir: str = "media") -> str | None:
    if not url:
        return None
    try:
        os.makedirs(dest_dir, exist_ok=True)
        # Generate a unique filename
        filename = f"todotoday_{int(datetime.now().timestamp())}_{hash(url)}.jpg"
        filepath = os.path.join(dest_dir, filename)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(resp.content)
        return filepath
    except Exception as e:
        logger.warning(f"Failed to download image {url}: {e}")
        return None

async def process_event(sem: asyncio.Semaphore, ai_analyzer: EventAnalyzer, image_gen: EventImageGenerator, venue_enricher: VenueEnricher, dedup: EventDedup, db: Database, ev: dict, label_cache: LabelCache):
    async with sem:
        raw_text = ev['raw_text']
        source_url = ev['source_url']
        image_url = ev.get('image_url')
        logger.info(f"Extracting: {raw_text[:50]}...")
        
        result = await ai_analyzer.extract(raw_text, chat_title="todo.today")
        if not result or not result.get("is_event"):
            logger.warning(f"AI rejected event: {raw_text}")
            label_cache.add(raw_text)
            return
            
        if dedup.is_duplicate(result):
            logger.info(f"🚫 Duplicate skipped: {result.get('title', {})}")
            label_cache.add(raw_text)
            return
            
        # Enrich Venue (creates new if doesn't exist)
        if venue_enricher:
            try:
                result = await venue_enricher.enrich_event(result)
            except Exception as e:
                logger.error(f"Venue enrich error: {e}")
            
        # Download image if available
        local_image_path = await download_image(image_url) if image_url else None
            
        # Append meta
        result["_meta"] = {
            "chat_id": None,
            "chat_title": "todo.today",
            "message_id": 0,
            "sender": "scraper",
            "original_text": f"{raw_text}\\nSource: {source_url}"
        }
        
        # Insert to DB
        try:
            event_id, is_new, has_image = await db.insert_event(result, source="todotoday")
            label_cache.add(raw_text)
            if is_new:
                logger.info(f"✅ inserted NEW event: {result.get('title', {}).get('ru', 'Unknown')} (ID: {event_id})")
            else:
                logger.info(f"🔄 updated EXISTING event: {result.get('title', {}).get('ru', 'Unknown')} (ID: {event_id})")

            # Trigger Image Generation only if image is missing
            if event_id and local_image_path and (is_new or not has_image):
                logger.info(f"🎨 Generating high-quality WebP cover using scraped reference image...")
                filename = await image_gen.generate_cover(
                    raw_tg_text=raw_text, 
                    category=result.get("category", "General"), 
                    event_id=event_id, 
                    reference_image_path=local_image_path
                )
                if filename:
                    try:
                        os.remove(local_image_path)
                        logger.info(f"🗑️ Deleted raw scraped GUI image {local_image_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete scraped image {local_image_path}: {e}")
            elif local_image_path:
                # Cleanup raw image if we didn't need to generate a new cover
                try:
                    os.remove(local_image_path)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"DB insert failed: {e}")

async def scrape_todo_today():
    db = Database()
    await db.connect()
    ai_analyzer = EventAnalyzer()
    image_gen = EventImageGenerator(db)
    
    venue_enricher = VenueEnricher(db)
    await venue_enricher.cache.load_from_pg()
    logger.info(f"📍 Venue Enricher activated ({len(venue_enricher.cache)} in cache).")
    
    html_data = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Using a proper Mac user agent to look human
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        logger.info("Loading todo.today...")
        await page.goto("https://todo.today/koh-phangan/", wait_until="domcontentloaded")
        
        # Wait to let Cloudflare JS challenge pass
        logger.info("Waiting for Cloudflare bypass...")
        await page.wait_for_timeout(8000)
        
        # 1. Grab Today's events from the initial DOM
        logger.info("Capturing 'Today's' events from initial DOM...")
        today_html = await page.content()
        
        # 2. Grab Tomorrow's events by directly visiting the URL
        logger.info("Visiting /tomorrow/ to capture next day's events...")
        await page.goto("https://todo.today/koh-phangan/tomorrow/", wait_until="domcontentloaded")
        logger.info("Waiting for Cloudflare bypass on tomorrow page...")
        await page.wait_for_timeout(8000)
        
        tomorrow_html = await page.content()
        logger.info("Successfully fetched 'Tomorrow' HTML payload!")

        logger.info("Closing browser.")
        await browser.close()
        
        html_data = today_html + tomorrow_html

    if not html_data:
        logger.error("No HTML data to parse.")
        await db.close()
        return

    # Parse HTML
    logger.info("Parsing DOM for event cards...")
    soup = BeautifulSoup(html_data, "html.parser")
    
    event_boxes = soup.select(".event-box")
    logger.info(f"Found {len(event_boxes)} event cards.")
    
    parsed_events = []
    for box in event_boxes:
        link_tag = box.find("a", class_="event_image")
        if not link_tag:
            continue
            
        href = link_tag.get("href", "")
        aria_label = link_tag.get("aria-label", "")
        
        # Extract the image URL from the img tag
        img_tag = box.find("img")
        image_url = img_tag.get("src") if img_tag else None
        
        if aria_label:
            parsed_events.append({
                "source_url": href,
                "raw_text": aria_label,
                "image_url": image_url
            })
            
    # Deduplicate by URL
    unique_events = list({e["source_url"]: e for e in parsed_events}.values())
    logger.info(f"Successfully extracted {len(unique_events)} unique events via aria-label.")

    # Pre-filter: skip events already in local aria-label cache
    label_cache = LabelCache()
    label_cache.load()
    new_events = [e for e in unique_events if not label_cache.contains(e["raw_text"])]
    cached_count = len(unique_events) - len(new_events)
    if cached_count:
        logger.info(f"⚡ Label cache: {cached_count} known, {len(new_events)} new → sending to AI")

    # Deduplicator (cross-source DB check)
    dedup = EventDedup()
    await dedup.load_from_db(db)

    BATCH_SIZE = 10
    SLEEP_BETWEEN_BATCHES = 5
    
    logger.info(f"Piping {len(new_events)} events to AI Analytics in batches of {BATCH_SIZE}...")
    sem = asyncio.Semaphore(BATCH_SIZE)
    
    for i in range(0, len(new_events), BATCH_SIZE):
        batch = new_events[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(new_events) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logger.info(f"⏳ Processing batch {batch_num}/{total_batches} ({len(batch)} events)...")
        tasks = [process_event(sem, ai_analyzer, image_gen, venue_enricher, dedup, db, ev, label_cache) for ev in batch]
        await asyncio.gather(*tasks)
        
        if i + BATCH_SIZE < len(new_events):
            logger.info(f"💤 Sleeping {SLEEP_BETWEEN_BATCHES}s to prevent API rate limits...")
            await asyncio.sleep(SLEEP_BETWEEN_BATCHES)
    
    label_cache.save()
    logger.info(f"💾 Label cache saved ({len(label_cache)} entries)")
    
    if venue_enricher:
        venue_enricher.close()
    
    # Close DB connection
    await db.close()
    logger.info("Scraping and insertion complete.")

if __name__ == "__main__":
    asyncio.run(scrape_todo_today())
