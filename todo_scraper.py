import asyncio
import logging
import re
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
from datetime import datetime, timedelta

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

# ─── Day-of-week validation ───
_DAY_NAMES_EN = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                 "friday": 4, "saturday": 5, "sunday": 6}
_DAY_NAMES_RU = {"понедельник": 0, "вторник": 1, "среда": 2, "четверг": 3,
                 "пятница": 4, "суббота": 5, "воскресенье": 6}

def _fix_weekday_mismatch(result: dict) -> bool:
    """If title contains a weekday name, validate event_date matches. Fix if not."""
    title_en = (result.get("title") or {}).get("en", "").lower()
    title_ru = (result.get("title") or {}).get("ru", "").lower()
    date_str = result.get("date", "")
    if not date_str or date_str == "TBD":
        return False

    try:
        event_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    except ValueError:
        return False

    # Check EN and RU day names
    expected_weekday = None
    matched_day = None
    for day_name, wday in {**_DAY_NAMES_EN, **_DAY_NAMES_RU}.items():
        if day_name in title_en or day_name in title_ru:
            expected_weekday = wday
            matched_day = day_name
            break

    if expected_weekday is None:
        return False

    if event_date.weekday() == expected_weekday:
        return False  # All good

    # Shift to next correct weekday
    days_ahead = expected_weekday - event_date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    correct_date = event_date + timedelta(days=days_ahead)
    old_date = result["date"]
    result["date"] = correct_date.strftime("%Y-%m-%d")
    logger.warning(
        f"📅 Day-of-week fix: '{matched_day}' in title but date was "
        f"{old_date} ({event_date.strftime('%A')}). Fixed to {result['date']} ({correct_date.strftime('%A')})"
    )
    return True


async def process_event(sem: asyncio.Semaphore, ai_analyzer: EventAnalyzer, image_gen: EventImageGenerator, venue_enricher: VenueEnricher, dedup: EventDedup, db: Database, ev: dict, label_cache: LabelCache):
    async with sem:
        raw_text = ev['raw_text']
        source_url = ev['source_url']
        image_url = ev.get('image_url')
        logger.info(f"Extracting: {raw_text[:50]}...")
        
        result = await ai_analyzer.extract(raw_text, chat_title="todo.today")
        if not result or not result.get("is_event"):
            logger.warning(f"AI rejected event: {raw_text[:60]}")
            # NOTE: do NOT add to label_cache here! If AI was wrong,
            # the event would be lost forever. Let it retry next hour.
            return

        # Fix 2: Day-of-week validation (e.g. "Bachata Monday" on a Sunday)
        _fix_weekday_mismatch(result)
            
        if await dedup.is_duplicate(result):
            logger.info(f"🚫 Duplicate skipped: {result.get('title', {})}")
            # NOTE: do NOT cache — dedup relies on DB state which changes.
            # If the original was deleted, this event would never re-appear.
            return

        # Geo-filter: reject events outside Koh Phangan
        from event_dedup import is_off_island
        if is_off_island(result, raw_text):
            logger.info(f"🌍 Off-island event skipped: {result.get('title', {})}")
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
            if not event_id:
                logger.warning(f"⛔ Event rejected by DB (island filter?): {result.get('title', {})}")
                return
            label_cache.add(raw_text)
            label_cache.add(source_url)  # URL dedup: prevent re-processing same event
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

async def smart_bump_events(db, cached_events):
    """
    Finds duplicated events that were scraped on a previous day and bumps their 
    event_date to align with dynamic 'Today' / 'Tomorrow' labels in their raw UI text,
    bypassing the need to regenerate prompts and images.
    """
    from datetime import datetime, timedelta
    import re
    import json
    
    today = datetime.now().date()
    bumped_count: int = 0
    
    for ev in cached_events:
        raw_text = ev.get("raw_text", "")
        raw_title = ev.get("raw_title", "")
        raw_date = ev.get("raw_date", "")
        
        if not raw_title or not raw_date:
            continue
            
        if raw_date.lower() == "today":
            target_date = today
        elif raw_date.lower() == "tomorrow":
            target_date = today + timedelta(days=1)
        else:
            continue
            
        try:
            # Extract venue from raw_text for cross-validation
            # raw_text format: "Title: X\nDate: Y\nTime: Z\nPrice: W\nLocation: V"
            raw_venue = ""
            venue_match = re.search(r'Location:\s*(.+?)(?:\\\\n|$)', raw_text)
            if venue_match:
                raw_venue = venue_match.group(1).strip().rstrip(',').strip()
            
            # Strategy: Match by EXACT title + venue (both required)
            # Title includes teacher name, e.g. "Vinyasa Yoga w/ Fah" ≠ "Vinyasa Yoga w/ Tony"
            row = None
            if not raw_venue:
                logger.warning(f"No venue in raw_text, skipping: '{raw_title[:40]}'")
                continue
            
            # Match title AND venue in original_text
            row = await db.pool.fetchrow("""
                SELECT id, title, event_date
                FROM events
                WHERE source ILIKE '%todo%'
                  AND original_text LIKE $1
                  AND original_text LIKE $2
                ORDER BY detected_at DESC
                LIMIT 1
            """, f"Title: {raw_title}%", f"%Location: {raw_venue}%")
            
            if not row:
                logger.warning(f"DB Row not found for '{raw_title[:40]}' @ '{raw_venue[:20]}'")
                continue
                
            old_id = row['id']
            old_date = row['event_date']
            
            if old_date and old_date < target_date:
                title_data = row['title']
                if isinstance(title_data, str):
                    try:
                        title_data = json.loads(title_data)
                    except:
                        pass
                        
                new_fp = db._fingerprint(title_data, target_date.isoformat())
                
                exists = await db.pool.fetchval("SELECT id FROM events WHERE fingerprint = $1", new_fp)
                if not exists:
                    await db.pool.execute(
                        "UPDATE events SET event_date = $1, fingerprint = $2, detected_at = now() WHERE id = $3", 
                        target_date, new_fp, old_id
                    )
                    bumped_count += 1
                    t_str = str(raw_title)
                    logger.info(f"🔄 Bumped event {old_id} ('{t_str[:20]}...') from {old_date} to {target_date}")
                else:
                    logger.warning(f"Target DB entry already exists for FP {new_fp}")
            else:
                 logger.warning(f"Old date ({old_date}) not less than target_date ({target_date}) for event {old_id}")
                    
        except Exception as e:
            t_str = str(raw_title)
            logger.error(f"Error checking smart bump for '{t_str[:20]}': {e}")
            
    if bumped_count > 0:
        logger.info(f"🚀 Smart Bump complete: {bumped_count} recurring events moved forward.")

async def scrape_todo_today():
    db = Database()
    await db.connect()
    ai_analyzer = EventAnalyzer()
    image_gen = EventImageGenerator(db)
    
    venue_enricher = VenueEnricher(db)
    await venue_enricher.cache.load_from_pg()
    logger.info(f"📍 Venue Enricher activated ({len(venue_enricher.cache)} in cache).")
    all_raw_events = []

    async def fetch_events(url: str, label: str) -> list:
        logger.info(f"Loading todo.today ({label}) in an isolated context...")
        events_batch = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            async def handle_response(response):
                if "todo-today/v1/events" in response.url:
                    try:
                        data = await response.json()
                        sections = data.get("sections", [])
                        for s in sections:
                            for ev in s.get("events", []):
                                events_batch.append(ev)
                    except Exception as e:
                        logger.error(f"Intercept error on {label}: {e}")

            page.on("response", handle_response)
            
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(8000)
            
            await browser.close()
            return events_batch

    # Fetch sequentially in isolated browsers to bypass CF and SPA cache
    today_events = await fetch_events("https://todo.today/koh-phangan/", "Today")
    if today_events:
        all_raw_events.extend(today_events)
    
    tomorrow_events = await fetch_events("https://todo.today/koh-phangan/tomorrow/", "Tomorrow")
    if tomorrow_events:
        all_raw_events.extend(tomorrow_events)

    if not all_raw_events:
        logger.error("No API JSON data intercepted.")
        await db.close()
        return

    # Parse and deduplicate intercepted events
    logger.info("Parsing intercepted API events...")
    events_map = {}
    for ev in all_raw_events:
        if ev.get('link'):
            events_map[ev['link']] = ev
            
    parsed_events = []
    for link, ev in events_map.items():
        # Fix 1: Extract reliable date from URL instead of display_date
        # URL format: https://todo.today/koh-phangan/2026/03/09/event-slug
        url_date_match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', link)
        if url_date_match:
            url_date = f"{url_date_match.group(1)}-{url_date_match.group(2)}-{url_date_match.group(3)}"
        else:
            url_date = ev.get('display_date', '')  # fallback

        # Build raw_text with reliable date from URL
        raw_text = f"Title: {ev.get('name', '')}\\nDate: {url_date}\\nTime: {ev.get('start_time', '')} to {ev.get('end_time', '')}\\nPrice: {ev.get('price_label', '')}\\nLocation: {ev.get('venue', '')}"
        
        parsed_events.append({
            "source_url": link,
            "raw_text": raw_text.strip(),
            "image_url": ev.get('image'),
            "raw_title": ev.get('name', '').strip(),
            "raw_date": url_date
        })

    unique_events = parsed_events
    logger.info(f"Successfully extracted {len(unique_events)} unique events via embedded JSON.")

    # Pre-filter: skip events already in local aria-label cache
    label_cache = LabelCache()
    label_cache.load()

    new_events = []
    cached_events = []
    for e in unique_events:
        if label_cache.contains(e["raw_text"]) or label_cache.contains(e["source_url"]):
            cached_events.append(e)
        else:
            new_events.append(e)

    cached_count = len(cached_events)
    if cached_count:
        logger.info(f"⚡ Label cache: {cached_count} known, {len(new_events)} new (sending to AI).")
        await smart_bump_events(db, cached_events)

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
