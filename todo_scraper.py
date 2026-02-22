import asyncio
import logging
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from db import Database
from ai_analyzer import EventAnalyzer
from image_generator import EventImageGenerator

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

async def process_event(sem: asyncio.Semaphore, ai_analyzer: EventAnalyzer, image_gen: EventImageGenerator, db: Database, ev: dict):
    async with sem:
        raw_text = ev['raw_text']
        source_url = ev['source_url']
        image_url = ev.get('image_url')
        logger.info(f"Extracting: {raw_text[:50]}...")
        
        result = await ai_analyzer.extract(raw_text, chat_title="todo.today")
        if not result or not result.get("is_event"):
            logger.warning(f"AI rejected event: {raw_text}")
            return
            
        # Download image if available
        local_image_path = await download_image(image_url) if image_url else None
            
        # Append meta
        result["_meta"] = {
            "chat_id": 0,
            "chat_title": "todo.today",
            "message_id": 0,
            "sender": "scraper",
            "original_text": f"{raw_text}\\nSource: {source_url}"
        }
        
        # Insert to DB
        try:
            event_id, is_new, _ = await db.insert_event(result, source="todotoday")
            if is_new:
                logger.info(f"✅ inserted NEW event: {result.get('title', {}).get('ru', 'Unknown')} (ID: {event_id})")
            else:
                logger.info(f"🔄 updated EXISTING event: {result.get('title', {}).get('ru', 'Unknown')} (ID: {event_id})")

            if event_id and local_image_path:
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

        except Exception as e:
            logger.error(f"DB insert failed: {e}")

async def scrape_todo_today():
    db = Database()
    await db.connect()
    ai_analyzer = EventAnalyzer()
    image_gen = EventImageGenerator(db)
    
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
        
        # We will intercept the AJAX response directly by clicking a UI element
        logger.info("Setting up AJAX interception...")
        
        ajax_response_html = ""
        
        async def handle_response(response):
            nonlocal ajax_response_html
            if "admin-ajax.php" in response.url and response.request.method == "POST":
                try:
                    payload = await response.json()
                    if payload.get("success"):
                        ajax_response_html = payload.get("data", {}).get("html", "")
                        logger.info("Intercepted valid AJAX response.")
                except Exception:
                    pass
                    
        page.on("response", handle_response)
        
        logger.info("Clicking the 'Tomorrow' filter tab to trigger AJAX fetch...")
        trigger_js = """
        () => {
            const tabs = document.querySelectorAll('.mec-calendar-day');
            if(tabs && tabs.length > 1) {
                tabs[1].click(); // Click tomorrow
                return true;
            }
            const filters = document.querySelectorAll('li[data-filter]');
            if(filters && filters.length > 0) {
                filters[0].click();
                return true;
            }
            return false;
        }
        """
        clicked = await page.evaluate(trigger_js)
        if not clicked:
            logger.warning("Could not find date tabs to click via JS.")
            
        logger.info("Waiting 5s for AJAX to return...")
        await page.wait_for_timeout(5000)
        
        if ajax_response_html:
            logger.info("Successfully fetched HTML payload from interception!")
            html_data = ajax_response_html
        else:
            logger.warning("Failed to intercept AJAX HTML. Falling back to reading raw DOM...")
            html_data = await page.content()

        logger.info("Closing browser.")
        await browser.close()

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
        
        # Extract the background image URL from the style tag
        # e.g. background-image: url('https://todo.today/wp-content/uploads/something.jpg');
        style_attr = link_tag.get("style", "")
        image_url = None
        if "url('" in style_attr:
            image_url = style_attr.split("url('")[1].split("')")[0]
        elif "url(" in style_attr:
            image_url = style_attr.split("url(")[1].split(")")[0]
        
        if aria_label:
            parsed_events.append({
                "source_url": href,
                "raw_text": aria_label,
                "image_url": image_url
            })
            
    # Deduplicate by URL
    unique_events = list({e["source_url"]: e for e in parsed_events}.values())
    logger.info(f"Successfully extracted {len(unique_events)} unique events via aria-label.")
    
    sem = asyncio.Semaphore(10)
    tasks = [process_event(sem, ai_analyzer, image_gen, db, ev) for ev in unique_events]
    
    logger.info("Piping raw strings to AI Analyzer & Database...")
    await asyncio.gather(*tasks)
    
    # Close DB connection
    await db.close()
    logger.info("Scraping and insertion complete.")

if __name__ == "__main__":
    asyncio.run(scrape_todo_today())
