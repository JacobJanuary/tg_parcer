#!/usr/bin/env python3
"""
Bilingual Migrator — script to translate historical single-language events
into bilingual JSON format (ru/en) using Gemini 2.5-flash.
"""

import asyncio
import logging
import sys
import os
import json
from pydantic import BaseModel
from google import genai
from google.genai import types

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from db import Database

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("BilingualMigrator")

class BilingualText(BaseModel):
    en: str
    ru: str

class TranslationResult(BaseModel):
    title: BilingualText
    summary: BilingualText
    description: BilingualText

PROMPT = """You are a professional translator for a live event discovery app.
Translate the following event details into BOTH English and Russian.
If the original text is mixed, separate it cleanly into the two languages.
Maintain the original tone, emojis, and catchy style.
Return ONLY valid JSON according to the schema.

Original Title: {title}
Original Summary: {summary}
Original Description: {description}
"""

async def process_batch(client: genai.Client, db: Database, events: list[dict], semaphore: asyncio.Semaphore):
    model_name = "gemini-2.5-flash"
    
    async def process_event(event):
        async with semaphore:
            prompt = PROMPT.format(
                title=event['orig_title'],
                summary=event['orig_summary'],
                description=event['orig_description']
            )
            try:
                result = await asyncio.to_thread(
                    client.models.generate_content,
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                        response_schema=TranslationResult,
                    )
                )
                
                # Parse result
                data = json.loads(result.text)
                title_json = json.dumps(data["title"], ensure_ascii=False)
                summary_json = json.dumps(data["summary"], ensure_ascii=False)
                desc_json = json.dumps(data["description"], ensure_ascii=False)
                
                await db.pool.execute("""
                    UPDATE events 
                    SET title = $1::jsonb, summary = $2::jsonb, description = $3::jsonb
                    WHERE id = $4
                """, title_json, summary_json, desc_json, event['id'])
                
                logger.info(f"✅ Migrated event {event['id']}: {data['title']['en']}")
            except Exception as e:
                logger.error(f"❌ Failed to migrate event {event['id']}: {e}")

    await asyncio.gather(*(process_event(e) for e in events))

async def run_migration(is_test: bool = False):
    logger.info("🌍 Initializing Bilingual Migrator...")
    
    db = Database(config.get_dsn())
    await db.connect()
    
    gemini_key, gemini_proxy = config.validate_gemini()
    client = genai.Client(api_key=gemini_key)
    if gemini_proxy:
        import httpx
        http_client = httpx.Client(proxy=gemini_proxy, timeout=120.0)
        client._api_client._httpx_client = http_client

    # Fetch events where 'en' and 'ru' texts are perfectly identical
    limit_clause = "LIMIT 5" if is_test else ""
    query = f"""
        SELECT 
            id, 
            title->>'ru' as orig_title, 
            summary->>'ru' as orig_summary, 
            description->>'ru' as orig_description 
        FROM events 
        WHERE title->>'ru' = title->>'en'
        ORDER BY id DESC
        {limit_clause}
    """
    
    events_to_migrate = await db.pool.fetch(query)
    events_to_migrate = [dict(r) for r in events_to_migrate]
    
    if not events_to_migrate:
        logger.info("🎉 All events are already translated!")
        await db.close()
        return

    logger.info(f"🚀 Found {len(events_to_migrate)} events needing translation.")
    
    # Process with max 5 concurrent requests to avoid aggressive rate limiting
    semaphore = asyncio.Semaphore(5)
    await process_batch(client, db, events_to_migrate, semaphore)
    
    logger.info("✨ Migration complete!")
    await db.close()

if __name__ == "__main__":
    is_test = "--test" in sys.argv
    asyncio.run(run_migration(is_test))
