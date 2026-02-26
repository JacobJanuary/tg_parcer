import asyncio
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from db import Database
from image_generator import EventImageGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Backfill")

async def main():
    db = Database(config.get_dsn())
    await db.connect()
    print("🐘 PostgreSQL connected")

    events = await db.pool.fetch("SELECT id, original_text, category, title->>'en' as title_en FROM events WHERE image_path IS NULL")
    
    if not events:
        print("✅ No events missing images.")
        await db.close()
        return

    print(f"🔍 Found {len(events)} events missing images. Starting backfill...")
    generator = EventImageGenerator(db=db)

    for row in events:
        eid = row["id"]
        text = row["original_text"] or row["title_en"]
        cat = row["category"]
        
        print(f"\\n➡️ Processing ID={eid} [{cat}] {row['title_en'][:50]}...")
        if text and cat:
            try:
                filename = await generator.generate_cover(text, cat, event_id=eid)
                if filename:
                    print(f"✅ Generated: {filename}")
                else:
                    print(f"❌ Failed to generate image for ID={eid}")
            except Exception as e:
                print(f"❌ Error generating image for ID={eid}: {e}")
        else:
            print(f"⚠️ Skipping ID={eid}: Missing text or category.")
            
        await asyncio.sleep(2) # Give APIs a short break

    print("\\n🎉 Backfill complete!")
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
