import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from db import Database

async def main():
    db = Database(config.get_dsn())
    await db.connect()
    print("🐘 PostgreSQL connected")

    events = await db.pool.fetch("SELECT id, title, image_path FROM events WHERE image_path IS NOT NULL")
    
    missing_count = 0
    media_dir = config.DEFAULT_MEDIA_DIR

    for row in events:
        file_path = os.path.join(media_dir, row["image_path"])
        if not os.path.exists(file_path):
            missing_count += 1
            print(f"❌ Missing: [{row['id']}] {row['image_path']} - {row['title'].get('en') if isinstance(row['title'], dict) else row['title']}")
            # Set to NULL
            await db.pool.execute("UPDATE events SET image_path = NULL WHERE id = $1", row["id"])

    print(f"\\n✅ Done! Fixed {missing_count} events by setting their image_path to NULL.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(main())
