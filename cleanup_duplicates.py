import asyncio
import asyncpg
from db import Database

async def main():
    db = Database()
    await db.connect()
    
    print("Recalculating event fingerprints to match new deduplication logic...")
    records = await db.pool.fetch("SELECT id, title, event_date FROM events")
    
    updated = 0
    deleted = 0
    
    for r in records:
        date_str = r['event_date'].isoformat() if r['event_date'] else None
        new_fp = db._fingerprint(r['title'], date_str)
        
        try:
            await db.pool.execute("UPDATE events SET fingerprint = $1 WHERE id = $2", new_fp, r['id'])
            updated += 1
        except asyncpg.UniqueViolationError:
            print(f"Deleting duplicate event: {r['title']} on {date_str}")
            await db.pool.execute("DELETE FROM events WHERE id = $1", r['id'])
            deleted += 1
            
    print(f"Cleanup complete. Updated: {updated}, Deleted: {deleted}")
    
    # 2. Venues deduplication (just empty them if needed, but user said 0 venues anyway)
    venues_count = await db.pool.fetchval("SELECT COUNT(*) FROM venues")
    print(f"Total venues in db: {venues_count}")

    await db.close()

if __name__ == '__main__':
    asyncio.run(main())
