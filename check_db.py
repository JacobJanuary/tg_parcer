import asyncio
from db import Database

async def check():
    db = Database()
    await db.connect()
    
    events = await db.pool.fetchval("SELECT COUNT(*) FROM events")
    venues = await db.pool.fetchval("SELECT COUNT(*) FROM venues")
    found_venues = await db.pool.fetchval("SELECT COUNT(*) FROM venues WHERE found = true")
    discovered = await db.pool.fetchval("SELECT COUNT(*) FROM discovered_chats")
    
    print("=" * 40)
    print(" 📊 FINAL DATABASE PARSE METRICS")
    print("=" * 40)
    print(f"✅ Total Events Saved:          {events}")
    print(f"✅ Total Venues Processed:      {venues}")
    print(f"📍 Venues Successfully Found:   {found_venues}")
    print(f"🕷️ Total Discoveries (Spider):  {discovered}")
    print("=" * 40)
    
    # Let's also print 3 latest events just to be cool
    print("\nRecent Events:")
    recent = await db.pool.fetch("SELECT title, event_date, location_name FROM events ORDER BY id DESC LIMIT 3")
    for r in recent:
        print(f" - {r['title']} | Date: {r['event_date']} | Loc: {r['location_name']}")
        
    print("\nRecent Discoveries:")
    recent_spider = await db.pool.fetch("SELECT title, invite_link, times_seen FROM discovered_chats ORDER BY id DESC LIMIT 3")
    for r in recent_spider:
        print(f" - {r['title']} | Link: {r['invite_link']} | Seen: {r['times_seen']}x")

if __name__ == '__main__':
    asyncio.run(check())
