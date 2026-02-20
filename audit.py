import asyncio
from db import Database
from collections import Counter
import pprint

async def audit_db():
    db = Database()
    await db.connect()

    # 1. Venues Audit
    venues = await db.pool.fetch("SELECT * FROM venues ORDER BY id DESC")
    print(f"\\n--- VENUES AUDIT ({len(venues)} total) ---")
    
    found_count = sum(1 for v in venues if v['found'])
    print(f"✅ Found on Google Maps: {found_count}")
    print(f"❌ Not found (fallback/TBD): {len(venues) - found_count}")

    # Check for duplicate names/locations
    v_names = [v['name'] for v in venues if v['name']]
    duplicates = [item for item, count in Counter(v_names).items() if count > 1]
    if duplicates:
        print(f"⚠️  POSSIBLE DUPLICATE VENUES: {duplicates}")
    else:
        print("✅ No duplicate venue names detected.")

    print("\\nSample of recently enriched venues:")
    for v in venues[:5]:
        print(f"  - [{v['id']}] {v['name']} | Found: {v['found']} | Lat: {v['lat']} | Lng: {v['lng']} | '{v['address']}'")

    # 2. Events Audit
    events = await db.pool.fetch("SELECT * FROM events ORDER BY id DESC")
    print(f"\\n--- EVENTS AUDIT ({len(events)} total) ---")

    with_venues = sum(1 for e in events if e['venue_id'] is not None)
    print(f"🎯 Events with linked venues: {with_venues}")
    print(f"❓ Events without linked venues: {len(events) - with_venues}")

    # Check for duplicate events (Same Title + Date)
    e_signatures = [(e['title'], str(e['event_date'])) for e in events]
    e_dupes = [item for item, count in Counter(e_signatures).items() if count > 1]
    
    if e_dupes:
        print(f"⚠️  POSSIBLE DUPLICATE EVENTS: {e_dupes}")
    else:
        print("✅ No duplicate events detected (Unique Title + Date).")

    print("\\nSample of recent events:")
    for e in events[:5]:
        v_name = "N/A"
        if e['venue_id']:
            v_match = next((v for v in venues if v['id'] == e['venue_id']), None)
            if v_match:
                v_name = v_match['name']
        print(f"  - [{e['id']}] {e['event_date']} | {e['title'][:40]} | Venue: {v_name}")
        print(f"    Desc: {e['description'][:80]}...")

    await db.close()

if __name__ == '__main__':
    asyncio.run(audit_db())
