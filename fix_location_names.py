import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from db import Database

async def normalize_locations():
    db = Database(config.get_dsn())
    await db.connect()
    print("🐘 PostgreSQL connected")

    # 1. Gather all events with a venue_id and location_name
    rows = await db.pool.fetch('''
        SELECT venue_id, location_name, COUNT(*) as cnt
        FROM events
        WHERE venue_id IS NOT NULL AND location_name IS NOT NULL
        GROUP BY venue_id, location_name
        ORDER BY venue_id, cnt DESC
    ''')

    # Group by venue_id
    from collections import defaultdict
    groups = defaultdict(list)
    for r in rows:
        groups[r['venue_id']].append((r['location_name'], r['cnt']))

    updates = 0
    venue_count = 0

    # 2. For each venue with multiple names, pick the most frequent one (canonical)
    for vid, names in groups.items():
        if len(names) > 1:
            canonical_name = names[0][0] # The one with the highest count
            # Heuristics to pick a better canonical name if the most frequent one has a suffix
            # For example, if "Aum Sound Healing Center" is #1 but we prefer shorter "AUM"
            # Here we just stick to the most frequent one to be safe, except if we can trim ", "
            for n, c in names:
                if "," not in n and len(n) < len(canonical_name) and c > 0:
                     # Prefer names without commas if they exist
                     canonical_name = n
                     break

            print(f"\\n📍 Venue {vid} -> Canonical: '{canonical_name}'")
            for n, c in names:
                if n != canonical_name:
                    print(f"   🔄 Changing '{n}' ({c} events) -> '{canonical_name}'")
                    # Update all events with this secondary name to the canonical name
                    updated = await db.pool.execute(
                        "UPDATE events SET location_name = $1 WHERE venue_id = $2 AND location_name = $3",
                        canonical_name, vid, n
                    )
                    # execute returns string like "UPDATE 5"
                    try:
                        updates += int(updated.split()[1])
                    except:
                        pass
            venue_count += 1

    print(f"\\n✅ Normalization complete. Updated {updates} events across {venue_count} venues.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(normalize_locations())
