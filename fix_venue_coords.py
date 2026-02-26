#!/usr/bin/env python3
"""
Phase 5: Fix venue coordinates.
- Remove venues with clearly wrong coords (Prague IP Club, Lake House wrong lng)
- Re-enrich bad venues via Google Maps API
"""
import asyncio, sys
sys.path.insert(0, '/home/elcrypto/TG_parcer')
import config
from db import Database
from venue_enricher import VenueEnricher

# Real Koh Phangan bounding box (expanded to include southern coast)
PHANGAN_LAT_MIN = 9.65
PHANGAN_LAT_MAX = 9.82
PHANGAN_LNG_MIN = 99.86
PHANGAN_LNG_MAX = 100.10

# Known legitimate non-Phangan venues (Samui/Tao) — don't touch
LEGITIMATE_OTHER_ISLANDS = {
    'Secret Party Koh Tao',
    'Buffalo Jungle Samui',
    'The eXperience Samui',
    'Q Bar Samui',
    'Koh Tao - Secret Garden',
}

async def main():
    db = Database(config.get_dsn())
    await db.connect()
    print("🐘 Connected\n")

    # Get all venues outside Phangan bbox
    out_of_bounds = await db.pool.fetch("""
        SELECT id, name, lat, lng, address
        FROM venues
        WHERE lat IS NOT NULL
        ORDER BY id
    """)

    clearly_wrong = []
    samui_area = []
    legitimate = []

    for v in out_of_bounds:
        lat, lng = float(v['lat']), float(v['lng'])
        name = v['name']

        if PHANGAN_LAT_MIN <= lat <= PHANGAN_LAT_MAX and PHANGAN_LNG_MIN <= lng <= PHANGAN_LNG_MAX:
            continue  # Inside bbox, skip

        if name in LEGITIMATE_OTHER_ISLANDS:
            legitimate.append(v)
            continue

        # Clearly wrong: Prague, wrong ocean, etc
        if lat > 20 or lng < 90 or lng > 110:
            clearly_wrong.append(v)
        # Southern Phangan area (9.49-9.65) — these are likely correct but borderline
        elif 9.49 <= lat < PHANGAN_LAT_MIN:
            samui_area.append(v)
        else:
            samui_area.append(v)

    print(f"Clearly wrong coords: {len(clearly_wrong)}")
    for v in clearly_wrong:
        print(f"  id={v['id']}  «{v['name']}»  ({v['lat']}, {v['lng']})")

    print(f"\nSamui/borderline area: {len(samui_area)}")
    for v in samui_area:
        print(f"  id={v['id']}  «{v['name']}»  ({v['lat']}, {v['lng']})")

    print(f"\nLegitimate other islands: {len(legitimate)}")
    for v in legitimate:
        print(f"  id={v['id']}  «{v['name']}»")

    # Fix clearly wrong venues — re-enrich with Google Maps
    enricher = VenueEnricher(db=db)
    await enricher.cache.load_from_pg()

    fixed = 0
    for v in clearly_wrong:
        print(f"\n🔧 Re-enriching: {v['name']}")
        # Delete old cache entry
        await db.pool.execute("DELETE FROM venue_aliases WHERE venue_id = $1", v['id'])

        result = await enricher.enrich(v['name'])
        if result and result.get('found'):
            await db.pool.execute(
                "UPDATE venues SET lat = $1, lng = $2, address = $3 WHERE id = $4",
                result['lat'], result['lng'], result.get('address', ''), v['id']
            )
            print(f"  ✅ Fixed: ({result['lat']}, {result['lng']})")
            fixed += 1
        else:
            print(f"  ❌ Could not re-enrich")

    # Also fix The Lake House which has lng=99.0006 (clearly wrong)
    lake = await db.pool.fetchrow("SELECT id, name, lat, lng FROM venues WHERE name ILIKE '%lake house%'")
    if lake and lake['lng'] and float(lake['lng']) < 99.5:
        print(f"\n🔧 Re-enriching: {lake['name']} (bad lng={lake['lng']})")
        result = await enricher.enrich(lake['name'])
        if result and result.get('found'):
            await db.pool.execute(
                "UPDATE venues SET lat = $1, lng = $2, address = $3 WHERE id = $4",
                result['lat'], result['lng'], result.get('address', ''), lake['id']
            )
            print(f"  ✅ Fixed: ({result['lat']}, {result['lng']})")
            fixed += 1

    print(f"\n📊 Fixed {fixed} venues")
    enricher.close()
    await db.close()

asyncio.run(main())
