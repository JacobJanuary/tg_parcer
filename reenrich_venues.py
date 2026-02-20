#!/usr/bin/env python3
"""
Re-enrich missing venues: purge found=false cache, call Gemini fresh.

1. Находит все events с реальным location_name но без venue_id
2. Удаляет found=false записи из venues для этих локаций
3. Вызывает VenueEnricher.enrich() для каждой → Gemini + Google Search
4. Обновляет events.venue_id для обогащённых

Запуск: DB_PORT=5435 python reenrich_venues.py
"""

import asyncio
from dotenv import load_dotenv
load_dotenv()

import config
from db import Database
from venue_enricher import VenueEnricher, _normalize_venue_name


async def main():
    db = Database(config.get_dsn())
    await db.connect()

    # 1. Find events with real location but no venue_id
    rows = await db.pool.fetch("""
        SELECT DISTINCT location_name FROM events
        WHERE venue_id IS NULL
          AND location_name IS NOT NULL
          AND location_name != 'TBD'
          AND location_name != ''
        ORDER BY location_name
    """)
    locations = [r["location_name"] for r in rows]
    print(f"📍 Локации без venue: {len(locations)}")
    for loc in locations:
        print(f"   • {loc}")

    # 2. Purge found=false cache entries for these by deleting from aliases where venue_id IS NULL
    purged = 0
    for loc in locations:
        key = _normalize_venue_name(loc)
        result = await db.pool.execute(
            "DELETE FROM venue_aliases WHERE query = $1 AND venue_id IS NULL", key
        )
        if "DELETE 1" in result:
            purged += 1
            print(f"   🗑️  Purged cache: '{key}'")

    # Also purge Shivari specifically
    r = await db.pool.execute(
        "DELETE FROM venue_aliases WHERE query = 'shivari' AND venue_id IS NULL"
    )
    if "DELETE 1" in r:
        purged += 1
        print(f"   🗑️  Purged cache: 'shivari'")

    print(f"\n🗑️  Purged {purged} stale cache entries")

    # 3. Init enricher + load cache
    enricher = VenueEnricher(db=db)
    await enricher.cache.load_from_pg()
    print(f"📦 Venues в кэше: {len(enricher.cache)}")

    # 4. Enrich each missing location via Gemini
    # 4. Enrich each missing location via Gemini concurrently
    all_to_enrich = list(set(locations + ["Shivari"]))
    
    print(f"\n{'─' * 60}")
    print(f"🔍 Async Enriching {len(all_to_enrich)} venues via Gemini (max 50 concurrent)...")
    print(f"{'─' * 60}")

    sem = asyncio.Semaphore(50)
    
    async def _enrich_task(loc: str):
        async with sem:
            try:
                result = await enricher.enrich(loc)
                if result and result.get("found"):
                    print(f"  ✅ '{loc}' → {result.get('name')} ({result.get('lat')}, {result.get('lng')})")
                    return True
                else:
                    print(f"  ❌ '{loc}' → not found")
                    return False
            except Exception as e:
                print(f"  💥 '{loc}' → {type(e).__name__}: {e}")
                return False

    tasks = [_enrich_task(loc) for loc in sorted(all_to_enrich)]
    results = await asyncio.gather(*tasks)
    
    enriched = sum(results)
    failed = len(results) - enriched

    # 5. Update events.venue_id for newly enriched venues
    print(f"\n{'─' * 60}")
    print(f"📝 Updating events.venue_id...")

    updated = 0
    events_no_venue = await db.pool.fetch("""
        SELECT id, location_name FROM events
        WHERE venue_id IS NULL
          AND location_name IS NOT NULL
          AND location_name != 'TBD'
    """)

    for ev in events_no_venue:
        venue = await db.get_venue(ev["location_name"])
        if venue and venue.get("found"):
            await db.pool.execute(
                "UPDATE events SET venue_id = $1 WHERE id = $2",
                venue["id"], ev["id"]
            )
            updated += 1

    # Final stats
    total = await db.pool.fetchval("SELECT count(*) FROM events")
    with_venue = await db.pool.fetchval("SELECT count(*) FROM events WHERE venue_id IS NOT NULL")
    venues_total = await db.pool.fetchval("SELECT count(*) FROM venues")
    venues_found = await db.pool.fetchval("SELECT count(*) FROM venues")

    print(f"\n{'═' * 60}")
    print(f"✅ Enriched: {enriched}")
    print(f"❌ Not found: {failed}")
    print(f"📝 Events updated: {updated}")
    print(f"📦 Venues: {venues_total} ({venues_found} found)")
    pct = round(100 * with_venue / total, 1) if total > 0 else 0
    print(f"🎯 Events with venue: {with_venue}/{total} ({pct}%)")

    enricher.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
