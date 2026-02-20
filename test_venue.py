#!/usr/bin/env python3
"""
Тест venue enrichment pipeline — изолированная проверка.

Подключается к PG, загружает кэш, пробует enrich несколько площадок,
показывает результат и проверяет наличие "found" ключа.
"""

import asyncio
import json
import sys

from dotenv import load_dotenv
load_dotenv()

import config
from db import Database
from venue_enricher import VenueEnricher


TEST_VENUES = [
    "Кефир",
    "ДОБРОПАР ПАНГАН",
    "NASHEMESTO",
    "Orion Healing Centre",
    "Shivari"
]


async def main():
    print("=" * 60)
    print("🧪 Venue Enrichment Test")
    print("=" * 60)

    # 1. Connect DB
    db = Database(config.get_dsn())
    await db.connect()
    venue_count = await db.get_venue_count()
    print(f"\n📦 Venues в PG: {venue_count}")

    # 2. Init enricher + load cache
    enricher = VenueEnricher(db=db)
    await enricher.cache.load_from_pg()
    print(f"📦 Venues в memory cache: {len(enricher.cache)}")

    # 3. Test enrich() for each venue
    print(f"\n{'─' * 60}")
    print("Phase 1: enrich() — raw lookup")
    print(f"{'─' * 60}")

    for name in TEST_VENUES:
        try:
            result = await enricher.enrich(name)
            if result:
                print(f"\n  ✅ '{name}' → found")
                print(f"     name:  {result.get('name')}")
                print(f"     lat:   {result.get('lat')}")
                print(f"     lng:   {result.get('lng')}")
                print(f"     found: {result.get('found')}  ← {'✅' if result.get('found') else '🔴 MISSING!'}")
            else:
                print(f"\n  ❌ '{name}' → None (not found or TBD)")
        except Exception as e:
            print(f"\n  💥 '{name}' → ERROR: {type(e).__name__}: {e}")

    # 4. Test enrich_event() — simulate what test_listener does
    print(f"\n{'─' * 60}")
    print("Phase 2: enrich_event() — simulate test_listener")
    print(f"{'─' * 60}")

    for name in ["Catch", "Shivari", "SATI YOGA"]:
        event = {"location_name": name, "title": f"Test Event at {name}"}
        try:
            await enricher.enrich_event(event)
            venue = event.get("venue")
            if venue:
                print(f"\n  ✅ '{name}' → venue attached")
                print(f"     venue keys: {list(venue.keys())}")
                print(f"     found:      {venue.get('found')}  ← {'✅' if venue.get('found') else '🔴 MISSING!'}")
                print(f"     name:       {venue.get('name')}")
            else:
                print(f"\n  ❌ '{name}' → no venue attached to event")
        except Exception as e:
            print(f"\n  💥 '{name}' → ERROR: {type(e).__name__}: {e}")

    # 5. Simulate insert_event venue_id resolution
    print(f"\n{'─' * 60}")
    print("Phase 3: insert_event() venue_id resolution simulation")
    print(f"{'─' * 60}")

    for name in ["Catch", "Shivari"]:
        event = {"location_name": name, "title": f"Test at {name}"}
        await enricher.enrich_event(event)
        venue = event.get("venue")
        # This is what db.insert_event() does:
        if venue and venue.get("found"):
            print(f"  ✅ '{name}' → venue_id WOULD be resolved")
        else:
            found_val = venue.get("found") if venue else "no venue"
            print(f"  🔴 '{name}' → venue_id SKIPPED (found={found_val})")

    # Stats
    print(f"\n{'─' * 60}")
    print(f"📊 Enricher stats: {json.dumps(enricher.stats, indent=2)}")

    enricher.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
