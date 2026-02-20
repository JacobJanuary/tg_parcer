#!/usr/bin/env python3
"""
Миграция: мержит дублирующиеся venue записи в PostgreSQL.

Для каждой группы дублей (по нормализованному имени):
  - Оставляет запись с found=true (или с координатами)
  - Удаляет остальные
  - Обновляет query на нормализованное имя

Запуск: DB_PORT=5435 python migrate_venues.py
"""

import asyncio
import re
from dotenv import load_dotenv
load_dotenv()

import config
from db import Database, _normalize_name


async def main():
    db = Database(config.get_dsn())
    await db.connect()

    # 1. Load all venues
    rows = await db.pool.fetch("SELECT * FROM venues ORDER BY id")
    print(f"📦 Total venues: {len(rows)}")

    # 2. Group by normalized name
    groups: dict[str, list] = {}
    for r in rows:
        key = _normalize_name(r["query"])
        groups.setdefault(key, []).append(dict(r))

    dupes = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"🔍 Groups with duplicates: {len(dupes)}")

    merged = 0
    deleted_ids = []

    for key, entries in dupes.items():
        # Pick the best entry: prefer found=True with coordinates
        best = None
        for e in entries:
            if e["found"] and e.get("lat") and e.get("lng"):
                if best is None or e["id"] < best["id"]:
                    best = e
        if best is None:
            # No found entry — keep the first one
            best = entries[0]

        others = [e for e in entries if e["id"] != best["id"]]

        print(f"\n  🔀 '{key}' → keep #{best['id']} ({best['name']}, found={best['found']})")
        for o in others:
            print(f"     ❌ delete #{o['id']} (query='{o['query']}', found={o['found']})")
            deleted_ids.append(o["id"])

        # Delete duplicates FIRST (to avoid UNIQUE conflict)
        for o in others:
            await db.pool.execute("DELETE FROM venues WHERE id = $1", o["id"])

        # Then update best entry's query to normalized key
        await db.pool.execute(
            "UPDATE venues SET query = $1, name_normalized = $1 WHERE id = $2",
            key, best["id"]
        )

        merged += 1


    # 3. Also normalize ALL non-duplicate queries
    remaining = await db.pool.fetch("SELECT id, query FROM venues")
    updated = 0
    for r in remaining:
        norm = _normalize_name(r["query"])
        if norm != r["query"]:
            try:
                await db.pool.execute(
                    "UPDATE venues SET query = $1, name_normalized = $1 WHERE id = $2",
                    norm, r["id"]
                )
                updated += 1
            except Exception as e:
                # Conflict — another venue already has this normalized query
                print(f"  ⚠️ Conflict normalizing '{r['query']}' → '{norm}': {e}")
                # Delete this duplicate
                await db.pool.execute("DELETE FROM venues WHERE id = $1", r["id"])
                deleted_ids.append(r["id"])

    final_count = await db.pool.fetchval("SELECT count(*) FROM venues")

    print(f"\n{'═' * 60}")
    print(f"✅ Merged: {merged} groups")
    print(f"🗑️  Deleted: {len(deleted_ids)} duplicates")
    print(f"📝 Normalized: {updated} queries")
    print(f"📦 Final venues: {final_count}")

    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
