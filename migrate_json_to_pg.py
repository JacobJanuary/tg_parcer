#!/usr/bin/env python3
"""
Миграция JSON → PostgreSQL.

Читает все JSON-файлы и импортирует в PostgreSQL.
Безопасен для повторного запуска (upsert / ON CONFLICT).

Использование:
    python migrate_json_to_pg.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime

# Добавляем текущую директорию в path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from db import Database


async def migrate():
    db = Database(config.get_dsn())
    await db.connect()

    print("🔄 Миграция JSON → PostgreSQL\n")

    # ═══════════════════════════════════════
    # 1. selected_chats.json → chats
    # ═══════════════════════════════════════
    chats_file = "selected_chats.json"
    if os.path.exists(chats_file):
        with open(chats_file) as f:
            chats = json.load(f)
        for c in chats:
            await db.upsert_chat(c["id"], c["title"], c.get("type", "megagroup"))
        print(f"✅ chats: {len(chats)} записей")
    else:
        print(f"⚠️  {chats_file} не найден")

    # ═══════════════════════════════════════
    # 2. discovered_chats.json → discovered_chats
    # ═══════════════════════════════════════
    disc_file = "discovered_chats.json"
    if os.path.exists(disc_file):
        with open(disc_file) as f:
            discovered = json.load(f)

        count = 0
        for d in discovered:
            # found_in_chat нужно резолвить в chat_id
            found_in = d.get("found_in_chat")
            found_in_id = None
            if found_in:
                # Ищем чат по названию
                row = await db.pool.fetchrow(
                    "SELECT id FROM chats WHERE title = $1", found_in
                )
                if row:
                    found_in_id = row["id"]

            await db.upsert_discovered(
                chat_id=d.get("chat_id"),
                username=d.get("username"),
                invite_link=d.get("invite_link"),
                title=d.get("title"),
                chat_type=d.get("type"),
                source_type=d.get("source_type", "forward"),
                found_in_chat_id=found_in_id,
                participants_count=d.get("participants_count"),
                status=d.get("status", "new"),
                resolved=d.get("resolved", False),
            )

            # Обновляем times_seen, first_seen, last_seen
            existing = None
            if d.get("username"):
                existing = await db.pool.fetchrow(
                    "SELECT id FROM discovered_chats WHERE lower(username) = lower($1)",
                    d["username"],
                )
            elif d.get("invite_link"):
                existing = await db.pool.fetchrow(
                    "SELECT id FROM discovered_chats WHERE invite_link = $1",
                    d["invite_link"],
                )
            elif d.get("chat_id"):
                existing = await db.pool.fetchrow(
                    "SELECT id FROM discovered_chats WHERE chat_id = $1",
                    d["chat_id"],
                )

            if existing and d.get("times_seen"):
                first = d.get("first_seen")
                last = d.get("last_seen")
                await db.pool.execute("""
                    UPDATE discovered_chats
                    SET times_seen = $2,
                        first_seen = COALESCE($3, first_seen),
                        last_seen = COALESCE($4, last_seen)
                    WHERE id = $1
                """,
                    existing["id"],
                    d.get("times_seen", 1),
                    datetime.fromisoformat(first) if first else None,
                    datetime.fromisoformat(last) if last else None,
                )
            count += 1

        print(f"✅ discovered_chats: {count} записей")
    else:
        print(f"⚠️  {disc_file} не найден")

    # ═══════════════════════════════════════
    # 3. data/venues.json → venues
    # ═══════════════════════════════════════
    venues_file = "data/venues.json"
    if os.path.exists(venues_file):
        with open(venues_file) as f:
            venues = json.load(f)

        for query_key, data in venues.items():
            await db.upsert_venue(query_key, {
                "name": data.get("name", query_key),
                "found": data.get("found", False),
                "lat": data.get("lat"),
                "lng": data.get("lng"),
                "google_maps_url": data.get("google_maps_url"),
                "instagram_url": data.get("instagram_url"),
                "address": data.get("address"),
                "description": data.get("description"),
            })
        print(f"✅ venues: {len(venues)} записей")
    else:
        print(f"⚠️  {venues_file} не найден")

    # ═══════════════════════════════════════
    # 4. output/events.jsonl → events (listener)
    # ═══════════════════════════════════════
    events_file = "output/events.jsonl"
    ev_count = 0
    dup_count = 0
    if os.path.exists(events_file):
        with open(events_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                result = await db.insert_event(event, source="listener")
                if result:
                    ev_count += 1
                else:
                    dup_count += 1
        print(f"✅ events (listener): {ev_count} записей ({dup_count} дубликатов)")
    else:
        print(f"⚠️  {events_file} не найден")

    # ═══════════════════════════════════════
    # 5. output/quick_events.json → events (quick_scan)
    # ═══════════════════════════════════════
    quick_file = "output/quick_events.json"
    q_count = 0
    q_dup = 0
    if os.path.exists(quick_file):
        with open(quick_file) as f:
            quick_events = json.load(f)
        for event in quick_events:
            # Адаптируем формат: _chat → _meta
            if "_chat" in event and "_meta" not in event:
                event["_meta"] = {
                    "chat_title": event.pop("_chat", ""),
                    "detected_at": datetime.now().isoformat(),
                }
            result = await db.insert_event(event, source="quick_scan")
            if result:
                q_count += 1
            else:
                q_dup += 1
        print(f"✅ events (quick_scan): {q_count} записей ({q_dup} дубликатов)")
    else:
        print(f"⚠️  {quick_file} не найден")

    # ═══════════════════════════════════════
    # 6. output/test_report.json → test_runs
    # ═══════════════════════════════════════
    report_file = "output/test_report.json"
    if os.path.exists(report_file):
        with open(report_file) as f:
            report = json.load(f)
        run_id = await db.save_test_run(report)
        print(f"✅ test_runs: 1 запись (id={run_id})")
    else:
        print(f"⚠️  {report_file} не найден")

    # ═══════════════════════════════════════
    # Итоги
    # ═══════════════════════════════════════
    print(f"\n{'=' * 40}")
    chat_count = await db.pool.fetchval("SELECT count(*) FROM chats")
    disc_count = await db.pool.fetchval("SELECT count(*) FROM discovered_chats")
    venue_count = await db.pool.fetchval("SELECT count(*) FROM venues")
    event_total = await db.pool.fetchval("SELECT count(*) FROM events")
    test_count = await db.pool.fetchval("SELECT count(*) FROM test_runs")

    print(f"📊 Итого в PostgreSQL:")
    print(f"   chats:            {chat_count}")
    print(f"   discovered_chats: {disc_count}")
    print(f"   venues:           {venue_count}")
    print(f"   events:           {event_total}")
    print(f"   test_runs:        {test_count}")
    print(f"{'=' * 40}")
    print("✅ Миграция завершена!")

    await db.close()


if __name__ == "__main__":
    asyncio.run(migrate())
