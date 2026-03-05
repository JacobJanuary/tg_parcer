#!/usr/bin/env python3
"""
Импорт чатов из approved_chats.json в discovered_chats + отправка карточек Spider Bot.

Логика:
  1. Читает approved_chats.json
  2. Проверяет каждый чат: есть ли уже в discovered_chats / chats
  3. Новые вставляет в discovered_chats (status='new', resolved=True)
  4. Отправляет карточки через Spider Bot в SPIDER_CHANNEL_ID
  5. Пользователь вступает → жмёт ✅ → spider_daemon меняет status → добавляет в мониторинг

Использование:
    python import_chats.py /path/to/approved_chats.json [--dry-run]
"""

import asyncio
import json
import sys
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

sys.path.insert(0, "/home/elcrypto/TG_parcer")

import config
import db as db_module
from spider import DiscoveredChat
from spider_bot import format_card, make_buttons


async def main():
    parser = argparse.ArgumentParser(description="🕷️ Импорт чатов в Spider Bot")
    parser.add_argument("file", help="Путь к approved_chats.json")
    parser.add_argument("--dry-run", action="store_true", help="Только проверка, без записи в БД и отправки")
    args = parser.parse_args()

    # ─── Загрузка файла ───
    with open(args.file, "r", encoding="utf-8") as f:
        chats = json.load(f)

    print(f"📂 Загружено: {len(chats)} чатов из {args.file}")

    # ─── Подключение к БД ───
    database = db_module.Database(config.get_dsn())
    await database.connect(min_size=1, max_size=2)

    # ─── Собираем уже известные чаты ───
    existing_chat_ids = set()
    existing_usernames = set()

    # Из chats (активные)
    active = await database.get_active_chats()
    for c in active:
        existing_chat_ids.add(c["id"])

    # Из discovered_chats
    discovered = await database.get_all_discovered()
    for d in discovered:
        if d.get("chat_id"):
            existing_chat_ids.add(d["chat_id"])
        if d.get("username"):
            existing_usernames.add(d["username"].lower())

    print(f"📊 Уже известно: {len(existing_chat_ids)} chat_ids, {len(existing_usernames)} usernames")

    # ─── Фильтрация ───
    new_chats = []
    skipped = []

    for c in chats:
        chat_id = c.get("id")
        username = (c.get("username") or "").lower()

        if chat_id and chat_id in existing_chat_ids:
            skipped.append((c.get("title", "?"), "already known by ID"))
            continue
        if username and username in existing_usernames:
            skipped.append((c.get("title", "?"), "already known by username"))
            continue

        new_chats.append(c)

    print(f"\n{'='*50}")
    print(f"✅ Новых чатов: {len(new_chats)}")
    print(f"⏭️  Пропущено: {len(skipped)}")

    if skipped:
        print(f"\n📋 Пропущенные:")
        for title, reason in skipped:
            print(f"   ⏭️  {title} — {reason}")

    if new_chats:
        print(f"\n📋 Новые для импорта:")
        for c in new_chats:
            members = c.get("members", "?")
            print(f"   🆕 {c.get('title', '?')} (@{c.get('username', '?')}) — {members} уч.")

    if not new_chats:
        print("\n✅ Все чаты уже известны!")
        await database.close()
        return

    if args.dry_run:
        print("\n🔍 DRY RUN — ничего не записано")
        await database.close()
        return

    # ─── Вставка в discovered_chats ───
    print(f"\n{'='*50}")
    print(f"📝 Вставка в discovered_chats...")

    inserted = 0
    for c in new_chats:
        try:
            await database.upsert_discovered(
                username=c.get("username"),
                chat_id=c.get("id"),
                title=c.get("title"),
                chat_type=c.get("type"),
                source_type="public_link",
                participants_count=c.get("members"),
                resolved=True,
                status="new",
                increment_seen=False,
            )
            inserted += 1
            print(f"   ✅ {c.get('title', '?')}")
        except Exception as e:
            print(f"   ❌ {c.get('title', '?')}: {e}")

    print(f"\n📊 Вставлено: {inserted}/{len(new_chats)}")

    # ─── Отправка карточек через Spider Bot ───
    if not config.BOT_TOKEN or not config.SPIDER_CHANNEL_ID:
        print("\n⚠️  BOT_TOKEN / SPIDER_CHANNEL_ID не заданы — карточки не отправлены")
        await database.close()
        return

    print(f"\n{'='*50}")
    print(f"📤 Отправка карточек в Spider Bot...")

    from telethon import TelegramClient

    bot_client = TelegramClient(
        "import_bot_session",
        int(config.API_ID),
        config.API_HASH,
    )
    await bot_client.start(bot_token=config.BOT_TOKEN)

    me = await bot_client.get_me()
    print(f"🤖 Bot: @{me.username}")

    sent = 0
    for c in new_chats:
        try:
            dc = DiscoveredChat(
                chat_id=c.get("id"),
                username=c.get("username"),
                title=c.get("title"),
                type=c.get("type"),
                source_type="public_link",
                found_in_chat="📂 Импорт из approved_chats.json",
                times_seen=1,
                status="new",
                resolved=True,
                participants_count=c.get("members"),
            )

            text = format_card(dc)
            buttons = make_buttons(dc)

            await bot_client.send_message(
                config.SPIDER_CHANNEL_ID,
                text,
                buttons=buttons,
                parse_mode="html",
            )
            # Mark as notified so spider_notify cron won't re-send
            await database.upsert_discovered(
                username=c.get("username"),
                chat_id=c.get("id"),
                status="notified",
                increment_seen=False,
            )
            print(f"   📤 {dc.title or dc.username}")
            sent += 1
            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"   ❌ {c.get('title', '?')}: {e}")

    print(f"\n✅ Отправлено карточек: {sent}/{len(new_chats)}")
    print(f"🔌 Кнопки обрабатывает spider_daemon.py")

    await bot_client.disconnect()
    await database.close()


if __name__ == "__main__":
    asyncio.run(main())
