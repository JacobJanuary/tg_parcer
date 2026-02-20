#!/usr/bin/env python3
"""
Ревью обнаруженных чатов — просмотр и управление базой spider.

Использование:
    python review_discovered.py              # Показать новые + интерактивный ревью
    python review_discovered.py --all        # Показать все (включая rejected)
    python review_discovered.py --stats      # Только статистика
"""

import argparse
import asyncio
import json
import os

from telethon import TelegramClient

import config
from db import Database
from display import Colors, SOURCE_ICONS, STATUS_ICONS
from spider import ChatSpider, DiscoveredChat


def print_stats(spider: ChatSpider):
    """Вывод статистики."""
    stats = spider.get_stats()
    print(f"\n{'=' * 50}")
    print(f"  🕷️ Spider Database Stats")
    print(f"{'=' * 50}")
    print(f"  Всего записей: {stats['total']}")

    if stats["by_status"]:
        print(f"\n  По статусу:")
        for status, cnt in sorted(stats["by_status"].items()):
            icon = STATUS_ICONS.get(status, "❓")
            print(f"    {icon} {status}: {cnt}")

    if stats["by_source"]:
        print(f"\n  По источнику:")
        for src, cnt in sorted(stats["by_source"].items()):
            icon = SOURCE_ICONS.get(src, "❓")
            print(f"    {icon} {src}: {cnt}")
    print(f"{'=' * 50}")


def display_chats(chats: list[DiscoveredChat], show_status: bool = False):
    """Вывод списка обнаруженных чатов."""
    if not chats:
        print("\n  (пусто)")
        return

    for i, dc in enumerate(chats, 1):
        src_icon = SOURCE_ICONS.get(dc.source_type, "❓")

        # Основная строка: название + тип + подписчики
        if dc.title and dc.title != "(ссылка истекла)":
            name = dc.title
            if dc.username:
                name += f" (@{dc.username})"
        elif dc.username:
            name = f"@{dc.username}"
        elif dc.invite_link:
            name = dc.invite_link
        else:
            name = str(dc.chat_id) or "?"

        # Тип
        type_str = f" [{dc.type}]" if dc.type else ""

        # Участники — ключевая инфо
        if dc.participants_count:
            members_str = f" 👥 {dc.participants_count:,}"
        else:
            members_str = ""

        # Resolved?
        resolve_mark = ""
        if not dc.resolved and (dc.username or dc.invite_link):
            resolve_mark = f" {Colors.YELLOW}❓{Colors.RESET}"

        # Статус
        status_str = ""
        if show_status:
            status_icon = STATUS_ICONS.get(dc.status, "❓")
            status_str = f" {status_icon}"

        # Вывод основной строки
        print(
            f"  {Colors.BOLD}{i:3d}{Colors.RESET}. "
            f"{src_icon} {Colors.CYAN}{name}{Colors.RESET}"
            f"{Colors.DIM}{type_str}{Colors.RESET}"
            f"{Colors.GREEN}{Colors.BOLD}{members_str}{Colors.RESET}"
            f"{resolve_mark}{status_str}"
        )

        # Вторая строка: откуда найдено
        seen_str = f" ×{dc.times_seen}" if dc.times_seen > 1 else ""
        print(
            f"       {Colors.DIM}← {dc.found_in_chat}{seen_str}{Colors.RESET}"
        )


def interactive_review(spider: ChatSpider, db=None):
    """Интерактивный ревью новых чатов."""
    pending = spider.get_pending()

    if not pending:
        print(f"\n  {Colors.GREEN}✅ Нет новых чатов для ревью!{Colors.RESET}")
        return

    print(f"\n{'=' * 60}")
    print(f"  🕷️ Новые обнаруженные чаты: {len(pending)}")
    print(f"{'=' * 60}")

    display_chats(pending)

    print(f"\n{'─' * 60}")
    print(f"  Команды:")
    print(f"    a 1,3,5-8    → approve (добавить в мониторинг)")
    print(f"    r 1,3,5-8    → reject (скрыть)")
    print(f"    aa           → approve all")
    print(f"    q            → выход")
    print(f"{'─' * 60}")

    while True:
        try:
            raw = input(f"\n  ▶ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n")
            break

        if not raw or raw.lower() == "q":
            break

        if raw.lower() == "aa":
            for dc in pending:
                if dc.status == "new":
                    dc.status = "approved"
            spider.save()
            _sync_approved(spider, db)
            print(f"  ✅ Все {len(pending)} чатов approved!")
            break

        # Парсинг команды: "a 1,3,5" или "r 2,4"
        parts = raw.split(maxsplit=1)
        if len(parts) != 2 or parts[0] not in ("a", "r"):
            print(f"  ⚠️ Формат: a 1,3,5 или r 2,4")
            continue

        action = parts[0]
        indices = _parse_indices(parts[1])

        for idx in indices:
            if 1 <= idx <= len(pending):
                dc = pending[idx - 1]
                if action == "a":
                    dc.status = "approved"
                    print(f"  ✅ {dc.title or dc.username or dc.invite_link}")
                else:
                    dc.status = "rejected"
                    print(f"  ❌ {dc.title or dc.username or dc.invite_link}")
            else:
                print(f"  ⚠️ Номер {idx} вне диапазона")

        spider.save()

        if action == "a":
            _sync_approved(spider, db)


def _parse_indices(raw: str) -> list[int]:
    """Парсинг индексов: 1,3,5-8"""
    indices = []
    for part in raw.replace(",", " ").split():
        part = part.strip()
        if "-" in part:
            try:
                a, b = part.split("-", 1)
                indices.extend(range(int(a), int(b) + 1))
            except ValueError:
                pass
        elif part.isdigit():
            indices.append(int(part))
    return indices


def _sync_approved(spider: ChatSpider, db=None):
    """Добавляет approved чаты в PG (primary) и selected_chats.json (backup)."""
    selected_path = spider.selected_path

    # JSON backup
    existing = []
    if os.path.exists(selected_path):
        try:
            with open(selected_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except json.JSONDecodeError:
            existing = []

    existing_ids = {item["id"] for item in existing}

    added = 0
    for dc in spider.discovered:
        if dc.status == "approved" and dc.chat_id and dc.chat_id not in existing_ids:
            chat_data = {
                "id": dc.chat_id,
                "title": dc.title or dc.username or str(dc.chat_id),
                "type": dc.type or "megagroup",
            }
            existing.append(chat_data)
            existing_ids.add(dc.chat_id)
            added += 1

            # PG: upsert chat + update discovered status
            if db:
                try:
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(db.upsert_chat(
                        chat_id=dc.chat_id,
                        title=chat_data["title"],
                        chat_type=chat_data["type"],
                        is_active=True,
                    ))
                    loop.run_until_complete(db.update_discovered(
                        dc.id if hasattr(dc, 'id') else 0,
                        status="approved",
                    ))
                except Exception as e:
                    print(f"  ⚠️ PG sync error: {e}")

    if added > 0:
        with open(selected_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"\n  💾 JSON: +{added} чат(ов) в {selected_path}")
        if db:
            print(f"  🐘 PG: +{added} чат(ов) синхронизировано")


async def run(args):
    """Основная async-логика: подключение к TG, resolve, ревью."""
    spider = ChatSpider()

    # PostgreSQL
    db = None
    try:
        db = Database(config.get_dsn())
        await db.connect()
        print("🐘 PostgreSQL подключён")
    except Exception as e:
        print(f"⚠️  PostgreSQL недоступен: {e} (продолжаем без PG)")
        db = None

    # Пробуем подключиться к Telegram для resolve и dedup
    client = None
    try:
        api_id, api_hash, phone = config.validate()
        client = TelegramClient("tg_review_session", api_id, api_hash)
        await client.start(phone=phone)

        me = await client.get_me()
        print(f"✅ Авторизован: {me.first_name}")

        # ─── Загрузка всех диалогов пользователя ───
        print("📋 Загрузка списка ваших групп...")
        my_ids = set()
        my_usernames = set()

        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            my_ids.add(entity.id)
            username = getattr(entity, "username", None)
            if username:
                my_usernames.add(username.lower())

        print(f"   Найдено: {len(my_ids)} диалогов")

        # ─── Пометить дубли как 'self' ───
        marked_self = 0
        for dc in spider.discovered:
            if dc.status not in ("new",):
                continue
            if dc.chat_id and dc.chat_id in my_ids:
                dc.status = "self"
                marked_self += 1
                continue
            if dc.username and dc.username.lower() in my_usernames:
                dc.status = "self"
                marked_self += 1
                continue

        if marked_self > 0:
            spider.save()
            print(f"   📌 Уже в ваших чатах: {marked_self} (помечены как 'self')")

        # ─── Авто-resolve unresolved ───
        unresolved = [
            dc for dc in spider.discovered
            if not dc.resolved and dc.username and dc.status == "new"
        ]
        if unresolved and not args.stats:
            print(f"\n🔍 Резолвлю {len(unresolved)} username'ов...")
            count = await spider.resolve_pending(client)
            if count > 0:
                for dc in spider.discovered:
                    if dc.status == "new" and dc.chat_id and dc.chat_id in my_ids:
                        dc.status = "self"
                spider.save()
                print(f"   ✅ Зарезолвлено: {count}")

        await client.disconnect()

    except Exception as e:
        err_msg = str(e)
        print(f"⚠️  Ошибка подключения к Telegram: {type(e).__name__}: {err_msg}")
        print(f"   Работаю в offline-режиме (resolve и dedup недоступны)")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass

    # ─── Вывод ───
    if args.stats:
        print_stats(spider)
    elif args.all:
        print(f"\n  📋 Все записи ({len(spider.discovered)}):")
        display_chats(spider.discovered, show_status=True)
    else:
        interactive_review(spider, db=db)

    print_stats(spider)

    # Close DB
    if db:
        await db.close()


def main():
    p = argparse.ArgumentParser(description="🕷️ Ревью обнаруженных чатов")
    p.add_argument("--all", action="store_true", help="Показать все записи")
    p.add_argument("--stats", action="store_true", help="Только статистика")
    args = p.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()

