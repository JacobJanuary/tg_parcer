#!/usr/bin/env python3
"""
Управление группами — просмотр и выход из групп/каналов Telegram.

Использование:
    python leave_chats.py              # Список групп + интерактивный выход
    python leave_chats.py --channels   # Только каналы
    python leave_chats.py --groups     # Только группы
    python leave_chats.py --dry-run    # Показать список без возможности выхода
"""

import argparse
import asyncio
import sys
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.tl.functions.messages import DeleteChatUserRequest
from telethon.errors import FloodWaitError

import config
from display import Colors


async def leave_with_retry(client, entity, me, title: str, max_wait: int = 300):
    """Выход из чата с автоматической обработкой FloodWait."""
    for attempt in range(3):
        try:
            # delete_dialog корректно обрабатывает все типы чатов
            await client.delete_dialog(entity)
            return True
        except FloodWaitError as e:
            wait = e.seconds
            if wait > max_wait:
                print(f"  ⏳ {title}: ожидание {wait}с (>{max_wait}с лимит) — пропускаю")
                return False
            print(f"  ⏳ {title}: rate limit, жду {wait}с...", end="", flush=True)
            await asyncio.sleep(wait)
            print(" повтор")
        except Exception as e:
            print(f"  ❌ {title}: {e}")
            return False
    return False


async def main():
    p = argparse.ArgumentParser(description="🧹 Управление группами Telegram")
    p.add_argument("--channels", action="store_true", help="Только каналы")
    p.add_argument("--groups", action="store_true", help="Только группы/чаты")
    p.add_argument("--dry-run", action="store_true", help="Только показать список")
    p.add_argument("--max-wait", type=int, default=300, help="Макс. ожидание rate limit (сек)")
    args = p.parse_args()

    # Авторизация
    api_id, api_hash, phone = config.validate()
    client = TelegramClient(config.SESSION_NAME, api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"✅ Авторизован: {me.first_name} (@{me.username or 'N/A'})\n")

    # Сбор всех диалогов
    print("📋 Загрузка списка групп и каналов...")
    chats = []

    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        if isinstance(entity, Channel):
            chat_type = "📢 канал" if entity.broadcast else "👥 группа"
            if args.channels and not entity.broadcast:
                continue
            if args.groups and entity.broadcast:
                continue
        elif isinstance(entity, Chat):
            chat_type = "👥 мини-группа"
            if args.channels:
                continue
        else:
            continue

        members = getattr(entity, "participants_count", None)
        members_str = f"{members}" if members else "?"

        chats.append({
            "entity": entity,
            "title": dialog.title or "?",
            "type": chat_type,
            "members": members_str,
            "id": entity.id,
            "unread": dialog.unread_count,
            "date": dialog.date,
        })

    if not chats:
        print("Нет групп/каналов.")
        await client.disconnect()
        return

    # Вывод списка
    print(f"\n{'=' * 70}")
    print(f"  Найдено: {len(chats)} групп/каналов")
    print(f"{'=' * 70}\n")

    for i, c in enumerate(chats, 1):
        unread = f" 💬{c['unread']}" if c['unread'] > 0 else ""
        print(
            f"  {Colors.BOLD}{i:3d}{Colors.RESET}. "
            f"{c['type']} {Colors.CYAN}{c['title']}{Colors.RESET} "
            f"{Colors.DIM}({c['members']} уч.){Colors.RESET}"
            f"{Colors.YELLOW}{unread}{Colors.RESET}"
        )

    if args.dry_run:
        print(f"\n{Colors.DIM}(dry-run: выход из групп отключён){Colors.RESET}")
        await client.disconnect()
        return

    # Интерактивный выбор
    print(f"\n{'─' * 70}")
    print(f"  Введите номера групп для выхода через пробел или запятую.")
    print(f"  Диапазон: 1-5,8,12-15")
    print(f"  Пустая строка или 'q' — отмена.")
    print(f"{'─' * 70}")

    try:
        raw = input(f"\n  🔢 Номера: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\n❌ Отменено.")
        await client.disconnect()
        return

    if not raw or raw.lower() == 'q':
        print("👋 Выход без изменений.")
        await client.disconnect()
        return

    # Парсинг номеров (поддержка диапазонов: 1-5,8,12-15)
    selected_indices = set()
    for part in raw.replace(",", " ").split():
        part = part.strip()
        if "-" in part:
            try:
                a, b = part.split("-", 1)
                for n in range(int(a), int(b) + 1):
                    selected_indices.add(n)
            except ValueError:
                print(f"  ⚠️  Некорректный диапазон: {part}")
        elif part.isdigit():
            selected_indices.add(int(part))
        else:
            print(f"  ⚠️  Некорректный ввод: {part}")

    # Фильтруем валидные
    to_leave = []
    for idx in sorted(selected_indices):
        if 1 <= idx <= len(chats):
            to_leave.append(chats[idx - 1])
        else:
            print(f"  ⚠️  Номер {idx} вне диапазона (1-{len(chats)})")

    if not to_leave:
        print("Нечего удалять.")
        await client.disconnect()
        return

    # Подтверждение
    print(f"\n{'=' * 70}")
    print(f"  {Colors.RED}{Colors.BOLD}⚠️  Вы выходите из {len(to_leave)} групп:{Colors.RESET}")
    print(f"{'=' * 70}")

    for c in to_leave:
        print(f"  ❌ {c['type']} {Colors.RED}{c['title']}{Colors.RESET}")

    try:
        confirm = input(f"\n  Уверены? Введите 'yes' для подтверждения: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\n❌ Отменено.")
        await client.disconnect()
        return

    if confirm.lower() not in ("yes", "да", "y"):
        print("👋 Отменено.")
        await client.disconnect()
        return

    # Выход из групп
    print()
    left_count = 0
    for i, c in enumerate(to_leave, 1):
        entity = c["entity"]
        success = await leave_with_retry(client, entity, me, c["title"], args.max_wait)

        if success:
            left_count += 1
            print(f"  ✅ [{i}/{len(to_leave)}] Вышел: {c['title']}")

        await asyncio.sleep(2)  # 2с между запросами чтобы не словить flood

    print(f"\n{'=' * 40}")
    print(f"  📊 Вышел из {left_count}/{len(to_leave)} групп")
    print(f"{'=' * 40}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
