#!/usr/bin/env python3
"""
Показывает все диалоги (группы, каналы, личные чаты) в аккаунте.
Позволяет выбрать чаты для прослушивания в реальном времени.

Использование:
    python list_chats.py              # Показать все диалоги
    python list_chats.py --groups     # Только группы
    python list_chats.py --channels   # Только каналы
    python list_chats.py --all        # Всё (включая личные чаты)
"""

import argparse
import asyncio
import json
import os

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

import config
from db import Database


async def get_all_dialogs(client: TelegramClient, filter_type: str = "groups") -> list:
    """
    Получение всех диалогов аккаунта.

    Args:
        client: Авторизованный TelegramClient
        filter_type: 'groups', 'channels', 'all'

    Returns:
        Список словарей с информацией о диалогах.
    """
    dialogs = []

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        info = {
            "id": entity.id,
            "title": dialog.title or "—",
            "username": getattr(entity, "username", None),
            "type": "unknown",
            "unread": dialog.unread_count,
            "participants": None,
        }

        if isinstance(entity, Channel):
            if entity.megagroup:
                info["type"] = "megagroup"
            else:
                info["type"] = "channel"
            info["participants"] = getattr(entity, "participants_count", None)
        elif isinstance(entity, Chat):
            info["type"] = "group"
            info["participants"] = getattr(entity, "participants_count", None)
        elif isinstance(entity, User):
            info["type"] = "user"
            name_parts = [entity.first_name or "", entity.last_name or ""]
            info["title"] = " ".join(p for p in name_parts if p) or "—"
        else:
            continue

        # Фильтрация
        if filter_type == "groups" and info["type"] not in ("group", "megagroup"):
            continue
        elif filter_type == "channels" and info["type"] != "channel":
            continue
        # 'all' — без фильтрации

        dialogs.append(info)

    return dialogs


def load_selected_ids(filepath: str = "selected_chats.json") -> set:
    """Загрузка ID уже выбранных чатов."""
    if not os.path.exists(filepath):
        return set()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {item["id"] for item in data}
    except (json.JSONDecodeError, KeyError):
        return set()


def display_dialogs(dialogs: list):
    """Красивый вывод списка диалогов с отметкой выбранных."""
    type_icons = {
        "megagroup": "👥",
        "group": "👥",
        "channel": "📢",
        "user": "👤",
    }
    type_labels = {
        "megagroup": "Супергруппа",
        "group": "Группа",
        "channel": "Канал",
        "user": "Личный чат",
    }

    selected_ids = load_selected_ids()
    selected_count = 0

    print(f"\n{'#':<4} {'':2} {'Название':<40} {'Тип':<14} {'Участники':>10}  {'ID'}")
    print("─" * 95)

    for i, d in enumerate(dialogs, 1):
        icon = type_icons.get(d["type"], "❓")
        label = type_labels.get(d["type"], d["type"])
        participants = f"{d['participants']:,}" if d["participants"] else "—"
        username = f" (@{d['username']})" if d["username"] else ""
        title = (d["title"][:37] + "...") if len(d["title"]) > 40 else d["title"]

        if d["id"] in selected_ids:
            mark = " ✅"
            selected_count += 1
        else:
            mark = ""

        print(f"{i:<4} {icon} {title + username:<40} {label:<14} {participants:>10}  {d['id']}{mark}")

    if selected_count > 0:
        print(f"\n   ✅ = уже в selected_chats.json ({selected_count} шт.)")


def select_chats(dialogs: list) -> list | None:
    """Интерактивный выбор чатов для прослушивания (с поддержкой add/remove)."""
    selected_ids = load_selected_ids()

    print(f"\n{'=' * 60}")
    print("📋 Управление подписками на чаты")
    print(f"   Сейчас выбрано: {len(selected_ids)} чат(ов)")
    print(f"{'=' * 60}")
    print("   Числа (1,3,5-8)  → ДОБАВИТЬ к текущим")
    print("   -1,-3,-5          → УБРАТЬ из текущих")
    print("   'all'             → выбрать все")
    print("   'reset'           → сбросить и выбрать заново")
    print("   'q'               → выйти без изменений")

    while True:
        choice = input("\n▶ Ваш выбор: ").strip()

        if choice.lower() == 'q':
            return None  # None = no changes

        if choice.lower() == 'all':
            return dialogs

        if choice.lower() == 'reset':
            print("🔄 Список сброшен. Введите новые номера:")
            selected_ids.clear()
            continue

        try:
            add_indices = set()
            remove_indices = set()

            for part in choice.replace(",", " ").split():
                part = part.strip()
                if not part:
                    continue

                is_remove = part.startswith("-")
                if is_remove:
                    part = part[1:]

                target = remove_indices if is_remove else add_indices

                if "-" in part:
                    a, b = part.split("-", 1)
                    for n in range(int(a), int(b) + 1):
                        target.add(n)
                else:
                    target.add(int(part))

            # Строим результат
            # Начинаем с текущих выбранных
            result_ids = set(selected_ids)

            for idx in add_indices:
                if 1 <= idx <= len(dialogs):
                    result_ids.add(dialogs[idx - 1]["id"])
                else:
                    print(f"  ⚠️  Номер {idx} вне диапазона")

            for idx in remove_indices:
                if 1 <= idx <= len(dialogs):
                    result_ids.discard(dialogs[idx - 1]["id"])
                else:
                    print(f"  ⚠️  Номер {idx} вне диапазона")

            if not result_ids:
                print("❌ Список пуст. Добавьте хотя бы один чат.")
                continue

            # Собираем финальный список
            result = [d for d in dialogs if d["id"] in result_ids]
            return result

        except ValueError:
            print("❌ Некорректный ввод. Используйте числа через запятую.")


def save_selection(selected: list, filepath: str = "selected_chats.json"):
    """Сохранение выбранных чатов в JSON (backup)."""
    data = [{"id": d["id"], "title": d["title"], "type": d["type"]} for d in selected]


    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n💾 JSON backup: {filepath}")
    return filepath


async def save_selection_to_db(db, selected: list):
    """Сохранение выбранных чатов в PostgreSQL (primary)."""
    for d in selected:
        await db.upsert_chat(
            chat_id=d["id"],
            title=d["title"],
            chat_type=d.get("type", ""),
            is_active=True,
        )
    print(f"🐘 Сохранено в PostgreSQL: {len(selected)} чатов")


async def main():
    p = argparse.ArgumentParser(description="📋 Список диалогов Telegram")
    p.add_argument("--groups", action="store_true", help="Только группы")
    p.add_argument("--channels", action="store_true", help="Только каналы")
    p.add_argument("--all", action="store_true", help="Все чаты (включая личные)")
    p.add_argument("--select", action="store_true", help="Интерактивный выбор для прослушивания")
    args = p.parse_args()

    if args.channels:
        filter_type = "channels"
    elif args.all:
        filter_type = "all"
    else:
        filter_type = "groups"

    # Авторизация
    api_id, api_hash, phone = config.validate()
    client = TelegramClient(config.SESSION_NAME, api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"✅ Авторизован: {me.first_name} (@{me.username or 'N/A'})")

    # Получаем диалоги
    print(f"\n🔄 Загружаю список диалогов...")
    dialogs = await get_all_dialogs(client, filter_type)

    if not dialogs:
        print("⚠️  Диалоги не найдены.")
        await client.disconnect()
        return

    print(f"📊 Найдено: {len(dialogs)} диалогов")
    display_dialogs(dialogs)

    # Интерактивный выбор
    if args.select:
        selected = select_chats(dialogs)
        if selected is not None and selected:
            print(f"\n✅ Выбрано чатов: {len(selected)}")
            for d in selected:
                print(f"   • {d['title']} (ID: {d['id']})")
            # Сохранение в PostgreSQL (primary)
            db = None
            try:
                db = Database(config.get_dsn())
                await db.connect()
                await save_selection_to_db(db, selected)
            except Exception as e:
                print(f"⚠️  PG save error: {e}")
            finally:
                if db:
                    await db.close()
            print(f"\n💡 Запустите listener: python listener.py")
        elif selected is None:
            print("👋 Без изменений.")
        else:
            print("Выход.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
