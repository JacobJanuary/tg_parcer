"""
Управление списком отслеживаемых чатов — загрузка, сохранение, резолв.
Первичный источник: PostgreSQL. JSON — fallback/backup.
"""

import json
import os


SELECTED_CHATS_FILE = "selected_chats.json"


def load(filepath: str = SELECTED_CHATS_FILE) -> list:
    """Загрузка выбранных чатов из JSON (fallback)."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save(chats: list, filepath: str = SELECTED_CHATS_FILE):
    """Сохранение списка чатов в JSON (backup)."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(chats, f, ensure_ascii=False, indent=2)


async def load_from_db(db) -> list:
    """Загрузка активных чатов из PostgreSQL."""
    rows = await db.get_active_chats()
    return [{"id": r["id"], "title": r["title"], "type": r.get("type", "")} for r in rows]


async def save_to_db(db, chats: list):
    """Синхронизация списка чатов в PostgreSQL."""
    for chat in chats:
        await db.upsert_chat(
            chat_id=chat["id"],
            title=chat.get("title", ""),
            chat_type=chat.get("type", ""),
            is_active=True,
        )


async def resolve(client, chat_ids: list) -> list:
    """
    Резолвит ID/username'ы в Telegram entity.
    Для числовых ID использует PeerChannel (каналы/супергруппы).

    Returns:
        Список entity для успешно найденных чатов.
    """
    from telethon.tl.types import PeerChannel, PeerChat

    resolved = []
    for chat_id in chat_ids:
        try:
            # Строковые ID (@username) → напрямую
            if isinstance(chat_id, str) and not chat_id.lstrip("-").isdigit():
                entity = await client.get_entity(chat_id)
            else:
                # Числовой ID → пробуем как канал/супергруппу
                numeric_id = int(chat_id)
                try:
                    entity = await client.get_entity(PeerChannel(numeric_id))
                except Exception:
                    try:
                        entity = await client.get_entity(PeerChat(numeric_id))
                    except Exception:
                        entity = await client.get_entity(numeric_id)

            title = getattr(entity, "title", None) or getattr(entity, "first_name", str(chat_id))
            resolved.append(entity)
            print(f"   ✓ {title}")
        except Exception as e:
            print(f"   ✗ Не удалось найти: {chat_id} ({e})")
    return resolved
