"""
Spider Bot — уведомления о новых чатах в Telegram канал.

Отправляет карточки обнаруженных чатов с inline кнопками.
"""

import asyncio
import logging
from typing import Optional

from telethon import TelegramClient, events
from telethon.tl.types import Channel

import config

logger = logging.getLogger(__name__)


class SpiderBot:
    """Бот для уведомлений Spider в приватный канал."""

    def __init__(self, client: TelegramClient, db=None):
        self.client = client
        self.db = db
        self.channel_id = config.SPIDER_CHANNEL_ID
        self._bot_client: Optional[TelegramClient] = None

    async def start(self):
        """Инициализация бот-клиента."""
        if not config.BOT_TOKEN:
            logger.warning("BOT_TOKEN не задан, Spider Bot отключён")
            return False

        if not self.channel_id:
            logger.warning("SPIDER_CHANNEL_ID не задан, Spider Bot отключён")
            return False

        try:
            self._bot_client = TelegramClient(
                "spider_bot_session",
                int(config.API_ID),
                config.API_HASH,
            )
            await self._bot_client.start(bot_token=config.BOT_TOKEN)
            _register_callbacks(self._bot_client, self.db)

            me = await self._bot_client.get_me()
            logger.info(f"Spider Bot запущен: @{me.username}")
            return True

        except Exception as e:
            logger.error(f"Spider Bot ошибка запуска: {e}")
            self._bot_client = None
            return False

    async def stop(self):
        """Остановка бот-клиента."""
        if self._bot_client:
            await self._bot_client.disconnect()

    async def notify_new_chat(self, dc) -> bool:
        """Отправить карточку обнаруженного чата в канал."""
        if not self._bot_client or not self.channel_id:
            return False

        try:
            text = format_card(dc)
            buttons = make_buttons(dc)
            await self._bot_client.send_message(
                self.channel_id,
                text,
                buttons=buttons,
                parse_mode="html",
            )
            logger.info(f"Spider Bot: уведомление — {dc.title or dc.username}")
            return True

        except Exception as e:
            logger.error(f"Spider Bot: ошибка отправки: {e}")
            return False


def format_card(dc) -> str:
    """Форматирование карточки чата."""
    lines = ["🕷️ <b>Новый чат обнаружен!</b>\n"]

    if dc.title:
        lines.append(f"📌 <b>{dc.title}</b>")

    if dc.participants_count:
        lines.append(f"👥 Участников: <b>{dc.participants_count:,}</b>")

    if dc.type:
        type_emoji = {"channel": "📢", "megagroup": "💬", "group": "👥"}.get(dc.type, "❓")
        lines.append(f"{type_emoji} Тип: {dc.type}")

    if dc.username:
        lines.append(f"🔗 @{dc.username}")
    elif dc.invite_link:
        lines.append(f"🔗 {dc.invite_link}")
    elif dc.chat_id:
        lines.append(f"🆔 ID: {dc.chat_id}")

    if dc.found_in_chat:
        lines.append(f"📍 Источник: {dc.found_in_chat}")

    if dc.source_type:
        source_labels = {
            "forward": "📨 Пересылка",
            "invite_link": "🔗 Invite-ссылка",
            "public_link": "🌐 Публичная ссылка",
        }
        lines.append(f"🏷️ {source_labels.get(dc.source_type, dc.source_type)}")

    if dc.times_seen > 1:
        lines.append(f"👁️ Встречался: {dc.times_seen} раз")

    lines.append("\n→ <i>Вступи чтобы добавить в мониторинг</i>")

    return "\n".join(lines)


def make_buttons(dc):
    """Inline кнопки для карточки."""
    from telethon.tl.custom import Button

    key = dc.match_key()
    return [
        [
            Button.inline("✅ Подписался", data=f"joined:{key}".encode()),
            Button.inline("❌ Отклонить", data=f"reject:{key}".encode()),
        ]
    ]


def _register_callbacks(bot_client, db):
    """Регистрация callback handlers для inline кнопок."""

    @bot_client.on(events.CallbackQuery(pattern=b"reject:"))
    async def on_reject(event):
        try:
            data = event.data.decode()
            key = data.split(":", 1)[1]

            if db:
                row = await _find_discovered_by_key(db, key)
                if row:
                    await db.update_discovered(row["id"], status="rejected")

            msg = await event.get_message()
            await event.edit(
                msg.text + "\n\n❌ <b>Отклонено</b>",
                buttons=None,
                parse_mode="html",
            )
            await event.answer("Чат отклонён")
            print(f"  🔴 Reject callback: {key}")
            logger.info(f"Reject callback: {key}")

        except Exception as e:
            logger.error(f"Reject callback error: {e}")
            await event.answer(f"Ошибка: {e}")

    @bot_client.on(events.CallbackQuery(pattern=b"joined:"))
    async def on_joined(event):
        try:
            data = event.data.decode()
            key = data.split(":", 1)[1]

            if db:
                row = await _find_discovered_by_key(db, key)
                if row:
                    await db.update_discovered(row["id"], status="approved")

            msg = await event.get_message()
            await event.edit(
                msg.text + "\n\n✅ <b>Подписано! Удаляю через 2 сек...</b>",
                buttons=None,
                parse_mode="html",
            )
            await event.answer("Отмечено как подписанный")
            print(f"  🟢 Joined callback: {key}")
            logger.info(f"Joined callback: {key}")

            await asyncio.sleep(2)
            await msg.delete()

        except Exception as e:
            logger.error(f"Joined callback error: {e}")
            await event.answer(f"Ошибка: {e}")


async def _find_discovered_by_key(db, key: str):
    """Найти discovered_chat по match_key."""
    if key.startswith("id:"):
        chat_id = int(key.split(":")[1])
        return await db.pool.fetchrow(
            "SELECT id FROM discovered_chats WHERE chat_id = $1", chat_id
        )
    elif key.startswith("user:"):
        username = key.split(":")[1]
        return await db.pool.fetchrow(
            "SELECT id FROM discovered_chats WHERE lower(username) = $1",
            username.lower(),
        )
    elif key.startswith("invite:"):
        invite = key.split(":", 1)[1]
        return await db.pool.fetchrow(
            "SELECT id FROM discovered_chats WHERE invite_link = $1", invite
        )
    return None
