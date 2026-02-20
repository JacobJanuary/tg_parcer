"""
Основной модуль парсинга чатов Telegram через Telethon.
"""

import os
import asyncio
from datetime import datetime

from telethon import TelegramClient
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    Channel,
    Chat,
    User,
    PeerChannel,
    PeerChat,
)
from tqdm import tqdm

import config


class TelegramParser:
    """Парсер чатов Telegram."""

    def __init__(self):
        api_id, api_hash, phone = config.validate()
        self.phone = phone
        self.client = TelegramClient(config.SESSION_NAME, api_id, api_hash)

    async def connect(self):
        """Подключение и авторизация."""
        await self.client.start(phone=self.phone)
        me = await self.client.get_me()
        print(f"✅ Авторизован как: {me.first_name} {me.last_name or ''} (@{me.username or 'N/A'})")
        return self

    async def disconnect(self):
        """Отключение от Telegram."""
        await self.client.disconnect()
        print("🔌 Сессия завершена.")

    async def resolve_chat(self, chat_identifier: str):
        """
        Резолвим чат по username, ссылке или ID.
        Поддерживаемые форматы:
          - @username
          - https://t.me/username
          - https://t.me/+invite_hash
          - -100XXXXXXXXXX (ID)
          - XXXXXXXXXX (числовой ID)
        """
        # Убираем t.me ссылки -> username или invite
        if "t.me/" in chat_identifier:
            # Извлекаем часть после t.me/
            part = chat_identifier.split("t.me/")[-1].strip("/")
            if part.startswith("+"):
                # Invite ссылка — нужно join или получить через invite hash
                from telethon.tl.functions.messages import CheckChatInviteRequest
                try:
                    result = await self.client(CheckChatInviteRequest(part[1:]))
                    if hasattr(result, 'chat'):
                        return result.chat
                    else:
                        print(f"⚠️ Для доступа к приватному чату по ссылке {chat_identifier} необходимо сначала вступить.")
                        return None
                except Exception as e:
                    print(f"❌ Ошибка при проверке инвайт-ссылки: {e}")
                    return None
            else:
                chat_identifier = part

        # Числовой ID
        try:
            chat_id = int(chat_identifier)
            entity = await self.client.get_entity(chat_id)
            return entity
        except ValueError:
            pass

        # Username (с или без @)
        if chat_identifier.startswith("@"):
            chat_identifier = chat_identifier[1:]

        try:
            entity = await self.client.get_entity(chat_identifier)
            return entity
        except Exception as e:
            print(f"❌ Не удалось найти чат '{chat_identifier}': {e}")
            return None

    async def get_chat_info(self, entity) -> dict:
        """Получение метаданных чата."""
        info = {
            "id": entity.id,
            "title": getattr(entity, "title", None),
            "username": getattr(entity, "username", None),
            "type": "unknown",
            "participants_count": None,
        }

        if isinstance(entity, Channel):
            if entity.megagroup:
                info["type"] = "megagroup"
            elif entity.broadcast:
                info["type"] = "channel"
            else:
                info["type"] = "channel"
            try:
                full = await self.client.get_participants(entity, limit=0)
                info["participants_count"] = full.total
            except Exception:
                info["participants_count"] = getattr(entity, "participants_count", None)
        elif isinstance(entity, Chat):
            info["type"] = "group"
            info["participants_count"] = getattr(entity, "participants_count", None)
        elif isinstance(entity, User):
            info["type"] = "user"
            info["title"] = f"{entity.first_name or ''} {entity.last_name or ''}".strip()

        return info

    async def parse_messages(
        self,
        entity,
        limit: int = None,
        offset_date: datetime = None,
        download_media: bool = False,
        media_types: list = None,
        output_dir: str = None,
    ) -> list:
        """
        Парсинг сообщений из чата.

        Args:
            entity: Telegram entity (чат/канал)
            limit: Максимальное кол-во сообщений (None = все)
            offset_date: Парсить сообщения до этой даты
            download_media: Скачивать ли медиафайлы
            media_types: Список типов медиа для скачивания ('photo', 'video', 'document')
            output_dir: Директория для сохранения медиа

        Returns:
            Список словарей с данными сообщений.
        """
        if media_types is None:
            media_types = ["photo", "video", "document"]

        messages_data = []
        media_dir = None

        if download_media and output_dir:
            media_dir = os.path.join(output_dir, config.DEFAULT_MEDIA_DIR)
            os.makedirs(media_dir, exist_ok=True)

        # Считаем общее кол-во для прогресс-бара
        total = limit
        if total is None:
            # Пробуем посчитать примерное кол-во
            try:
                async for _ in self.client.iter_messages(entity, limit=1):
                    pass
                # Telethon не предоставляет total напрямую, ставим None
                total = None
            except Exception:
                total = None

        print(f"\n📥 Начинаю парсинг сообщений...")
        progress = tqdm(
            total=total,
            desc="Сообщения",
            unit="msg",
            dynamic_ncols=True,
        )

        async for message in self.client.iter_messages(
            entity,
            limit=limit,
            offset_date=offset_date,
        ):
            msg_data = self._extract_message_data(message)

            # Скачивание медиа
            if download_media and message.media and media_dir:
                media_info = await self._download_media_file(
                    message, media_dir, media_types
                )
                if media_info:
                    msg_data["media_type"] = media_info["type"]
                    msg_data["media_file"] = media_info["file"]

            messages_data.append(msg_data)
            progress.update(1)

        progress.close()
        print(f"✅ Спарсено сообщений: {len(messages_data)}")

        return messages_data

    def _extract_message_data(self, message) -> dict:
        """Извлечение данных из объекта сообщения."""
        sender_name = ""
        sender_id = None

        if message.sender:
            sender_id = message.sender_id
            if isinstance(message.sender, User):
                parts = [message.sender.first_name or "", message.sender.last_name or ""]
                sender_name = " ".join(p for p in parts if p)
            else:
                sender_name = getattr(message.sender, "title", "")

        # Определяем тип медиа (без скачивания)
        media_type = None
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                media_type = "photo"
            elif isinstance(message.media, MessageMediaDocument):
                doc = message.media.document
                if doc:
                    for attr in doc.attributes:
                        if isinstance(attr, DocumentAttributeVideo):
                            media_type = "video"
                            break
                        elif isinstance(attr, DocumentAttributeAudio):
                            media_type = "audio"
                            break
                    if media_type is None:
                        media_type = "document"

        return {
            "id": message.id,
            "date": message.date.isoformat() if message.date else None,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "text": message.text or "",
            "views": getattr(message, "views", None),
            "forwards": getattr(message, "forwards", None),
            "reply_to_msg_id": (
                message.reply_to.reply_to_msg_id
                if message.reply_to
                else None
            ),
            "media_type": media_type,
            "media_file": None,
        }

    async def _download_media_file(
        self, message, media_dir: str, media_types: list
    ) -> dict | None:
        """Скачивание медиафайла сообщения."""
        media = message.media

        if isinstance(media, MessageMediaPhoto) and "photo" in media_types:
            file_path = await self.client.download_media(
                message, file=media_dir
            )
            if file_path:
                return {"type": "photo", "file": os.path.basename(file_path)}

        elif isinstance(media, MessageMediaDocument) and media.document:
            doc = media.document
            detected_type = "document"
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    detected_type = "video"
                    break
                elif isinstance(attr, DocumentAttributeAudio):
                    detected_type = "audio"
                    break

            if detected_type in media_types:
                file_path = await self.client.download_media(
                    message, file=media_dir
                )
                if file_path:
                    return {"type": detected_type, "file": os.path.basename(file_path)}

        return None
