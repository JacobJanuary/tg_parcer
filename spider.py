#!/usr/bin/env python3
"""
Telegram Spider — автообнаружение новых групп и каналов.

Извлекает потенциальные источники из:
1. Forward — пересылки из других каналов/групп
2. Invite links — t.me/joinchat/..., t.me/+...
3. Public links — t.me/username

Использование:
    # Как модуль (в listener.py):
    from spider import ChatSpider
    spider = ChatSpider()
    discoveries = spider.process_message(message)

    # Тест на собранных сообщениях:
    python spider.py --test samples/raw_messages.jsonl
"""

import json
import os
import re
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Константы ───────────────────────────────────────────────────────────

DEFAULT_DB_PATH = "discovered_chats.json"

# Системные/служебные username'ы Telegram (не каналы)
TG_SYSTEM_USERNAMES = {
    "joinchat", "addstickers", "addemoji", "share", "socks",
    "proxy", "setlanguage", "bg", "addtheme", "invoice",
    "confirmphone", "login", "addlist", "boost", "giftcode",
    "iv", "contact", "telegrampassport",
    # Common false positives
    "gmail", "user", "admin", "here", "everyone",
    "channel", "group", "chat", "test", "info",
    "help", "support", "botfather", "username",
}

# Регулярки для извлечения ссылок
RE_INVITE_NEW = re.compile(r"t\.me/\+([A-Za-z0-9_-]+)")
RE_INVITE_OLD = re.compile(r"t\.me/joinchat/([A-Za-z0-9_-]+)")
RE_PUBLIC_LINK = re.compile(r"t\.me/([A-Za-z][A-Za-z0-9_]{3,30})\b")
# @mentions НЕ собираем — это почти всегда личные аккаунты, а не группы


# ─── Модели ──────────────────────────────────────────────────────────────

@dataclass
class Discovery:
    """Одно обнаружение потенциального источника."""
    source_type: str        # "forward", "invite_link", "public_link"
    title: Optional[str] = None
    username: Optional[str] = None
    chat_id: Optional[int] = None
    invite_link: Optional[str] = None
    found_in_chat: str = ""
    found_in_text: str = ""


@dataclass
class DiscoveredChat:
    """Запись об обнаруженном чате в базе."""
    # Идентификация (хотя бы одно из трёх)
    chat_id: Optional[int] = None
    username: Optional[str] = None
    invite_link: Optional[str] = None

    # Метаданные
    title: Optional[str] = None
    type: Optional[str] = None      # "channel", "megagroup", "group", None
    source_type: str = "unknown"     # "forward", "invite_link", "public_link"
    found_in_chat: str = ""

    # Трекинг
    first_seen: str = ""
    last_seen: str = ""
    times_seen: int = 0

    # Статус: "new", "approved", "rejected", "self"
    status: str = "new"

    # Resolved info (заполняется при resolve)
    resolved: bool = False
    participants_count: Optional[int] = None

    def match_key(self) -> str:
        """Уникальный ключ для дедупликации."""
        if self.chat_id:
            return f"id:{self.chat_id}"
        if self.username:
            return f"user:{self.username.lower()}"
        if self.invite_link:
            return f"invite:{self.invite_link}"
        return f"unknown:{self.title}"


# ─── Основной класс ─────────────────────────────────────────────────────

class ChatSpider:
    """Автообнаружение новых групп/каналов из потока сообщений."""

    def __init__(self, db=None):
        self.db = db  # asyncpg Database instance
        self.discovered: list[DiscoveredChat] = []
        self.known_keys: set[str] = set()
        self._key_index: dict[str, DiscoveredChat] = {}  # O(1) lookup
        self._pg_loaded = False

        logger.info("Spider: инициализирован (ожидание load_from_pg)")

    # ─── Загрузка / Сохранение ───

    async def load_from_pg(self):
        """Загрузка discovered_chats из PostgreSQL (primary)."""
        if not self.db:
            return

        try:
            rows = await self.db.get_all_discovered()
            if not rows:
                logger.info("Spider: PG discovered_chats пуст, оставляем JSON")
                return

            # Полностью перезагружаем из PG
            self.discovered = []
            self.known_keys = set()
            self._key_index = {}

            for r in rows:
                dc = DiscoveredChat(
                    chat_id=r.get("chat_id"),
                    username=r.get("username"),
                    invite_link=r.get("invite_link"),
                    title=r.get("title"),
                    type=r.get("type"),
                    source_type=r.get("source_type", "unknown"),
                    found_in_chat=r.get("found_in_chat", ""),
                    first_seen=str(r.get("first_seen", "")),
                    last_seen=str(r.get("last_seen", "")),
                    times_seen=r.get("times_seen", 0),
                    status=r.get("status", "new"),
                    resolved=r.get("resolved", False),
                    participants_count=r.get("participants_count"),
                )
                self.discovered.append(dc)
                key = dc.match_key()
                self.known_keys.add(key)
                self._key_index[key] = dc

            # Также добавим активные чаты в known_keys (замена _load_selected)
            active_chats = await self.db.get_active_chats()
            for c in active_chats:
                self.known_keys.add(f"id:{c['id']}")

            self._pg_loaded = True
            logger.info(f"Spider: загружено из PG: {len(self.discovered)} discovered, {len(active_chats)} active")

        except Exception as e:
            logger.warning(f"Spider: ошибка загрузки из PG: {e}, используем JSON")

    def save(self):
        """No-op: данные сохраняются в PG в реальном времени."""
        pass

    # ─── Обработка сообщений ───

    def process_message(self, message, chat_title: str = "") -> list[Discovery]:
        """
        Извлекает потенциальные источники из одного сообщения.
        Вызывается ДО фильтров, для каждого входящего сообщения.

        Returns:
            Список новых (ранее неизвестных) обнаружений.
        """
        discoveries = []

        # 1. Forward — пересылка из канала/группы
        fwd = self._check_forward(message, chat_title)
        if fwd:
            discoveries.append(fwd)

        # 2. Ссылки в тексте
        text = getattr(message, "text", None) or getattr(message, "message", None) or ""
        if text:
            discoveries.extend(self._extract_links(text, chat_title))

        # 3. Регистрируем новые
        new_discoveries = []
        for disc in discoveries:
            if self._register(disc):
                new_discoveries.append(disc)

        return new_discoveries

    def _check_forward(self, message, chat_title: str) -> Optional[Discovery]:
        """Проверяет, является ли сообщение пересылкой из канала/группы."""
        fwd = getattr(message, "forward", None) or getattr(message, "fwd_from", None)
        if not fwd:
            return None

        # Telethon: forward.chat для resolved, fwd_from.from_id для raw
        chat = getattr(fwd, "chat", None)
        from_id = getattr(fwd, "from_id", None)

        if chat:
            return Discovery(
                source_type="forward",
                title=getattr(chat, "title", None),
                username=getattr(chat, "username", None),
                chat_id=getattr(chat, "id", None),
                found_in_chat=chat_title,
            )
        elif from_id:
            # PeerChannel или PeerChat
            channel_id = getattr(from_id, "channel_id", None)
            chat_id = getattr(from_id, "chat_id", None)
            if channel_id or chat_id:
                return Discovery(
                    source_type="forward",
                    chat_id=channel_id or chat_id,
                    found_in_chat=chat_title,
                )
        return None

    def _extract_links(self, text: str, chat_title: str) -> list[Discovery]:
        """Извлекает ссылки и @упоминания из текста."""
        results = []
        seen = set()  # дедупликация в рамках одного сообщения

        # Invite links (новый формат: t.me/+xxx)
        for m in RE_INVITE_NEW.finditer(text):
            link = f"t.me/+{m.group(1)}"
            if link not in seen:
                seen.add(link)
                results.append(Discovery(
                    source_type="invite_link",
                    invite_link=link,
                    found_in_chat=chat_title,
                    found_in_text=text[:200],
                ))

        # Invite links (старый формат: t.me/joinchat/xxx)
        for m in RE_INVITE_OLD.finditer(text):
            link = f"t.me/joinchat/{m.group(1)}"
            if link not in seen:
                seen.add(link)
                results.append(Discovery(
                    source_type="invite_link",
                    invite_link=link,
                    found_in_chat=chat_title,
                    found_in_text=text[:200],
                ))

        # Public links (t.me/username)
        for m in RE_PUBLIC_LINK.finditer(text):
            username = m.group(1).lower()
            if username in TG_SYSTEM_USERNAMES or username in seen:
                continue
            # Пропускаем если это часть invite-ссылки
            start = m.start()
            prefix = text[max(0, start - 5):start]
            if "+" in prefix or "joinchat" in text[max(0, start - 15):start]:
                continue
            seen.add(username)
            results.append(Discovery(
                source_type="public_link",
                username=username,
                found_in_chat=chat_title,
                found_in_text=text[:200],
            ))


        return results

    def _register(self, disc: Discovery) -> bool:
        """Регистрирует обнаружение. Возвращает True если это НОВЫЙ источник."""
        now = datetime.now().isoformat()

        # Формируем ключ для дедупликации
        if disc.chat_id:
            key = f"id:{disc.chat_id}"
        elif disc.username:
            key = f"user:{disc.username.lower()}"
        elif disc.invite_link:
            key = f"invite:{disc.invite_link}"
        else:
            return False

        if key in self.known_keys:
            # O(1) обновление times_seen через dict
            dc = self._key_index.get(key)
            if dc:
                dc.times_seen += 1
                dc.last_seen = now
            return False

        # Новое обнаружение!
        dc = DiscoveredChat(
            chat_id=disc.chat_id,
            username=disc.username,
            invite_link=disc.invite_link,
            title=disc.title,
            source_type=disc.source_type,
            found_in_chat=disc.found_in_chat,
            first_seen=now,
            last_seen=now,
            times_seen=1,
            status="new",
        )
        self.discovered.append(dc)
        self.known_keys.add(key)
        self._key_index[key] = dc

        label = disc.title or disc.username or disc.invite_link or str(disc.chat_id)
        logger.info(f"🕷️ Новый: {label} ({disc.source_type} из {disc.found_in_chat})")

        return True

    # ─── Resolve ───

    async def resolve_pending(self, client) -> int:
        """
        Резолвит unresolved записи через Telegram API.
        Обрабатывает: username'ы, public links, invite links.

        Returns:
            Количество успешно зарезолвленных.
        """
        import asyncio
        from telethon.tl.types import Channel, Chat
        from telethon.tl.functions.messages import CheckChatInviteRequest
        from telethon.tl.functions.channels import GetFullChannelRequest

        # 1. Resolve username'ы и public links
        to_resolve_users = [
            dc for dc in self.discovered
            if not dc.resolved and dc.username and dc.status == "new"
        ]

        # 2. Resolve invite links
        to_resolve_invites = [
            dc for dc in self.discovered
            if not dc.resolved and dc.invite_link and dc.status == "new"
        ]

        total = to_resolve_users + to_resolve_invites
        if not total:
            return 0

        resolved_count = 0

        # --- Username'ы ---
        for dc in to_resolve_users[:10]:
            try:
                entity = await client.get_entity(dc.username)
                dc.chat_id = entity.id
                dc.title = getattr(entity, "title", None)
                dc.participants_count = getattr(entity, "participants_count", None)
                dc.resolved = True

                if isinstance(entity, Channel):
                    dc.type = "channel" if entity.broadcast else "megagroup"
                    # get_entity часто не возвращает participants_count —
                    # дополнительный запрос для точных данных
                    if dc.participants_count is None:
                        try:
                            full = await client(GetFullChannelRequest(entity))
                            dc.participants_count = full.full_chat.participants_count
                        except Exception:
                            pass
                elif isinstance(entity, Chat):
                    dc.type = "group"
                else:
                    dc.status = "rejected"
                    dc.type = "user"
                    logger.debug(f"Spider: {dc.username} — это юзер, пропускаем")
                    continue

                id_key = f"id:{dc.chat_id}"
                if id_key in self.known_keys:
                    dc.status = "self"
                else:
                    self.known_keys.add(id_key)

                resolved_count += 1
                logger.info(f"🕷️ Resolved: {dc.title} (@{dc.username}) — {dc.type}, {dc.participants_count} уч.")

            except Exception as e:
                logger.debug(f"Spider: не удалось resolve @{dc.username}: {e}")
                dc.resolved = True
                dc.status = "rejected"

            await asyncio.sleep(1)

        # --- Invite links ---
        for dc in to_resolve_invites[:10]:
            try:
                # Извлекаем hash из ссылки
                link = dc.invite_link
                if "/+" in link:
                    invite_hash = link.split("/+")[-1]
                elif "/joinchat/" in link:
                    invite_hash = link.split("/joinchat/")[-1]
                else:
                    dc.resolved = True
                    continue

                result = await client(CheckChatInviteRequest(hash=invite_hash))

                # ChatInviteAlready = мы уже в этом чате
                if hasattr(result, "chat"):
                    chat = result.chat
                    dc.chat_id = chat.id
                    dc.title = getattr(chat, "title", None)
                    dc.participants_count = getattr(chat, "participants_count", None)
                    dc.resolved = True

                    if isinstance(chat, Channel):
                        dc.type = "channel" if chat.broadcast else "megagroup"
                    else:
                        dc.type = "group"

                    # Уже состоим
                    id_key = f"id:{dc.chat_id}"
                    if id_key in self.known_keys:
                        dc.status = "self"
                    else:
                        self.known_keys.add(id_key)

                    resolved_count += 1
                    logger.info(f"🕷️ Invite resolved (already in): {dc.title} — {dc.participants_count} уч.")

                # ChatInvite = мы НЕ в чате, но видим инфо
                elif hasattr(result, "title"):
                    dc.title = result.title
                    dc.participants_count = getattr(result, "participants_count", None)
                    dc.resolved = True
                    dc.type = "channel" if getattr(result, "broadcast", False) else "megagroup"
                    resolved_count += 1
                    logger.info(f"🕷️ Invite resolved: {dc.title} — {dc.participants_count} уч.")

            except Exception as e:
                err = str(e)
                if "INVITE_HASH_EXPIRED" in err:
                    dc.resolved = True
                    dc.status = "rejected"
                    dc.title = "(ссылка истекла)"
                    logger.debug(f"Spider: invite {dc.invite_link} expired")
                else:
                    logger.debug(f"Spider: не удалось resolve invite {dc.invite_link}: {e}")
                    dc.resolved = True

            await asyncio.sleep(1)

        self.save()
        return resolved_count

    # ─── Статистика ───

    def get_pending(self) -> list[DiscoveredChat]:
        """Возвращает чаты со статусом 'new' для ревью."""
        return [dc for dc in self.discovered if dc.status == "new"]

    def get_stats(self) -> dict:
        """Статистика по базе."""
        by_status = {}
        by_source = {}
        for dc in self.discovered:
            by_status[dc.status] = by_status.get(dc.status, 0) + 1
            by_source[dc.source_type] = by_source.get(dc.source_type, 0) + 1
        return {
            "total": len(self.discovered),
            "by_status": by_status,
            "by_source": by_source,
        }


# ─── CLI: тест на raw_messages.jsonl ─────────────────────────────────────

def test_on_file(filepath: str):
    """Прогон spider по файлу с сообщениями."""
    from types import SimpleNamespace

    spider = ChatSpider()
    total = 0
    new_count = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            total += 1
            try:
                msg_data = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Создаём фейковый message объект
            msg = SimpleNamespace(
                text=msg_data.get("text", ""),
                message=msg_data.get("text", ""),
                forward=None,
                fwd_from=None,
            )

            # Если есть forward info в данных
            fwd_from = msg_data.get("fwd_from") or msg_data.get("forward")
            if fwd_from and isinstance(fwd_from, dict):
                fwd_chat = SimpleNamespace(
                    id=fwd_from.get("channel_id") or fwd_from.get("chat_id"),
                    title=fwd_from.get("title"),
                    username=fwd_from.get("username"),
                )
                if fwd_chat.id:
                    msg.forward = SimpleNamespace(chat=fwd_chat, from_id=None)

            chat_title = msg_data.get("chat_title", "?")
            discoveries = spider.process_message(msg, chat_title)
            new_count += len(discoveries)

            for d in discoveries:
                label = d.title or d.username or d.invite_link or str(d.chat_id)
                print(f"  🕷️ [{d.source_type}] {label} (из {d.found_in_chat})")

    spider.save()

    print(f"\n{'=' * 50}")
    print(f"📊 Spider Test Results:")
    print(f"   Сообщений: {total}")
    print(f"   Новых обнаружений: {new_count}")

    stats = spider.get_stats()
    print(f"   Всего в базе: {stats['total']}")
    if stats["by_source"]:
        print(f"   По источникам:")
        for src, cnt in sorted(stats["by_source"].items()):
            print(f"     {src}: {cnt}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    p = argparse.ArgumentParser(description="🕷️ Telegram Spider — тест")
    p.add_argument("--test", metavar="FILE", help="Прогнать по JSONL-файлу")
    args = p.parse_args()

    if args.test:
        if not os.path.exists(args.test):
            print(f"❌ Файл не найден: {args.test}")
            sys.exit(1)
        test_on_file(args.test)
    else:
        p.print_help()
