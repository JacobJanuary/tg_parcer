#!/usr/bin/env python3
"""
Real-time Telegram Listener — слушает выбранные чаты,
обнаруживает ивенты (фильтры + Gemini AI), находит новые группы (Spider).

Асинхронная архитектура:
  - Приём сообщений НЕ блокируется
  - AI + Venue обработка вынесена в background tasks
  - Spider resolve запускается сразу при обнаружении

Использование:
    python listener.py
"""

import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import User

import chats
import config
import filters
from db import Database
from display import Colors, format_timestamp, print_event

logger = logging.getLogger(__name__)


# ─── Дедупликатор ивентов ───

_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ0-9]+")


class EventDedup:
    """
    Двухуровневая дедупликация:
      1. Exact hash — title+date+location (быстрый отсев)
      2. Fuzzy tokens — Jaccard similarity ≥ 0.6 при совпадающей дате
    """

    SIMILARITY_THRESHOLD = 0.6  # 60% совпадение токенов

    def __init__(self):
        self._exact: set[str] = set()
        self._events: list[dict] = []  # для fuzzy

    @staticmethod
    def _normalize(text: str) -> str:
        return (text or "").lower().strip()

    @staticmethod
    def _tokenize(title: str) -> set[str]:
        """Токенизация → стемминг (первые 5 символов) для русской морфологии."""
        tokens = _TOKEN_RE.findall(title.lower())
        # Стемминг: берём первые 5 символов, чтобы «настолки» ≈ «настольные»
        return {t[:5] for t in tokens if len(t) > 1}

    @staticmethod
    def _jaccard(a: set, b: set) -> float:
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)

    def _exact_key(self, ev: dict) -> str:
        return (
            self._normalize(ev.get("title", ""))
            + "|" + (ev.get("date") or "")
            + "|" + self._normalize(ev.get("location_name", ""))
        )

    def _dates_compatible(self, d1: str, d2: str) -> bool:
        """True если даты совпадают или хотя бы одна TBD."""
        if d1 == "TBD" or d2 == "TBD" or not d1 or not d2:
            return True
        return d1 == d2

    def is_duplicate(self, event: dict) -> bool:
        """Проверяет и регистрирует ивент. True = дубликат."""
        # Layer 1: exact hash
        key = self._exact_key(event)
        if key in self._exact:
            return True
        self._exact.add(key)

        # Layer 2: fuzzy token match
        tokens = self._tokenize(event.get("title", ""))
        date = event.get("date", "")

        for stored in self._events:
            if not self._dates_compatible(date, stored["date"]):
                continue
            sim = self._jaccard(tokens, stored["tokens"])
            if sim >= self.SIMILARITY_THRESHOLD:
                logger.debug(
                    f"Fuzzy dedup: «{event.get('title')}» ≈ «{stored['title']}» "
                    f"(sim={sim:.2f})"
                )
                return True

        # Не дубль — запоминаем
        self._events.append({
            "title": event.get("title", ""),
            "date": date,
            "tokens": tokens,
        })
        return False


# ─── Утилиты медиа ───

def has_photo(media) -> bool:
    from telethon.tl.types import MessageMediaPhoto
    return isinstance(media, MessageMediaPhoto)


# ─── Основная логика ───

async def main():
    # 0. PostgreSQL
    db = Database(config.get_dsn())
    try:
        await db.connect()
        print(f"🐘 PostgreSQL подключён")
    except Exception as e:
        print(f"❌ PostgreSQL недоступен: {e}")
        sys.exit(1)

    # 1. Загрузка чатов из PostgreSQL (primary) + fallback на JSON
    selected = await chats.load_from_db(db)
    if selected:
        print(f"📋 Загружены чаты из PostgreSQL: {len(selected)}")
    else:
        # Fallback на JSON (миграция)
        selected = chats.load()
        if selected:
            print(f"📋 Загружены чаты из JSON (fallback): {len(selected)}")
            # Синхронизируем в PG
            await chats.save_to_db(db, selected)
    if not selected:
        print("❌ Нет активных чатов ни в БД, ни в JSON.")
        print("   Запустите: python list_chats.py --select")
        sys.exit(1)

    # Добавляем approved spider chats из discovered_chats
    existing_ids = {c["id"] for c in selected}
    try:
        approved = await db.get_all_discovered(status="approved")
        for row in approved:
            cid = row.get("chat_id")
            if cid and cid not in existing_ids:
                selected.append({
                    "id": cid,
                    "title": row.get("title") or str(cid),
                    "type": row.get("type") or "megagroup",
                })
                existing_ids.add(cid)
    except Exception as e:
        print(f"⚠️  Spider chats load: {e}")

    chat_ids = [c["id"] for c in selected]
    print(f"📋 Итого чатов для мониторинга: {len(selected)}")
    for c in selected:
        print(f"   • {c['title']} (ID: {c['id']})")
        await db.upsert_chat(c["id"], c["title"], c.get("type", "megagroup"))

    # 2. Инициализация Spider
    spider = None
    try:
        from spider import ChatSpider
        spider = ChatSpider(db=db)
        await spider.load_from_pg()
        print(f"🕷️ Spider активирован ({len(spider.discovered)} в базе, PG: {'✅' if spider._pg_loaded else '❌ JSON fallback'})")
    except Exception as e:
        print(f"⚠️  Spider недоступен: {e}")

    # 2b. Spider Bot
    spider_bot = None
    if spider and config.BOT_TOKEN:
        try:
            from spider_bot import SpiderBot
            spider_bot = SpiderBot(client, db=db)
            if await spider_bot.start():
                print(f"🤖 Spider Bot активирован (канал: {config.SPIDER_CHANNEL_ID})")
            else:
                spider_bot = None
        except Exception as e:
            print(f"⚠️  Spider Bot недоступен: {e}")
            spider_bot = None

    # 3. Инициализация AI
    try:
        from ai_analyzer import EventAnalyzer
        analyzer = EventAnalyzer()
        print(f"🤖 Gemini AI активирован (model: {analyzer.model})")
    except Exception as e:
        print(f"❌ Gemini AI недоступен: {e}")
        sys.exit(1)

    # 3b. Инициализация Venue Enricher
    venue_enricher = None
    try:
        from venue_enricher import VenueEnricher
        venue_enricher = VenueEnricher(db=db)
        await venue_enricher.cache.load_from_pg()
        print(f"📍 Venue Enricher активирован ({len(venue_enricher.cache)} в кэше)")
    except Exception as e:
        print(f"⚠️  Venue Enricher недоступен: {e}")

    # 3c. Инициализация Image Generator
    image_generator = None
    try:
        from image_generator import EventImageGenerator
        image_generator = EventImageGenerator(db=db)
        print("🎨 Image Generator активирован (Imagen 3)")
    except Exception as e:
        print(f"⚠️  Image Generator недоступен: {e}")

    # 4. Авторизация Telegram
    api_id, api_hash, phone = config.validate()
    client = TelegramClient(config.SESSION_NAME, api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"\n✅ Авторизован: {me.first_name} (@{me.username or 'N/A'})")

    # 5. Резолв чатов
    resolved_chats = await chats.resolve(client, chat_ids)
    if not resolved_chats:
        print("❌ Ни один чат не найден.")
        await client.disconnect()
        return



    # Счётчики
    msg_count = 0
    filtered_count = 0
    event_count = 0
    dup_count = 0
    spider_count = 0

    # Дедупликатор ивентов
    dedup = EventDedup()

    # Активные background tasks (чтобы не были GC'd)
    _bg_tasks: set[asyncio.Task] = set()

    def _fire_and_forget(coro):
        """Запускает корутину как background task, не блокируя цикл."""
        task = asyncio.create_task(coro)
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)

    # ─── Spider: авто-резолв обнаружений ───

    # Очередь для авто-резолва спайдера
    _spider_resolve_queue: asyncio.Queue = asyncio.Queue()

    async def _spider_resolve_worker():
        """Background worker: резолвит обнаружения из очереди."""
        while True:
            disc = await _spider_resolve_queue.get()
            try:
                username = disc.username
                invite_link = disc.invite_link
                chat_id = disc.chat_id
                resolved_dc = None  # track resolved DiscoveredChat

                if username:
                    entity = await client.get_entity(username)
                    from telethon.tl.types import Channel, Chat as TChat
                    from telethon.tl.functions.channels import GetFullChannelRequest
                    # Обновляем запись в spider
                    for dc in spider.discovered:
                        if dc.username and dc.username.lower() == username.lower():
                            dc.chat_id = entity.id
                            dc.title = getattr(entity, "title", None)
                            dc.participants_count = getattr(entity, "participants_count", None)
                            dc.resolved = True
                            if isinstance(entity, Channel):
                                dc.type = "channel" if entity.broadcast else "megagroup"
                                if dc.participants_count is None:
                                    try:
                                        full = await client(GetFullChannelRequest(entity))
                                        dc.participants_count = full.full_chat.participants_count
                                    except Exception:
                                        pass
                            elif isinstance(entity, TChat):
                                dc.type = "group"
                            else:
                                dc.status = "rejected"
                                dc.type = "user"
                            # Сохраняем ID в known_keys
                            spider.known_keys.add(f"id:{entity.id}")
                            label = dc.title or username
                            members = f" ({dc.participants_count} уч.)" if dc.participants_count else ""
                            print(f"  {Colors.MAGENTA}🕷️ Resolved: {label} ({dc.type}){members}{Colors.RESET}")
                            resolved_dc = dc
                            break

                elif invite_link:
                    from telethon.tl.functions.messages import CheckChatInviteRequest
                    link = invite_link
                    if "/+" in link:
                        invite_hash = link.split("/+")[-1]
                    elif "/joinchat/" in link:
                        invite_hash = link.split("/joinchat/")[-1]
                    else:
                        continue

                    result = await client(CheckChatInviteRequest(hash=invite_hash))
                    for dc in spider.discovered:
                        if dc.invite_link == invite_link:
                            if hasattr(result, "chat"):
                                chat_obj = result.chat
                                dc.chat_id = chat_obj.id
                                dc.title = getattr(chat_obj, "title", None)
                                dc.participants_count = getattr(chat_obj, "participants_count", None)
                            elif hasattr(result, "title"):
                                dc.title = result.title
                                dc.participants_count = getattr(result, "participants_count", None)
                            dc.resolved = True
                            print(f"  {Colors.MAGENTA}🕷️ Invite resolved: {dc.title}{Colors.RESET}")
                            resolved_dc = dc
                            break

                spider.save()

                # Уведомляем через бота (только resolved чаты)
                if spider_bot and resolved_dc and resolved_dc.resolved and resolved_dc.status != "rejected":
                    await spider_bot.notify_new_chat(resolved_dc)

            except Exception as e:
                logger.debug(f"Spider auto-resolve error: {e}")

            await asyncio.sleep(1)  # не флудим Telegram API

    # ─── Auto-join worker ───

    async def _auto_join_worker():
        """Background worker: проверяет вступил ли user в обнаруженные чаты."""
        while True:
            await asyncio.sleep(config.AUTO_JOIN_CHECK_INTERVAL)

            try:
                # Получаем чаты со status=approved и resolved=true
                pending = await db.get_all_discovered(status="approved")
                resolved_pending = [r for r in pending if r.get("resolved") and r.get("chat_id")]

                if not resolved_pending:
                    continue

                # Получаем текущие диалоги (кэшированный вызов)
                dialogs = await client.get_dialogs(limit=None)
                my_chat_ids = {d.entity.id for d in dialogs if hasattr(d.entity, "id")}

                for row in resolved_pending:
                    chat_id = row["chat_id"]
                    if chat_id in my_chat_ids:
                        # Пользователь вступил!
                        title = row.get("title") or str(chat_id)
                        chat_type = row.get("type") or "megagroup"

                        # 1. Обновляем статус в discovered (чтобы больше не обрабатывать)
                        await db.update_discovered(row["id"], status="monitoring")

                        # 2. Добавляем в active chats
                        await db.upsert_chat(
                            chat_id=chat_id,
                            title=title,
                            chat_type=chat_type,
                            is_active=True,
                        )

                        # 3. Добавляем в TG папку
                        try:
                            entity = await client.get_entity(chat_id)
                            await _add_to_folder(entity)
                        except Exception as e:
                            logger.debug(f"Folder add error for {title}: {e}")

                        # 4. Обновляем spider in-memory
                        if spider:
                            for dc in spider.discovered:
                                if dc.chat_id == chat_id:
                                    dc.status = "monitoring"
                                    break
                            spider.known_keys.add(f"id:{chat_id}")

                        # 5. Динамическая регистрация в мониторинге
                        try:
                            client.add_event_handler(
                                on_new_message,
                                events.NewMessage(chats=[entity]),
                            )
                            print(f"  {Colors.GREEN}🕷️ Auto-joined: {title} → мониторинг активирован{Colors.RESET}")
                        except Exception as eh_err:
                            logger.debug(f"Event handler registration error for {title}: {eh_err}")
                            print(f"  {Colors.GREEN}🕷️ Auto-joined: {title} → добавлен в БД{Colors.RESET}")

            except Exception as e:
                logger.debug(f"Auto-join worker error: {e}")

    async def _add_to_folder(entity):
        """Добавить чат в TG папку."""
        folder_name = config.TG_FOLDER_NAME
        if not folder_name:
            return

        try:
            from telethon.tl.functions.messages import (
                GetDialogFiltersRequest,
                UpdateDialogFilterRequest,
            )
            from telethon.tl.types import (
                DialogFilter,
                InputPeerChannel,
                InputPeerChat,
            )

            result = await client(GetDialogFiltersRequest())
            filters_list = result.filters if hasattr(result, 'filters') else result

            target_filter = None
            for f in filters_list:
                if isinstance(f, DialogFilter) and f.title == folder_name:
                    target_filter = f
                    break

            input_peer = await client.get_input_entity(entity)

            if target_filter:
                # Проверяем что чат ещё не в папке
                existing_ids = set()
                for p in target_filter.include_peers:
                    if hasattr(p, "channel_id"):
                        existing_ids.add(p.channel_id)
                    elif hasattr(p, "chat_id"):
                        existing_ids.add(p.chat_id)

                entity_id = getattr(input_peer, "channel_id", None) or getattr(input_peer, "chat_id", None)
                if entity_id in existing_ids:
                    return  # уже в папке

                target_filter.include_peers.append(input_peer)
                await client(UpdateDialogFilterRequest(
                    id=target_filter.id,
                    filter=target_filter,
                ))
                logger.info(f"Folder: добавлен {getattr(entity, 'title', '?')} в '{folder_name}'")
            else:
                # Создаём новую папку
                import random
                new_filter = DialogFilter(
                    id=random.randint(10, 255),
                    title=folder_name,
                    include_peers=[input_peer],
                    exclude_peers=[],
                    pinned_peers=[],
                )
                await client(UpdateDialogFilterRequest(
                    id=new_filter.id,
                    filter=new_filter,
                ))
                logger.info(f"Folder: создана папка '{folder_name}' с {getattr(entity, 'title', '?')}")

        except Exception as e:
            logger.debug(f"Folder error: {e}")

    # ─── Async обработка ивента (AI + Venue) ───

    async def _process_event(text: str, chat_title: str, event_obj, filter_score: int = 0):
        """Background task: AI анализ + venue enrichment + save."""
        nonlocal event_count, dup_count

        try:
            ai_result = await analyzer.analyze(text, chat_title)
            if ai_result and ai_result.get("is_event"):
                # Дедупликация (быстрая, не блокирует)
                if dedup.is_duplicate(ai_result):
                    dup_count += 1
                    return

                # Venue Enrichment (может быть медленным, но мы в background)
                if venue_enricher:
                    try:
                        await venue_enricher.enrich_event(ai_result)
                    except Exception as ve:
                        logger.error(f"Venue enrich error: {ve}")

                # Normalize TBD dates to None
                if ai_result.get("date") in ("TBD", "N/A", "", None):
                    ai_result["date"] = None
                if ai_result.get("time") in ("TBD", "N/A", "", None):
                    ai_result["time"] = None

                event_count += 1

                sender = await event_obj.get_sender()
                if isinstance(sender, User):
                    sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                else:
                    sender_name = getattr(sender, "title", "?")

                chat = await event_obj.get_chat()
                ai_result["_meta"] = {
                    "chat_id": chat.id,
                    "chat_title": chat_title,
                    "message_id": event_obj.id,
                    "sender": sender_name,
                    "filter_score": filter_score,
                    "detected_at": datetime.now().isoformat(),
                    "original_text": text,
                }

                print_event(ai_result, chat_title)

                # PostgreSQL
                try:
                    event_id, is_new, has_image = await db.insert_event(ai_result, source="listener")
                    if event_id and image_generator and (is_new or not has_image):
                        # Запускаем генерацию обложки не блокируя цикл (до 10 секунд на Imagen)
                        _fire_and_forget(image_generator.generate_cover(
                            raw_tg_text=text,
                            category=ai_result.get("category", "Party"),
                            event_id=event_id
                        ))
                except Exception as db_err:
                    logger.error(f"DB insert error: {db_err}")



        except Exception as e:
            print(f"  {Colors.RED}  🤖 ошибка: {e}{Colors.RESET}")

    # ─── Обработчик сообщений (БЫСТРЫЙ, не блокирует) ───

    @client.on(events.NewMessage(chats=resolved_chats))
    async def on_new_message(event):
        nonlocal msg_count, filtered_count, spider_count
        msg_count += 1

        chat = await event.get_chat()
        chat_title = getattr(chat, "title", "Личный чат")
        text = event.text or ""

        # Spider: обнаружение (sync, мгновенное)
        if spider:
            try:
                new_found = spider.process_message(event.message, chat_title)
                if new_found:
                    spider_count += len(new_found)
                    for d in new_found:
                        label = d.title or d.username or d.invite_link or str(d.chat_id)
                        print(f"  {Colors.MAGENTA}🕷️ Новый: {label} [{d.source_type}]{Colors.RESET}")
                        # Авто-резолв: отправляем в очередь
                        await _spider_resolve_queue.put(d)
                        # Сохраняем в PG
                        try:
                            found_in_id = chat.id if hasattr(chat, 'id') else None
                            await db.upsert_discovered(
                                chat_id=d.chat_id,
                                username=d.username,
                                invite_link=d.invite_link,
                                title=d.title,
                                source_type=d.source_type,
                                found_in_chat_id=found_in_id,
                            )
                        except Exception as db_err:
                            logger.error(f"DB spider insert error: {db_err}")
                    if spider_count % 30 == 0:
                        spider.save()
            except Exception as e:
                logger.debug(f"Spider error: {e}")

        # Фильтр (sync, мгновенный)
        has_media = has_photo(event.media) if event.media else False
        filter_result = filters.check(text, has_media)

        if not filter_result.passed:
            filtered_count += 1
            return

        # Быстрая хэш-сверка с БД (защита от спама) чтобы не тратить ИИ
        if db:
            try:
                if await db.is_text_exists(text):
                    print(f"  {Colors.YELLOW}⏭️ Пропуск спама (текст уже в базе): {chat_title}{Colors.RESET}")
                    return
            except Exception as e:
                logger.debug(f"DB text deduplication error: {e}")

        print(f"  {Colors.YELLOW}⏳ Извлечение: {chat_title}{Colors.RESET}")
        _fire_and_forget(_process_event(text, chat_title, event, filter_result.score))

    # ─── Background tasks ───

    if spider:
        # Запуск spider resolve worker
        _fire_and_forget(_spider_resolve_worker())

        # Запуск auto-join worker
        _fire_and_forget(_auto_join_worker())

        # Resolve pending immediately at startup
        try:
            count = await spider.resolve_pending(client)
            if count > 0:
                print(f"  {Colors.MAGENTA}🕷️ Initial resolve: {count} чатов{Colors.RESET}")
        except Exception as e:
            logger.debug(f"Spider initial resolve error: {e}")

    # ─── Heartbeat worker ───

    _start_time = datetime.now()

    async def _heartbeat_worker():
        """Печатает статистику каждые 60 секунд."""
        while True:
            await asyncio.sleep(60)
            uptime = datetime.now() - _start_time
            mins = int(uptime.total_seconds()) // 60
            print(
                f"💓 [{mins}m] msgs={msg_count} filtered={filtered_count} "
                f"events={event_count} dups={dup_count} spider={spider_count}"
            )

    _fire_and_forget(_heartbeat_worker())

    # ─── Запуск ───

    ai_label = f"🤖 {analyzer.model}"
    spider_label = "🕷️ SPIDER" if spider else ""
    venue_label = f"📍 VENUE ({len(venue_enricher.cache)})" if venue_enricher else ""
    db_events = await db.get_event_count()

    print(f"\n{'=' * 60}")
    print(f" 🎯 EVENT DETECTION  │  {ai_label}  {spider_label}  {venue_label}")
    print(f" 🐘 PostgreSQL: {db_events} ивентов в базе")
    print(f" Слушаю {len(resolved_chats)} чат(ов)")
    print(f" ⚡ Async mode: AI + Venue в background")
    print(f" Нажмите Ctrl+C для остановки")
    print(f"{'=' * 60}\n")

    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        pass
    finally:

        analyzer.close()
        if venue_enricher:
            venue_enricher.close()
        if spider:
            spider.save()
        if spider_bot:
            await spider_bot.stop()
        await db.close()
        print(f"\n\n{'=' * 40}")
        print(f"📊 Итого:")
        print(f"   Сообщений:       {msg_count}")
        print(f"   Прошло фильтр:   {filtered_count}")
        print(f"   Ивентов найдено: {event_count}")
        if dup_count > 0:
            print(f"   Дубликатов:      {dup_count}")
        if spider:
            print(f"   🕷️ Новых чатов:   {spider_count}")
        analyzer.print_stats()
        if venue_enricher:
            print(f"   📍 Venue: {venue_enricher.stats}")
        print(f"{'=' * 40}")
        print("🔌 Сессия завершена.")


if __name__ == "__main__":
    asyncio.run(main())
