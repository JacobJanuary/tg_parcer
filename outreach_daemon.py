#!/usr/bin/env python3
"""
Outreach Daemon — автоматическое уведомление организаторов событий в DM.

Архитектура:
  1. LISTEN PostgreSQL → new_event_for_outreach
  2. Фильтрация (боты, кулдаун, rate-limit)
  3. Очередь отправки DM (round-robin между аккаунтами)
  4. AI Responder — слушает входящие DM и отвечает через Gemini

Запуск: python outreach_daemon.py [--dry-run]
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
from datetime import datetime, timedelta
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from google import genai
from telethon import TelegramClient, events
from telethon.tl.types import User

load_dotenv()

# ──── Logging ────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("outreach")

# ──── Config ────
DB_DSN = None  # built in main()

OUTREACH_ACCOUNTS = []  # populated in main()

COOLDOWN_DAYS = int(os.getenv("OUTREACH_COOLDOWN_DAYS", "7"))
DELAY_MIN = int(os.getenv("OUTREACH_DELAY_MIN", "180"))
DELAY_MAX = int(os.getenv("OUTREACH_DELAY_MAX", "420"))
DAILY_LIMIT = int(os.getenv("OUTREACH_DAILY_LIMIT", "20"))
APP_URL = os.getenv("APP_URL", "https://phangan.app")
EDIT_URL_TEMPLATE = os.getenv("EDIT_URL_TEMPLATE", "https://t.me/GoPhanganBot?startapp=event_{event_id}")
CHANNEL_URL = os.getenv("OUTREACH_CHANNEL_URL", "https://t.me/PhanganEvents")

DRY_RUN = "--dry-run" in sys.argv

# ──── First-contact message templates ────
TEMPLATES_RU = [
    "Привет! 👋 Увидел(а) твой ивент «{title}» в «{chat}» — круто звучит!\n"
    "Мы его уже закинули в Vibe Phangan — тут все события острова собраны в одном месте, "
    "3000+ человек каждый день свайпают ленту.\n"
    "Хочешь — скину ссылку, чтобы подправить описание, добавить фото или поменять время? Напиши! 🤙",

    "Йо! 👋 Заметил(а) «{title}» в чате «{chat}».\n"
    "Уже добавили в Vibe Phangan — приложение, где все ивенты Пангана собраны как карточки, "
    "свайпаешь вправо что нравится — и готов вайб на вечер 🌴\n"
    "Если хочешь поправить что-то в описании — дай знать, скину ссылку!",

    "Привет! ✌️ Видел(а) твоё событие «{title}» в «{chat}».\n"
    "Оно уже в Vibe Phangan — крупнейшем агрегаторе ивентов на острове. "
    "Тысячи людей видят его каждый день в ленте.\n"
    "Могу скинуть ссылку для редактирования, если интересно — просто ответь! 🤙",
]

TEMPLATES_EN = [
    "Hey! 👋 Spotted your event «{title}» in «{chat}» — sounds sick!\n"
    "We've already added it to Vibe Phangan — the app where all island events are in one place, "
    "3000+ people swipe through the feed daily.\n"
    "Want me to send you a link to edit the description, add photos or change the time? Just say the word! 🤙",

    "Yo! 👋 Saw «{title}» in «{chat}» — love it!\n"
    "It's already on Vibe Phangan — think of it like Tinder for events 🌴 "
    "you swipe right on stuff you vibe with, and boom — your evening is planned.\n"
    "If you wanna tweak anything in the listing just lmk, I'll send you the edit link!",

    "Hey there! ✌️ Noticed your event «{title}» in «{chat}».\n"
    "It's live on Vibe Phangan — the biggest event aggregator on the island. "
    "Thousands of people see it every day in the feed.\n"
    "I can send you a link to update the details if you're interested — just reply! 🤙",
]

# ──── AI prompts with full Vibe Phangan product knowledge ────

VIBE_PHANGAN_KNOWLEDGE = """О ПРОДУКТЕ Vibe Phangan:
- Vibe Phangan — приложение-агрегатор событий на Ко Пангане (Telegram Mini App)
- Автоматически собирает ВСЕ события острова из 60+ Telegram-групп в одно место
- Каждое событие — красивая карточка с фото, описанием, ценой, локацией и временем
- Свайп вправо = нравится, влево = не интересно (как Tinder для ивентов)
- За минуту формируется личный список «Мой Вайб»
- Vibe Pilot 🧭 — AI-планировщик, строит оптимальный маршрут на день
  (учитывает расстояния, время в пути на скутере, конфликты по расписанию)
- 3000+ активных пользователей видят ленту каждый день
- Для организаторов БЕСПЛАТНО — повышает видимость событий
- Русский и английский с автопереводом
- Доступно как Telegram Mini App: {app_url}
- Telegram-канал: {channel_url}"""

AI_SYSTEM_PROMPT = """Ты — живой, дружелюбный человек из команды Vibe Phangan.
Тебя зовут {name}. Ты общаешься в личке Telegram как обычный человек, не как бот.
Ты РЕАЛЬНЫЙ ЧЕЛОВЕК, не ассистент и не бот. Пиши как друг.

""" + VIBE_PHANGAN_KNOWLEDGE + """

КОНТЕКСТ: Ты написал организатору, что его ивент «{event_title}» из группы «{chat_title}» уже в Vibe Phangan.
Организатор ответил: «{their_message}»

ЯЗЫК СОБЕСЕДНИКА: {user_lang}

ГЛАВНОЕ ПРАВИЛО: Отвечай ЕСТЕСТВЕННО на то, что человек РЕАЛЬНО написал.
Не навязывай ссылки. Не предлагай редактирование если не просят.
Веди себя как живой человек, а не как промо-бот.

ПРАВИЛА:
1. СНАЧАЛА реагируй на смысл сообщения по-человечески:
   - Если пригашает куда-то → "Было бы круто! Может как-нибудь)" или что-то в духе
   - Если благодарит → "Рад(а)!" и т.п.
   - Если шутит → пошути в ответ
   - Если просто общается → поддержи разговор
2. Ссылку для редактирования ({edit_url}) давай ТОЛЬКО если сам попросил:
   "скинь ссылку", "хочу отредактировать", "как изменить" и т.п.
3. Про приложение ({app_url}) говори ТОЛЬКО если спрашивает что это / зачем / как работает
4. Если спрашивает что это → Vibe Phangan — как Tinder для ивентов на острове.
   Свайпаешь карточки, выбираешь вайб, AI строит маршрут на день
5. Если негативит → коротко извинись и попрощайся
6. Цена → для организаторов бесплатно
7. Пиши как друг: коротко (1-2 предложения), с эмодзи где уместно, без пафоса
8. Отвечай на языке собеседника ({user_lang})
9. НЕ выдумывай. НЕ повторяй одно и то же. НЕ будь навязчивым

Ответь ОДНИМ коротким сообщением (1-2 предложения)."""

AI_COLD_PROMPT = """Ты — живой, дружелюбный человек из команды Vibe Phangan.
Тебя зовут {name}. Тебе написал незнакомый человек в личку Telegram.

""" + VIBE_PHANGAN_KNOWLEDGE + """

Его сообщение: «{their_message}»

{events_context}

ЯЗЫК СОБЕСЕДНИКА: {user_lang}

ПРАВИЛА:
1. Если у него есть ивенты в нашей базе → предложи ссылки для редактирования
2. Если нет ивентов → предложи Vibe Phangan: {app_url} и канал {channel_url}
3. Пиши как друг, коротко, с эмодзи. Никакого официоза
4. Отвечай на языке собеседника ({user_lang})

Ответь ОДНИМ сообщением (1-3 предложения, максимум 4)."""

AI_REPHRASE_PROMPT = """Слегка перефразируй это сообщение, сохраняя:
- Тот же ЯЗЫК (если русский — отвечай по-русски, если английский — по-английски)
- Тот же смысл и тон
- Примерно ту же длину
Сделай его чуть более живым и естественным, как будто пишет реальный человек.
НЕ добавляй ссылки. НЕ меняй язык.
Верни ТОЛЬКО перефразированный текст, без пояснений.

{msg}"""

# ──── SQL ────

SQL_CREATE_OUTREACH_LOG = """
CREATE TABLE IF NOT EXISTS outreach_log (
    id           SERIAL PRIMARY KEY,
    sender_id    BIGINT NOT NULL,
    event_id     INTEGER REFERENCES events(id),
    account_idx  INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'queued',
    first_msg    TEXT,
    reply_text   TEXT,
    created_at   TIMESTAMPTZ DEFAULT now(),
    sent_at      TIMESTAMPTZ,
    replied_at   TIMESTAMPTZ,
    UNIQUE(sender_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_outreach_sender ON outreach_log(sender_id);
CREATE INDEX IF NOT EXISTS idx_outreach_status ON outreach_log(status);
"""

SQL_CREATE_NOTIFY_TRIGGER = """
CREATE OR REPLACE FUNCTION notify_new_event_outreach() RETURNS trigger AS $$
BEGIN
    IF NEW.sender_id IS NOT NULL AND NEW.sender_id > 0 AND NEW.source = 'listener' THEN
        PERFORM pg_notify('new_event_for_outreach', json_build_object(
            'event_id', NEW.id,
            'sender_id', NEW.sender_id,
            'sender', NEW.sender,
            'title_en', NEW.title->>'en',
            'source_chat_title', NEW.source_chat_title
        )::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_new_event_outreach ON events;
CREATE TRIGGER trg_new_event_outreach
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_event_outreach();
"""


def _build_dsn() -> str:
    """Build PostgreSQL DSN from env vars."""
    from urllib.parse import quote
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "tg_parser")
    user = os.getenv("DB_USER", "")
    pwd = os.getenv("DB_PASSWORD", "")
    auth = f"{quote(user, safe='')}:{quote(pwd, safe='')}@" if user else ""
    return f"postgresql://{auth}{host}:{port}/{name}"


def _load_accounts() -> list[dict]:
    """Load outreach Telegram accounts from env."""
    accounts = []
    for i in range(1, 10):
        api_id = os.getenv(f"OUTREACH_API_ID_{i}")
        api_hash = os.getenv(f"OUTREACH_API_HASH_{i}")
        phone = os.getenv(f"OUTREACH_PHONE_{i}", "")
        session = os.getenv(f"OUTREACH_SESSION_{i}", f"outreach_session_{i}")
        name = os.getenv(f"OUTREACH_NAME_{i}", f"Helper #{i}")
        if api_id and api_hash:
            accounts.append({
                "api_id": int(api_id),
                "api_hash": api_hash,
                "phone": phone or None,
                "session": session,
                "name": name,
                "idx": i,
                "client": None,
                "sent_today": 0,
                "last_reset": datetime.utcnow().date(),
            })
            logger.info(f"📱 Loaded outreach account #{i}: {name} (session={session})")
    return accounts


def _is_cyrillic(text: str) -> bool:
    """Check if text has Cyrillic characters."""
    return bool(re.search(r'[а-яА-ЯёЁ]', text or ""))


def _is_bot_sender(sender_id: int, sender_name: str) -> bool:
    """Check if sender is likely a bot or channel."""
    if sender_id <= 0:
        return True
    name_lower = (sender_name or "").lower()
    if "_bot" in name_lower or name_lower.endswith("bot"):
        return True
    return False


def _detect_language(sender_id: int, sender_name: str = "",
                     chat: str = "", title: str = "") -> str:
    """Detect user language.
    
    Priority:
    1. Telegram user's lang_code from listener session DB (most reliable)
    2. Cyrillic detection in sender name / chat name / event title (fallback)
    3. Default to English
    """
    import sqlite3
    listener_session = os.getenv("LISTENER_SESSION", "tg_parser_session")
    session_path = f"{listener_session}.session"
    
    # Try to get lang_code from listener's entity cache
    if os.path.exists(session_path):
        try:
            db = sqlite3.connect(session_path)
            # Check all columns available
            cols = [row[1] for row in db.execute("PRAGMA table_info(entities)").fetchall()]
            if 'phone' in cols:
                # Telethon stores entities but not lang_code in SQLite
                # However, we can check phone country code
                row = db.execute("SELECT phone FROM entities WHERE id = ?", (sender_id,)).fetchone()
                if row and row[0]:
                    phone = str(row[0])
                    # Russian phone codes
                    if phone.startswith('7') or phone.startswith('+7'):
                        db.close()
                        return 'ru'
            db.close()
        except Exception:
            pass
    
    # Fallback: check for Cyrillic in available text
    if _is_cyrillic(sender_name) or _is_cyrillic(chat) or _is_cyrillic(title):
        return 'ru'
    
    return 'en'


def _pick_template(title: str, chat: str, lang: str = "en") -> str:
    """Pick and fill a message template based on detected language."""
    templates = TEMPLATES_RU if lang == 'ru' else TEMPLATES_EN
    template = random.choice(templates)
    return template.format(title=title, chat=chat)


async def _ai_call_with_retry(prompt: str, max_retries: int = 3) -> str | None:
    """Call Gemini with model fallback and exponential backoff retry on 429 errors."""
    models = ["gemini-3-flash-preview", "gemini-2.5-flash"]
    delays = [10, 30, 60]
    client = genai.Client()

    for model in models:
        for attempt in range(max_retries):
            try:
                response = await client.aio.models.generate_content(
                    model=model,
                    contents=prompt,
                )
                if attempt > 0 or model != models[0]:
                    logger.info(f"✅ AI call succeeded with {model} (attempt {attempt+1})")
                return response.text.strip()
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    delay = delays[min(attempt, len(delays) - 1)]
                    logger.warning(f"🔄 AI rate-limited on {model} (attempt {attempt+1}/{max_retries}), retry in {delay}s...")
                    await asyncio.sleep(delay)
                elif "not found" in err_str.lower() or "404" in err_str or "not supported" in err_str.lower():
                    logger.warning(f"⚠️ Model {model} not available, trying next...")
                    break  # skip to next model
                else:
                    logger.error(f"AI call failed on {model}: {e}")
                    return None

    logger.error(f"AI call failed after trying all models")
    return None


async def _ai_rephrase(base_msg: str) -> str:
    """Use Gemini to slightly rephrase the message for naturalness."""
    prompt = AI_REPHRASE_PROMPT.format(msg=base_msg)
    result = await _ai_call_with_retry(prompt)
    if result and 20 <= len(result) <= len(base_msg) * 2:
        return result
    return base_msg


async def _ai_respond(context: dict) -> str | None:
    """Generate AI response for incoming DM."""
    prompt = AI_SYSTEM_PROMPT.format(**context)
    return await _ai_call_with_retry(prompt)


async def _ai_cold_respond(context: dict) -> str | None:
    """Generate AI response for cold incoming DM."""
    prompt = AI_COLD_PROMPT.format(**context)
    return await _ai_call_with_retry(prompt)


class OutreachDaemon:
    def __init__(self):
        self.pool: asyncpg.Pool = None
        self.listen_conn: asyncpg.Connection = None
        self.accounts = []
        self.queue: asyncio.Queue = None
        self._account_robin = 0

    async def start(self):
        """Initialize everything and start listening."""
        global DB_DSN, OUTREACH_ACCOUNTS

        DB_DSN = _build_dsn()
        self.accounts = _load_accounts()

        if not self.accounts:
            logger.error("❌ No outreach accounts configured! Set OUTREACH_API_ID_1, etc. in .env")
            sys.exit(1)

        # DB pool
        self.pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=5)
        logger.info("✅ PostgreSQL pool created")

        # Run migrations
        await self._migrate()

        # Connect Telethon clients
        for acc in self.accounts:
            client = TelegramClient(acc["session"], acc["api_id"], acc["api_hash"])
            if acc["phone"]:
                await client.start(phone=acc["phone"])
            else:
                await client.connect()
                if not await client.is_user_authorized():
                    logger.error(f"❌ Account #{acc['idx']} ({acc['session']}) not authorized! "
                                 f"Provide OUTREACH_PHONE_{acc['idx']} or run auth manually.")
                    continue
            me = await client.get_me()
            acc["client"] = client
            acc["tg_name"] = f"{me.first_name or ''} {me.last_name or ''}".strip()
            logger.info(f"✅ Telethon account #{acc['idx']} connected: {acc['tg_name']}")

            # Register incoming DM handler
            self._register_dm_handler(acc)

        # Queue
        self.queue = asyncio.Queue()

        # Start workers
        logger.info("🚀 Outreach Daemon started" + (" [DRY RUN]" if DRY_RUN else ""))
        await asyncio.gather(
            self._listen_postgres(),
            self._queue_worker(),
            self._run_telethon_clients(),
        )

    async def _migrate(self):
        """Create outreach_log table and trigger."""
        async with self.pool.acquire() as conn:
            await conn.execute(SQL_CREATE_OUTREACH_LOG)
            logger.info("✅ outreach_log table ready")
            await conn.execute(SQL_CREATE_NOTIFY_TRIGGER)
            logger.info("✅ PostgreSQL NOTIFY trigger installed")

    async def _listen_postgres(self):
        """Listen for new_event_for_outreach notifications."""
        self.listen_conn = await asyncpg.connect(DB_DSN)
        await self.listen_conn.add_listener("new_event_for_outreach", self._on_notify)
        logger.info("👂 Listening for new_event_for_outreach...")

        # Keep alive
        while True:
            await asyncio.sleep(60)

    def _on_notify(self, conn, pid, channel, payload):
        """Handle PostgreSQL NOTIFY."""
        try:
            data = json.loads(payload)
            logger.info(f"📨 NOTIFY received: event_id={data['event_id']}, "
                       f"sender={data['sender']}, title={data.get('title_en', '?')[:40]}")
            asyncio.create_task(self._process_notification(data))
        except Exception as e:
            logger.error(f"Failed to process NOTIFY: {e}")

    async def _process_notification(self, data: dict):
        """Filter and queue the outreach task."""
        sender_id = data.get("sender_id", 0)
        sender_name = data.get("sender", "")
        event_id = data.get("event_id")
        title = data.get("title_en", "Event")
        chat = data.get("source_chat_title", "")

        # Filter 1: Bot check
        if _is_bot_sender(sender_id, sender_name):
            logger.info(f"🤖 Skip bot/channel: {sender_name} (id={sender_id})")
            return

        # Filter 2: Cooldown check
        async with self.pool.acquire() as conn:
            recent = await conn.fetchval(
                "SELECT COUNT(*) FROM outreach_log "
                "WHERE sender_id = $1 AND created_at > now() - $2::interval",
                sender_id, timedelta(days=COOLDOWN_DAYS)
            )
            if recent > 0:
                logger.info(f"⏸️ Cooldown active for {sender_name} (sender_id={sender_id})")
                return

            # Filter 3: Already queued for this event?
            existing = await conn.fetchval(
                "SELECT id FROM outreach_log WHERE sender_id = $1 AND event_id = $2",
                sender_id, event_id
            )
            if existing:
                logger.info(f"⏭️ Already in outreach_log: event_id={event_id}, sender_id={sender_id}")
                return

        # Pick account (round-robin)
        acc = self._pick_account()
        if not acc:
            logger.warning("⚠️ All accounts hit daily limit, skipping")
            return

        # Insert into outreach_log
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO outreach_log (sender_id, event_id, account_idx, status) "
                "VALUES ($1, $2, $3, 'queued') ON CONFLICT DO NOTHING",
                sender_id, event_id, acc["idx"]
            )

        # Add to queue
        await self.queue.put({
            "sender_id": sender_id,
            "sender_name": sender_name,
            "event_id": event_id,
            "title": title,
            "chat": chat,
            "account": acc,
        })
        logger.info(f"📥 Queued DM to {sender_name} (acc #{acc['idx']}): «{title[:40]}»")

    def _pick_account(self) -> Optional[dict]:
        """Round-robin pick an account that hasn't hit daily limit."""
        today = datetime.utcnow().date()
        for _ in range(len(self.accounts)):
            acc = self.accounts[self._account_robin % len(self.accounts)]
            self._account_robin += 1

            # Reset daily counter
            if acc["last_reset"] != today:
                acc["sent_today"] = 0
                acc["last_reset"] = today

            if acc["sent_today"] < DAILY_LIMIT:
                return acc
        return None

    async def _queue_worker(self):
        """Process queued outreach tasks with rate limiting."""
        while True:
            task = await self.queue.get()
            try:
                await self._send_dm(task)
            except Exception as e:
                logger.error(f"Failed to send DM: {e}", exc_info=True)
            finally:
                self.queue.task_done()

            # Rate limit: random delay
            delay = random.randint(DELAY_MIN, DELAY_MAX)
            logger.info(f"⏳ Next DM in {delay}s...")
            await asyncio.sleep(delay)

    async def _send_dm(self, task: dict):
        """Send the first-contact DM."""
        acc = task["account"]
        client = acc["client"]
        sender_id = task["sender_id"]
        title = task["title"]
        chat = task["chat"]

        # Detect user's language
        sender_name = task.get("sender_name", "")
        lang = _detect_language(sender_id, sender_name, chat, title)
        logger.info(f"🌐 Language detected for {sender_name}: {lang}")

        # Generate message
        base_msg = _pick_template(title, chat, lang=lang)
        msg_text = await _ai_rephrase(base_msg)

        if DRY_RUN:
            logger.info(f"🏜️ DRY RUN — would send to {task['sender_name']} (id={sender_id}):\n{msg_text}")
            return

        # Send DM
        try:
            # Entity resolution strategies (in order of reliability):
            # 1. Resolve by username (works cross-account)
            # 2. AddContactRequest by phone (if available)
            # 3. InputPeerUser with borrowed access_hash (last resort)
            import sqlite3

            listener_session = os.getenv("LISTENER_SESSION", "tg_parser_session")
            session_path = f"{listener_session}.session"
            entity = None
            username = None
            phone = None
            access_hash = None

            # Read user info from listener session
            if os.path.exists(session_path):
                db = sqlite3.connect(session_path)
                try:
                    row = db.execute(
                        "SELECT hash, username, phone FROM entities WHERE id = ?",
                        (sender_id,)
                    ).fetchone()
                    if row:
                        access_hash = row[0]
                        username = row[1]
                        phone = row[2]
                        logger.info(f"🔑 Listener DB: user {sender_id} — username={username}, phone={'yes' if phone else 'no'}")
                except Exception as sql_err:
                    logger.warning(f"⚠️ SQLite query failed: {sql_err}")
                finally:
                    db.close()

            # Strategy 1: InputPeerUser with borrowed access_hash (fastest, works for recently seen users)
            if access_hash is not None and not entity:
                try:
                    from telethon.tl.types import InputPeerUser
                    peer = InputPeerUser(sender_id, access_hash)
                    await client.send_message(peer, msg_text)
                    logger.info(f"✅ DM sent via InputPeerUser (borrowed hash) to {sender_name}")
                    # Success — update DB and return
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE outreach_log SET status = 'sent', first_msg = $1, sent_at = now() "
                            "WHERE sender_id = $2 AND event_id = $3",
                            msg_text, sender_id, task["event_id"]
                        )
                    acc["sent_today"] += 1
                    return
                except Exception as e:
                    logger.warning(f"⚠️ InputPeerUser send failed: {e}")

            # Strategy 2: Resolve by username (works cross-account, but may hit flood limits)
            if username and not entity:
                try:
                    entity = await client.get_entity(username)
                    logger.info(f"✅ Resolved entity via username @{username}")
                except Exception as e:
                    logger.warning(f"⚠️ Username @{username} resolve failed: {e}")

            # Strategy 3: Add as contact temporarily (works if we have phone)
            if phone and not entity:
                try:
                    from telethon.tl.functions.contacts import AddContactRequest, DeleteContactsRequest
                    from telethon.tl.types import InputUser
                    result = await client(AddContactRequest(
                        id=InputUser(sender_id, access_hash or 0),
                        first_name=sender_name or "User",
                        last_name="",
                        phone=str(phone),
                        add_phone_privacy_exception=False
                    ))
                    entity = await client.get_entity(sender_id)
                    logger.info(f"✅ Resolved entity via AddContact (phone)")
                    # Clean up contact after resolving
                    try:
                        await client(DeleteContactsRequest(id=[entity]))
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning(f"⚠️ AddContact resolve failed: {e}")

            if not entity:
                logger.error(f"❌ Cannot resolve entity for {sender_name} (id={sender_id}) — all strategies failed")
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE outreach_log SET status = 'failed' "
                        "WHERE sender_id = $1 AND event_id = $2",
                        sender_id, task["event_id"]
                    )
                return

            await client.send_message(entity, msg_text)
            logger.info(f"✅ DM sent to {task['sender_name']} (id={sender_id}) via acc #{acc['idx']}")

            # Update outreach_log
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE outreach_log SET status = 'sent', first_msg = $1, sent_at = now() "
                    "WHERE sender_id = $2 AND event_id = $3",
                    msg_text, sender_id, task["event_id"]
                )

            acc["sent_today"] += 1

        except Exception as e:
            logger.error(f"❌ Failed to DM {task['sender_name']}: {e}")
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE outreach_log SET status = 'failed' "
                    "WHERE sender_id = $1 AND event_id = $2",
                    sender_id, task["event_id"]
                )

    def _register_dm_handler(self, acc: dict):
        """Register Telethon handler for incoming DMs on this account."""
        client = acc["client"]

        @client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
        async def on_dm(event):
            sender = await event.get_sender()
            if not isinstance(sender, User) or sender.bot:
                return

            sender_id = sender.id
            their_msg = event.text or ""
            if not their_msg.strip():
                return

            logger.info(f"💬 Incoming DM from {sender.first_name} (id={sender_id}) on acc #{acc['idx']}: "
                       f"{their_msg[:80]}")

            # Check if this is a reply to our outreach
            async with self.pool.acquire() as conn:
                outreach = await conn.fetchrow(
                    "SELECT ol.event_id, e.title->>'en' as title_en, e.source_chat_title "
                    "FROM outreach_log ol "
                    "JOIN events e ON e.id = ol.event_id "
                    "WHERE ol.sender_id = $1 AND ol.account_idx = $2 "
                    "AND ol.status = 'sent' "
                    "ORDER BY ol.sent_at DESC LIMIT 1",
                    sender_id, acc["idx"]
                )

            if outreach:
                # This is a reply to our outreach → AI respond with context
                event_id = outreach["event_id"]
                edit_url = EDIT_URL_TEMPLATE.format(event_id=event_id)
                # Detect language from the incoming message or sender profile
                user_lang = 'ru' if _is_cyrillic(their_msg) else _detect_language(
                    sender_id, sender.first_name or ""
                )

                response = await _ai_respond({
                    "name": acc.get("tg_name", acc["name"]),
                    "event_title": outreach["title_en"] or "Event",
                    "chat_title": outreach["source_chat_title"] or "",
                    "their_message": their_msg,
                    "edit_url": edit_url,
                    "app_url": APP_URL,
                    "channel_url": CHANNEL_URL,
                    "user_lang": "русский" if user_lang == 'ru' else "English",
                })

                if response:
                    if not DRY_RUN:
                        await event.respond(response)
                    logger.info(f"🤖 AI replied to {sender.first_name}: {response[:80]}")

                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE outreach_log SET status = 'replied', "
                            "reply_text = $1, replied_at = now() "
                            "WHERE sender_id = $2 AND event_id = $3",
                            their_msg, sender_id, event_id
                        )
            else:
                # Cold DM — check if this person has events in DB
                async with self.pool.acquire() as conn:
                    user_events = await conn.fetch(
                        "SELECT id, title->>'en' as title_en, event_date "
                        "FROM events WHERE sender_id = $1 AND event_date >= current_date "
                        "ORDER BY event_date LIMIT 5",
                        sender_id
                    )

                events_context = ""
                if user_events:
                    lines = []
                    for ev in user_events:
                        edit_url = EDIT_URL_TEMPLATE.format(event_id=ev["id"])
                        lines.append(f"- «{ev['title_en']}» ({ev['event_date']}) → {edit_url}")
                    events_context = "У этого человека есть ивенты в Vibe Phangan:\n" + "\n".join(lines)
                else:
                    events_context = "У этого человека нет ивентов в Vibe Phangan."

                # Detect language from the incoming message
                user_lang = 'ru' if _is_cyrillic(their_msg) else _detect_language(
                    sender_id, sender.first_name or ""
                )

                response = await _ai_cold_respond({
                    "name": acc.get("tg_name", acc["name"]),
                    "their_message": their_msg,
                    "events_context": events_context,
                    "app_url": APP_URL,
                    "channel_url": CHANNEL_URL,
                    "user_lang": "русский" if user_lang == 'ru' else "English",
                })

                if response and not DRY_RUN:
                    await event.respond(response)
                    logger.info(f"🤖 Cold AI reply to {sender.first_name}: {response[:80]}")

    async def _run_telethon_clients(self):
        """Run all Telethon clients to receive incoming messages."""
        tasks = []
        for acc in self.accounts:
            tasks.append(acc["client"].run_until_disconnected())
        await asyncio.gather(*tasks)


async def main():
    daemon = OutreachDaemon()
    await daemon.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
