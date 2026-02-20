#!/usr/bin/env python3
"""
PostgreSQL модуль — asyncpg connection pool + CRUD для всех таблиц.

Таблицы: chats, discovered_chats, venues, events, test_runs.

Использование:
    db = Database()
    await db.connect()     # создаёт pool + DDL
    ...
    await db.close()
"""

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime, date
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


# Суффиксы-локации для удаления при нормализации venue
_LOCATION_SUFFIXES = [
    ", koh phangan", ", ko phangan", ", ko pha-ngan",
    ", ко-панган", ", ко панган", ", панган",
    ", phangan", ", phangan island",
    ", haad rin", ", haad yao", ", haad salad",
    ", thong sala", ", ban tai", ", chaloklum",
    ", chaweng", ", samui", ", maduea wan",
    " koh phangan", " ko phangan",
    " (koh phangan)", " (ko phangan)",
    " (phangan)",
]


# Алиасы venue — синхронизировать с venue_enricher.py
_VENUE_ALIASES = {
    "aum": "aum sound healing center",
    "aum center": "aum sound healing center",
    "aum soundhealing center": "aum sound healing center",
    "aum soundhealing": "aum sound healing center",
    "aum phangan": "aum sound healing center",
    "aum sound center": "aum sound healing center",
    "kefir": "kefir family restaurant",
    "kefir restaurant": "kefir family restaurant",
    "sunset hill": "sunset hill resort",
    "sunset hill restaurant": "sunset hill resort",
    "nashe mesto": "mesto",
    "mesto phangan": "mesto",
    "mesto копанган": "mesto",
    "plunktone restaurant": "planktone restaurant lounge",
    "planktone restaurant  lounge": "planktone restaurant lounge",
    "planktone restaurant lounge chaweng": "planktone restaurant lounge",
    "sati yoga koh phangan": "sati yoga",
    "shivari amphitheater": "shivari",
    "shivari center": "shivari",
    "shivari koh phangan": "shivari",
    "lost paradise koh phangan": "lost paradise",
    "indriya retreat center koh phangan": "indriya retreat",
    "unclave koh phangan": "unclave",
    "the wave koh phangan": "the wave",
    "stay gold cafe  bar": "stay gold",
    "stay gold ko phangan": "stay gold",
    "soul terra phangan": "soulterra phangan",
    "soulterra phangan": "soulterra phangan",
    "catch phangan": "catch",
    "7eleven haad rin": "7eleven",
    "711": "7eleven",
    "711 meeting point": "7eleven",
}


def _normalize_name(name: str) -> str:
    """Нормализация имени для consistent matching.

    Убирает суффиксы-локации, пунктуацию, коллапсирует пробелы, применяет алиасы.
    """
    name = name.lower().strip()
    for suffix in _LOCATION_SUFFIXES:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    name = re.sub(r'[^a-zа-яёа-я0-9\s]', '', name)
    name = ' '.join(name.split())
    return _VENUE_ALIASES.get(name, name)

# ─── DDL ───

DDL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1. Чаты (наши подписки)
CREATE TABLE IF NOT EXISTS chats (
    id          BIGINT PRIMARY KEY,
    title       TEXT NOT NULL,
    type        TEXT,
    is_active   BOOLEAN DEFAULT true,
    added_at    TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- 2. Обнаруженные спайдером
CREATE TABLE IF NOT EXISTS discovered_chats (
    id                SERIAL PRIMARY KEY,
    chat_id           BIGINT,
    username          TEXT,
    invite_link       TEXT,
    title             TEXT,
    type              TEXT,
    source_type       TEXT NOT NULL,
    found_in_chat_id  BIGINT REFERENCES chats(id) ON DELETE SET NULL,
    participants_count INTEGER,
    status            TEXT DEFAULT 'new',
    resolved          BOOLEAN DEFAULT false,
    times_seen        INTEGER DEFAULT 1,
    first_seen        TIMESTAMPTZ DEFAULT now(),
    last_seen         TIMESTAMPTZ DEFAULT now()
);

-- Partial unique indexes (вместо UNIQUE constraint — допускаем NULL)
DO $$ BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uq_discovered_username
        ON discovered_chats(lower(username)) WHERE username IS NOT NULL;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

DO $$ BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uq_discovered_invite
        ON discovered_chats(invite_link) WHERE invite_link IS NOT NULL;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

DO $$ BEGIN
    CREATE UNIQUE INDEX IF NOT EXISTS uq_discovered_chat_id
        ON discovered_chats(chat_id) WHERE chat_id IS NOT NULL;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_discovered_status
    ON discovered_chats(status) WHERE status = 'new';
CREATE INDEX IF NOT EXISTS idx_discovered_unresolved
    ON discovered_chats(resolved) WHERE resolved = false;

-- 3. Площадки (venue cache)
CREATE TABLE IF NOT EXISTS venues (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) UNIQUE,
    name_normalized VARCHAR(255),
    lat             FLOAT,
    lng             FLOAT,
    google_maps_url TEXT,
    instagram_url   TEXT,
    address         TEXT,
    description     TEXT,
    cached_at       TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc', now())
);

-- name_normalized для consistent matching (unique constraint на name уже создаёт index)
CREATE INDEX IF NOT EXISTS idx_venues_normalized ON venues(name_normalized) WHERE name_normalized IS NOT NULL;

-- 4. Ивенты
CREATE TABLE IF NOT EXISTS events (
    id               SERIAL PRIMARY KEY,
    title            TEXT NOT NULL,
    category         TEXT,
    event_date       DATE,
    event_time       TEXT,
    location_name    TEXT,
    venue_id         INTEGER REFERENCES venues(id) ON DELETE SET NULL,
    price_thb        INTEGER DEFAULT 0,
    summary          TEXT,
    description      TEXT,
    source_chat_id   BIGINT REFERENCES chats(id) ON DELETE SET NULL,
    source_chat_title TEXT,
    message_id       BIGINT,
    sender           TEXT,
    filter_score     INTEGER DEFAULT 0,
    original_text    TEXT,
    source           TEXT DEFAULT 'listener',
    fingerprint      TEXT UNIQUE,
    detected_at      TIMESTAMPTZ DEFAULT now(),
    image_path       VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date) WHERE event_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);
CREATE INDEX IF NOT EXISTS idx_events_detected ON events(detected_at DESC);

-- GIN trgm index для fuzzy search по title
CREATE INDEX IF NOT EXISTS idx_events_trgm ON events USING GIN(title gin_trgm_ops);

-- Хэш-индекс оригинального текста для моментального обнаружения спама
CREATE INDEX IF NOT EXISTS idx_events_original_text_md5 ON events(md5(original_text));

-- 5. Отчёты тестов
CREATE TABLE IF NOT EXISTS test_runs (
    id              SERIAL PRIMARY KEY,
    elapsed_sec     REAL,
    chats_count     INTEGER,
    batch_messages  INTEGER,
    batch_filtered  INTEGER,
    batch_events    INTEGER,
    spider_discovered INTEGER,
    spider_resolved   INTEGER,
    live_messages   INTEGER,
    live_events     INTEGER,
    raw_report      JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Auto-update trigger для updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'chats_updated_at') THEN
        CREATE TRIGGER chats_updated_at BEFORE UPDATE ON chats
            FOR EACH ROW EXECUTE FUNCTION update_modified_column();
    END IF;
END $$;
"""


class Database:
    """Async PostgreSQL database interface."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self, min_size: int = 2, max_size: int = 10):
        """Создать connection pool и применить DDL."""
        if not self.dsn:
            import config
            self.dsn = config.get_dsn()
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=min_size,
            max_size=max_size,
        )
        async with self.pool.acquire() as conn:
            await conn.execute(DDL)
        logger.info("✅ PostgreSQL connected, DDL applied")

    async def close(self):
        """Закрыть pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL pool closed")

    # ═══════════════════════════════════════════
    # CHATS
    # ═══════════════════════════════════════════

    async def upsert_chat(self, chat_id: int, title: str, chat_type: str,
                          is_active: bool = True):
        """Добавить или обновить чат."""
        await self.pool.execute("""
            INSERT INTO chats (id, title, type, is_active)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                type = EXCLUDED.type,
                is_active = EXCLUDED.is_active
        """, chat_id, title, chat_type, is_active)

    async def get_active_chats(self) -> list[dict]:
        """Вернуть список активных чатов."""
        rows = await self.pool.fetch(
            "SELECT id, title, type FROM chats WHERE is_active = true ORDER BY title"
        )
        return [dict(r) for r in rows]

    async def get_all_chat_ids(self) -> set[int]:
        """Вернуть множество всех chat_id (для spider known_keys)."""
        rows = await self.pool.fetch("SELECT id FROM chats")
        return {r["id"] for r in rows}

    # ═══════════════════════════════════════════
    # DISCOVERED CHATS (Spider)
    # ═══════════════════════════════════════════

    async def upsert_discovered(
        self,
        *,
        chat_id: int | None = None,
        username: str | None = None,
        invite_link: str | None = None,
        title: str | None = None,
        chat_type: str | None = None,
        source_type: str = "forward",
        found_in_chat_id: int | None = None,
        participants_count: int | None = None,
        status: str = "new",
        resolved: bool = False,
    ) -> int:
        """
        Upsert discovery: если уже есть (по username/invite/chat_id) —
        обновляем times_seen и last_seen. Иначе вставляем.
        
        Returns: id записи.
        """
        # Определяем уникальный ключ для поиска
        existing = None
        if username:
            existing = await self.pool.fetchrow(
                "SELECT id FROM discovered_chats WHERE lower(username) = lower($1)",
                username,
            )
        elif invite_link:
            existing = await self.pool.fetchrow(
                "SELECT id FROM discovered_chats WHERE invite_link = $1",
                invite_link,
            )
        elif chat_id:
            existing = await self.pool.fetchrow(
                "SELECT id FROM discovered_chats WHERE chat_id = $1",
                chat_id,
            )

        if existing:
            # Обновляем times_seen + last_seen
            await self.pool.execute("""
                UPDATE discovered_chats
                SET times_seen = times_seen + 1,
                    last_seen = now(),
                    title = COALESCE($2, title),
                    participants_count = COALESCE($3, participants_count),
                    resolved = CASE WHEN $4 THEN true ELSE resolved END,
                    chat_id = COALESCE($5, chat_id),
                    type = COALESCE($6, type),
                    status = CASE WHEN $7 != 'new' THEN $7 ELSE status END
                WHERE id = $1
            """, existing["id"], title, participants_count, resolved,
                 chat_id, chat_type, status)
            return existing["id"]

        # Вставляем новый
        row = await self.pool.fetchrow("""
            INSERT INTO discovered_chats
                (chat_id, username, invite_link, title, type, source_type,
                 found_in_chat_id, participants_count, status, resolved)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        """, chat_id, username, invite_link, title, chat_type, source_type,
             found_in_chat_id, participants_count, status, resolved)
        return row["id"]

    async def is_text_exists(self, text: str) -> bool:
        """Быстрая проверка по хэшу текста (защита от повторного ИИ-анализа)."""
        if not self.pool or not text:
            return False
        res = await self.pool.fetchval("SELECT 1 FROM events WHERE md5(original_text) = md5($1) LIMIT 1", text)
        return bool(res)

    async def get_unresolved(self, limit: int = 10) -> list[dict]:
        """Вернуть записи для resolve."""
        rows = await self.pool.fetch("""
            SELECT * FROM discovered_chats
            WHERE resolved = false AND status = 'new'
            ORDER BY first_seen
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

    async def update_discovered(self, disc_id: int, **kwargs):
        """Обновить поля обнаруженного чата."""
        if not kwargs:
            return
        sets = []
        vals = []
        for i, (k, v) in enumerate(kwargs.items(), 1):
            sets.append(f"{k} = ${i}")
            vals.append(v)
        vals.append(disc_id)
        query = f"UPDATE discovered_chats SET {', '.join(sets)} WHERE id = ${len(vals)}"
        await self.pool.execute(query, *vals)

    async def get_discovered_stats(self) -> dict:
        """Статистика discovery."""
        rows = await self.pool.fetch("""
            SELECT status, count(*) as cnt
            FROM discovered_chats
            GROUP BY status
        """)
        return {r["status"]: r["cnt"] for r in rows}

    async def get_all_discovered(self, status: str | None = None) -> list[dict]:
        """Все обнаруженные с опциональным фильтром по статусу."""
        if status:
            rows = await self.pool.fetch(
                "SELECT * FROM discovered_chats WHERE status = $1 ORDER BY first_seen DESC",
                status,
            )
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM discovered_chats ORDER BY first_seen DESC"
            )
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════
    # VENUES
    # ═══════════════════════════════════════════

    async def get_venue(self, query: str) -> dict | None:
        """Получить venue из кэша по query (через venue_aliases)."""
        q = _normalize_name(query)
        row = await self.pool.fetchrow("""
            SELECT v.*, va.venue_id
            FROM venue_aliases va
            LEFT JOIN venues v ON va.venue_id = v.id
            WHERE va.query = $1
        """, q)
        
        if not row:
            return None
            
        result = dict(row)
        if result.get('venue_id') is None:
            # venue_id is NULL -> "not found" cache
            return {"found": False}
            
        result["found"] = True
        return result

    async def upsert_venue(self, query: str, data: dict) -> int | None:
        """Добавить venue и привязать alias."""
        q = _normalize_name(query)
        
        if not data.get("found", True):
            # Cache "not found" by pointing alias to NULL
            await self.pool.execute(
                "INSERT INTO venue_aliases (query, venue_id) VALUES ($1, NULL) ON CONFLICT DO NOTHING",
                q
            )
            return None
            
        name = data.get("name")
        if not name:
            name = query  # Fallback
            
        # 1. Upsert into venues (Unique by name)
        row = await self.pool.fetchrow("""
            INSERT INTO venues (name, name_normalized, lat, lng, google_maps_url, instagram_url, address, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (name) DO UPDATE SET
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                google_maps_url = EXCLUDED.google_maps_url,
                instagram_url = EXCLUDED.instagram_url,
                address = EXCLUDED.address,
                description = EXCLUDED.description,
                cached_at = now()
            RETURNING id
        """,
            name,
            _normalize_name(name),
            data.get("lat"),
            data.get("lng"),
            data.get("google_maps_url"),
            data.get("instagram_url"),
            data.get("address"),
            data.get("description"),
        )
        
        venue_id = row["id"]
        
        # 2. Add to aliases
        await self.pool.execute(
            "INSERT INTO venue_aliases (query, venue_id) VALUES ($1, $2) ON CONFLICT (query) DO UPDATE SET venue_id = EXCLUDED.venue_id",
            q, venue_id
        )
        
        return venue_id

    async def get_venue_count(self) -> int:
        """Количество алиасов в кэше."""
        return await self.pool.fetchval("SELECT count(*) FROM venue_aliases")

    # ═══════════════════════════════════════════
    # EVENTS
    # ═══════════════════════════════════════════

    @staticmethod
    def _fingerprint(title: str, event_date: str | None, location: str | None = None) -> str:
        """Генерирует fingerprint для дедупликации.

        Нормализует title: lowercase, убирает пунктуацию,
        коллапсирует пробелы — чтобы ловить дупли из разных чатов.
        Location больше не участвует в fingerprint во избежание дублей
        из-за незначительных вариаций написания площадки.
        """
        def _norm(s: str) -> str:
            s = (s or "").lower().strip()
            s = re.sub(r"[^a-zа-яёа-я0-9 ]", "", s)
            return " ".join(s.split())

        raw = f"{_norm(title)}|{event_date or 'TBD'}"
        return hashlib.md5(raw.encode()).hexdigest()

    async def insert_event(self, event: dict, source: str = "listener") -> tuple[int | None, bool, bool]:
        """
        Вставить ивент или обновить дубликат. Возвращает: (event_id, is_new, has_image)
        """
        meta = event.get("_meta", {})
        venue_data = event.get("venue")

        # Resolve venue_id
        venue_id = None
        location_name = event.get("location_name", "")
        if venue_data and venue_data.get("found") and location_name:
            # Exact query match
            v = await self.get_venue(location_name)
            if v:
                venue_id = v["id"]
            else:
                # Fallback: normalized name match
                normalized = _normalize_name(location_name)
                if normalized:
                    row = await self.pool.fetchrow(
                        "SELECT id FROM venues WHERE name_normalized = $1",
                        normalized,
                    )
                    if row:
                        venue_id = row["id"]

        # Parse date
        event_date = None
        date_str = event.get("date")
        if date_str and date_str not in ("TBD", "N/A", ""):
            try:
                event_date = date.fromisoformat(date_str)
            except (ValueError, TypeError):
                pass

        fp = self._fingerprint(
            event.get("title", ""),
            date_str,
            event.get("location_name"),
        )

        try:
            row = await self.pool.fetchrow("""
                INSERT INTO events
                    (title, category, event_date, event_time, location_name,
                     venue_id, price_thb, summary, description,
                     source_chat_id, source_chat_title, message_id, sender,
                     filter_score, original_text, source, fingerprint, detected_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                ON CONFLICT (fingerprint) DO UPDATE SET
                    description = COALESCE(EXCLUDED.description, events.description),
                    summary = COALESCE(EXCLUDED.summary, events.summary),
                    venue_id = COALESCE(EXCLUDED.venue_id, events.venue_id),
                    location_name = COALESCE(EXCLUDED.location_name, events.location_name),
                    price_thb = EXCLUDED.price_thb,
                    category = EXCLUDED.category,
                    event_time = COALESCE(EXCLUDED.event_time, events.event_time)
                RETURNING id, (xmax = 0) AS is_new, image_path
            """,
                event.get("title"),
                event.get("category"),
                event_date,
                event.get("time"),
                event.get("location_name"),
                venue_id,
                event.get("price_thb", 0),
                event.get("summary"),
                event.get("description"),
                meta.get("chat_id"),
                meta.get("chat_title"),
                meta.get("message_id"),
                meta.get("sender"),
                meta.get("filter_score", 0),
                meta.get("original_text"),
                source,
                fp,
                datetime.fromisoformat(meta["detected_at"]) if meta.get("detected_at") else datetime.now(),
            )
            return row["id"], row["is_new"], bool(row["image_path"])
        except asyncpg.UniqueViolationError:
            return None, False, False

    async def get_events(
        self,
        limit: int = 50,
        category: str | None = None,
        since: datetime | None = None,
    ) -> list[dict]:
        """Получить ивенты с фильтрами."""
        conditions = []
        params = []
        n = 1

        if category:
            conditions.append(f"category = ${n}")
            params.append(category)
            n += 1
        if since:
            conditions.append(f"detected_at >= ${n}")
            params.append(since)
            n += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        rows = await self.pool.fetch(f"""
            SELECT e.*, v.name as venue_name, v.lat, v.lng, v.google_maps_url
            FROM events e
            LEFT JOIN venues v ON e.venue_id = v.id
            {where}
            ORDER BY e.detected_at DESC
            LIMIT ${n}
        """, *params)
        return [dict(r) for r in rows]

    async def get_event_count(self) -> int:
        """Общее количество ивентов."""
        return await self.pool.fetchval("SELECT count(*) FROM events")

    # ═══════════════════════════════════════════
    # TEST RUNS
    # ═══════════════════════════════════════════

    async def save_test_run(self, report: dict) -> int:
        """Сохранить результат теста."""
        # Safely extract ints from possibly nested/dict values
        chats = report.get("chats")
        chats_count = chats.get("loaded") if isinstance(chats, dict) else chats

        batch = report.get("batch", {})
        spider = report.get("spider", {})
        live = report.get("live", {})

        row = await self.pool.fetchrow("""
            INSERT INTO test_runs
                (elapsed_sec, chats_count, batch_messages, batch_filtered,
                 batch_events, spider_discovered, spider_resolved,
                 live_messages, live_events, raw_report)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING id
        """,
            report.get("elapsed_sec"),
            chats_count,
            batch.get("total_messages"),
            batch.get("filter_passed"),
            batch.get("events"),
            spider.get("new_found"),
            None,  # resolved не всегда есть
            live.get("messages"),
            live.get("ai_events") or live.get("events"),
            json.dumps(report, ensure_ascii=False, default=str),
        )
        return row["id"]

    async def get_last_test_runs(self, limit: int = 5) -> list[dict]:
        """Последние N тестовых прогонов."""
        rows = await self.pool.fetch("""
            SELECT id, elapsed_sec, chats_count, batch_events,
                   live_events, created_at
            FROM test_runs ORDER BY created_at DESC LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

    async def upsert_discovery(
        self,
        chat_id: int | None = None,
        username: str | None = None,
        invite_link: str | None = None,
        title: str | None = None,
        chat_type: str | None = None,
        source: str | None = "spider",
        relevance_score: int | None = None,
        relevance_reason: str | None = None,
    ):
        """Интеллектуальный Upsert для discovered_chats (обход partial indexes)."""
        if not self.pool:
            return
        if not chat_id and not username and not invite_link:
            return

        conditions = []
        args = []
        if chat_id:
            args.append(chat_id)
            conditions.append(f"chat_id = ${len(args)}")
        if username:
            args.append(username.lower())
            conditions.append(f"lower(username) = ${len(args)}")
        if invite_link:
            args.append(invite_link)
            conditions.append(f"invite_link = ${len(args)}")

        where_clause = " OR ".join(conditions)

        async with self.pool.acquire() as conn:
            existing_id = await conn.fetchval(f"""
                SELECT id FROM discovered_chats WHERE {where_clause} LIMIT 1
            """, *args)

            if existing_id:
                await conn.execute("""
                    UPDATE discovered_chats
                    SET times_seen = times_seen + 1,
                        last_seen = now(),
                        title = COALESCE($1, title),
                        type = COALESCE($2, type)
                    WHERE id = $3
                """, title, chat_type, existing_id)
            else:
                try:
                    await conn.execute("""
                        INSERT INTO discovered_chats (
                            chat_id, username, invite_link, title, type, source_type
                        ) VALUES ($1, $2, $3, $4, $5, $6)
                    """, chat_id, username, invite_link, title, chat_type, source)
                except asyncpg.UniqueViolationError:
                    pass
