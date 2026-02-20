"""
AI Analyzer — двухступенчатый анализ сообщений через Gemini.

Стадия 1 (pre-screen): gemini-2.5-flash-lite → «ивент / не ивент»
Стадия 2 (extract):     gemini-2.5-flash → полная структура данных

Fallback: при 503/504 на основной модели → gemini-2.5-flash-lite.
"""

import json
import asyncio
import logging
from datetime import date
from typing import Optional

import httpx
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

import config
from utils import RateLimiter

logger = logging.getLogger(__name__)


# ─── Pydantic Schemas (Structured Outputs) ───

class PreScreenResult(BaseModel):
    is_event: bool = Field(description="True if the text contains a real offline event")

class EventResult(BaseModel):
    is_event: bool = Field(description="True if this is a real offline event")
    title: Optional[str] = Field(description="Short catchy title, max 30 characters")
    category: Optional[str] = Field(description="One of 5 categories: Party, Sport, Business, Education, Chill")
    date: Optional[str] = Field(description="Date in YYYY-MM-DD format if specified, otherwise null")
    time: Optional[str] = Field(description="Time in HH:MM format if specified, otherwise null")
    location_name: Optional[str] = Field(description="Venue name for Google Maps lookup, otherwise null")
    price_thb: Optional[int] = Field(description="Price in Thai Baht (0 if free), otherwise null")
    summary: Optional[str] = Field(description="One sentence summary, max 80 characters")
    description: Optional[str] = Field(description="Event announcement for listing, 2-4 sentences")


# ─── Prompts ───

PRESCREEN_PROMPT = """Determine if this Telegram message contains information about a REAL OFFLINE EVENT (party, concert, yoga, meetup, sports, masterclass, networking, festival, excursion, meditation, retreat, etc.).

The following are NOT events (return is_event=false):
- Buy/sell posts: "selling bike", "buying iPhone", "used furniture"
- Rent/lease: "villa for rent", "looking for apartment", "bike rental"
- Currency exchange: "USDT exchange", "baht rate", "p2p"
- Services: "massage", "transfer", "cleaning", "nails"
- Questions/discussions: "where is it happening?", "who knows?", "we're going there", casual chat
- Channel/bot ads and online webinars
- 🚨 CRITICAL: Announcements with NO indication of a physical venue (no direct address like "Moo 5", no branded venue name like "AUM", "Prana", "Catch", "Osho", "Orion". For example "location in DM" or "join our group" with no venue) — these are NOT events.

IMPORTANT: Messages may be in Russian, English, or mixed. Analyze the CONTENT regardless of language."""


EXTRACT_PROMPT = """You are an AI assistant for a geo-location event app on Phuket/Koh Phangan.
Extract data about the OFFLINE EVENT from the text.

RULES:
1. Category: one of "Party", "Sport", "Business", "Education", "Chill".
2. Price (price_thb): number in Thai Baht, 0 if free, null if unknown.
3. Location (location_name): exact venue name for Google Maps lookup. 🚨 IMPORTANT: If no direct address exists but the event has a branded name (e.g. "AUM DAY", "training at Prana", "Osho meditation", "Orion Healing Center"), extract the brand ("AUM", "Prana", "Osho", "Orion") as location_name. Otherwise null.
4. Date: "today" = {today}, "tomorrow" = next day. Otherwise null. Parse Russian date words: "сегодня"=today, "завтра"=tomorrow.
5. Title: short catchy title, max 30 characters.
6. Summary: one sentence, max 80 characters.
7. Description: attractive event announcement for a listing, 2-4 sentences, max 500 chars. Convey the atmosphere, what will happen and why it's worth attending.
8. 🚨 EXCLUSIONS: If this is a question ("where is it?"), personal discussion, service offer (massage) OR if location_name is null and cannot be derived from text — return is_event = false. STRICT RULE: An event without a location (even implied) is not an event.
9. IMPORTANT: extract ONLY ONE object (the nearest/most relevant event).

IMPORTANT: The message text may be in Russian, English, or mixed languages. Analyze content regardless of language."""



# ─── Analyzer ───

class EventAnalyzer:
    """Двухступенчатый анализатор: pre-screen (lite) → extract (full)."""

    def __init__(self):
        gemini_key, gemini_proxy = config.validate_gemini()

        # Rate limiters
        self.screen_limiter = RateLimiter(max_requests=500, period=60.0)
        self.extract_limiter = RateLimiter(max_requests=100, period=60.0)

        # HTTP
        http_options = {"timeout": 60_000}

        self._http_client = None
        if gemini_proxy:
            self._http_client = httpx.Client(
                proxy=gemini_proxy,
                timeout=60.0,
            )
            proxy_display = gemini_proxy.split('@')[-1] if '@' in gemini_proxy else gemini_proxy
            print(f"   🌐 Gemini proxy: {proxy_display}")

        self.client = genai.Client(
            api_key=gemini_key,
            http_options=http_options,
        )

        if self._http_client:
            self.client._api_client._httpx_client = self._http_client

        # Модели
        self.screen_model = "gemini-2.5-flash-lite"
        self.model = "gemini-2.5-flash"
        self.fallback_model = "gemini-2.5-flash-lite"

        # Счётчики
        self.stats = {
            "screened": 0,
            "screen_passed": 0,
            "extracted": 0,
            "events_found": 0,
            "fallbacks": 0,
            "errors": 0,
        }

    async def pre_screen(self, text: str, chat_title: str = "") -> bool:
        """
        Стадия 1: быстрый скрининг — ивент или нет.
        Используется дешёвая модель gemini-2.5-flash-lite.

        Returns:
            True если сообщение похоже на ивент.
        """
        if not text or len(text.strip()) < 30:
            return False

        await self.screen_limiter.acquire()
        self.stats["screened"] += 1

        user_prompt = f"Chat: {chat_title}\n\nMessage:\n{text[:1000]}"

        try:
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.screen_model,
                contents=user_prompt,
                config=types.GenerateContentConfig(
                    system_instruction=PRESCREEN_PROMPT,
                    temperature=0.0,
                    max_output_tokens=32,
                    response_mime_type="application/json",
                    response_schema=PreScreenResult,
                ),
            )

            raw = response.text.strip()
            result = json.loads(raw)
            is_event = result.get("is_event", False)

            if is_event:
                self.stats["screen_passed"] += 1

            return is_event

        except Exception as e:
            logger.debug(f"Pre-screen error: {e}")
            # При ошибке — пропускаем на следующую стадию (на всякий)
            self.stats["screen_passed"] += 1
            return True

    async def analyze(self, text: str, chat_title: str = "") -> dict | None:
        """
        Полный пайплайн: pre-screen → extract.

        Returns:
            Словарь с данными ивента или {"is_event": False}.
        """
        # Stage 1: Pre-screen
        is_event = await self.pre_screen(text, chat_title)
        if not is_event:
            return {"is_event": False}

        # Stage 2: Extract details
        return await self.extract(text, chat_title)

    async def analyze_batch(
        self, items: list[tuple[str, str]], concurrency: int = 10
    ) -> list[dict | None]:
        """
        Batch-анализ: параллельный pre-screen, затем extract для прошедших.

        Args:
            items: Список кортежей (text, chat_title).
            concurrency: Макс. параллельных pre-screen вызовов.

        Returns:
            Список результатов (в том же порядке).
        """
        sem = asyncio.Semaphore(concurrency)

        async def _screen(idx: int, text: str, chat_title: str):
            async with sem:
                return idx, await self.pre_screen(text, chat_title)

        # Параллельный pre-screen
        tasks = [_screen(i, t, c) for i, (t, c) in enumerate(items)]
        screen_results = await asyncio.gather(*tasks)

        # Extract только для прошедших (последовательно — rate limit 10/min)
        results: list[dict | None] = [None] * len(items)
        for idx, passed in screen_results:
            if passed:
                results[idx] = await self.extract(items[idx][0], items[idx][1])
            else:
                results[idx] = {"is_event": False}

        return results

    async def extract(self, text: str, chat_title: str = "") -> dict | None:
        """
        Стадия 2: полное извлечение данных о мероприятии.
        Используется основная модель с fallback.
        """
        await self.extract_limiter.acquire()
        self.stats["extracted"] += 1

        today = date.today().isoformat()
        system_prompt = EXTRACT_PROMPT.replace("{today}", today)
        user_prompt = f"Chat: {chat_title}\n\nMessage:\n{text[:2000]}"

        models_to_try = [self.model]

        for model in models_to_try:
            for attempt in range(2):
                try:
                    response = await asyncio.to_thread(
                        self.client.models.generate_content,
                        model=model,
                        contents=user_prompt,
                        config=types.GenerateContentConfig(
                            system_instruction=system_prompt,
                            temperature=0.1,
                            max_output_tokens=8192,
                            response_mime_type="application/json",
                            response_schema=EventResult,
                        ),
                    )

                    raw = response.text.strip()
                    result = json.loads(raw)
                    validated = self._validate_result(result)

                    if validated.get("is_event"):
                        self.stats["events_found"] += 1

                    return validated

                except json.JSONDecodeError as e:
                    logger.warning(f"Невалидный JSON: {e}\nRaw: {raw[:200]}")
                    if attempt == 0:
                        logger.info("Retry на JSON-ошибку...")
                        await asyncio.sleep(1)
                        continue
                    self.stats["errors"] += 1
                    return None

                except Exception as e:
                    err = str(e)
                    is_server_err = ("503" in err or "UNAVAILABLE" in err
                                     or "504" in err or "DEADLINE" in err
                                     or "timeout" in err.lower())

                    if is_server_err:
                        if attempt == 0:
                            logger.info(f"⏳ {model} retry через 3с ({type(e).__name__})")
                            await asyncio.sleep(3)
                            continue
                        # 2-я попытка не удалась → fallback
                        if model == self.model and self.fallback_model not in models_to_try:
                            print(f"  ⚠️ {model} → fallback {self.fallback_model}")
                            logger.warning(f"{model} → fallback {self.fallback_model}")
                            models_to_try.append(self.fallback_model)
                            self.stats["fallbacks"] += 1
                            break
                        self.stats["errors"] += 1
                        logger.error(f"Fallback тоже упал: {e}")
                        return None

                    self.stats["errors"] += 1
                    logger.error(f"Ошибка Gemini: {e}")
                    return None

        return None

    def _validate_result(self, result) -> dict:
        """Валидация и нормализация ответа."""
        # Gemini иногда возвращает массив ивентов — берём первый
        if isinstance(result, list):
            result = result[0] if result else {}
        if not isinstance(result, dict):
            return {"is_event": False}

        if not result.get("is_event"):
            return {"is_event": False}

        required = ["title", "category", "summary"]
        for field in required:
            if field not in result:
                result[field] = "N/A"

        valid_categories = {"Party", "Sport", "Business", "Education", "Chill"}
        if result.get("category") not in valid_categories:
            result["category"] = "Chill"

        try:
            result["price_thb"] = int(result.get("price_thb", 0))
        except (ValueError, TypeError):
            result["price_thb"] = 0

        result.setdefault("date", "TBD")
        result.setdefault("time", "TBD")
        result.setdefault("location_name", "TBD")
        result.setdefault("description", "")

        return result

    def print_stats(self):
        """Вывод статистики AI модуля."""
        s = self.stats
        print(f"  🤖 AI Stats:")
        print(f"     Pre-screened: {s['screened']}")
        print(f"     Screen passed: {s['screen_passed']}")
        print(f"     Extracted: {s['extracted']}")
        print(f"     Events found: {s['events_found']}")
        if s['fallbacks'] > 0:
            print(f"     Fallbacks: {s['fallbacks']}")
        if s['errors'] > 0:
            print(f"     Errors: {s['errors']}")

    def close(self):
        """Освобождение ресурсов."""
        if self._http_client:
            self._http_client.close()
