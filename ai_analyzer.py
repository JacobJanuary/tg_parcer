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

class BilingualText(BaseModel):
    en: str = Field(description="English text")
    ru: str = Field(description="Russian text")

class EventResult(BaseModel):
    is_event: bool = Field(description="True if this is a real offline event")
    title: Optional[BilingualText] = Field(description="Bilingual title (keys: 'en' and 'ru'), max 30 chars each")
    category: Optional[str] = Field(description="One of 5 categories: Party, Sport, Business, Education, Chill")
    date: Optional[str] = Field(description="Date in YYYY-MM-DD format if specified, otherwise null")
    time: Optional[str] = Field(description="Time in HH:MM format if specified, otherwise null")
    location_name: Optional[str] = Field(description="Venue name for Google Maps lookup, otherwise null")
    price_thb: Optional[int] = Field(description="Price in Thai Baht (0 if free), otherwise null")
    summary: Optional[BilingualText] = Field(description="Bilingual summary (keys: 'en' and 'ru'), max 80 chars each")
    description: Optional[BilingualText] = Field(description="Bilingual description (keys: 'en' and 'ru'), 2-4 sentences each")


# ─── Prompts ───

PRESCREEN_PROMPT = """Determine if this Telegram message contains information about a REAL OFFLINE EVENT (party, concert, yoga, meetup, sports, masterclass, networking, festival, excursion, meditation, retreat, etc.).

The following are NOT events (return is_event=false):
- Buy/sell posts: "selling bike", "buying iPhone", "used furniture"
- Rent/lease: "villa for rent", "looking for apartment", "bike rental"
- Currency exchange: "USDT exchange", "baht rate", "p2p"
- Services: "massage", "transfer", "cleaning", "nails"
- Questions/discussions: "where is it happening?", "who knows?", "we're going there", casual chat
- Event cancellations: "cancelled", "won't be held", "postponed indefinitely"
- Channel/bot ads and online webinars
- 🚨 OFF-ISLAND EVENTS: Events explicitly located on Koh Tao, Koh Samui, Samui, Bangkok, Pattaya, Chiang Mai, or any location that is clearly NOT on Koh Phangan. This app is ONLY for Koh Phangan events.
- 🚨 CRITICAL: Announcements with NO indication of a physical venue (no direct address like "Moo 5", no branded venue name like "AUM", "Prana", "Catch", "Osho", "Orion"). For example "location in DM" or "join our group" with no venue — these are NOT events.
- HOWEVER: If the CHAT NAME itself is a known venue or brand (e.g. chat "SATI YOGA" or "Ошо медитация Koh Phangan"), the chat name CAN serve as the venue indicator.

IMPORTANT: Messages may be in Russian, English, or mixed. Analyze the CONTENT regardless of language."""


EXTRACT_PROMPT = """You are an AI assistant for a geo-location event app on Koh Phangan (Thailand).
Extract data about the OFFLINE EVENT from the text.

RULES:
1. Category: one of "Party", "Sport", "Business", "Education", "Chill".
2. Price (price_thb): number in Thai Baht, 0 if free, null if unknown.
3. Location (location_name): exact venue name for Google Maps lookup.
   a) First, look for a venue name in the MESSAGE TEXT (direct address, branded name like "AUM", "Prana", "Cafe 13", "Soul Breakfast Phangan").
   b) If NOT found in text, check the CHAT NAME. If the chat name IS a venue or brand (e.g. "SATI YOGA", "Ошо медитация Koh Phangan", "NEWS_NASHEMESTO" → "Mesto"), use that as location_name.
   c) If neither text nor chat name yields a venue → location_name = null.
   d) IMPORTANT: Return ONLY the exact venue name WITHOUT any area or district suffix. Example: "Orion Healing" (NOT "Orion Healing, Srithanu"), "AUM" (NOT "AUM Sound Healing Center, Maduea Wan"). The location area is handled separately by the geo-enrichment system.
4. Date: "today" = {today}, "tomorrow" = next day.
   - Parse ALL date formats: "17.02" = February 17 of current year, "15 февраля" = February 15, "20 числа" = 20th of current month, "в среду" / "Wednesday" = nearest upcoming Wednesday.
   - Parse Russian words: "сегодня"=today, "завтра"=tomorrow, "послезавтра"=day after tomorrow.
   - RECURRING EVENTS: If the text describes a recurring schedule ("каждый день", "every morning", "каждое утро", "по вторникам", "every Friday"), return the NEXT upcoming occurrence date from {today}. For daily events use {today}.
   - If NO specific date, day of week, or recurrence pattern is mentioned, ASSUME IT IS HAPPENING TODAY ({today}).
   - Return null ONLY if the message is clearly NOT an event announcement (e.g. a review of past event, discussion, question). An actual event MUST always have a date.
   - YEAR VALIDATION: When the text does not specify a year, use the year from {today}. If the resulting date is MORE than 14 days in the past, treat it as a past event and return is_event=false. If the resulting date is MORE than 60 days in the future, double-check the year.
5. Title: Bilingual JSON object with keys "en" and "ru". Short catchy title, max 30 chars. Translate if needed.
6. Summary: Bilingual JSON object with keys "en" and "ru". One sentence, max 80 chars. Translate if needed.
7. Description: Bilingual JSON object with keys "en" and "ru". Attractive event announcement, 2-4 sentences, max 500 chars. Translate if needed.
8. Text Cleanliness: ALL text fields (title, summary, description) MUST be plain text. STRICTLY NO HTML tags, NO Markdown formatting (like **, _, #, ```), NO emojis in the text, and NO conversational filler. Just the pure, clean text.
9. 🚨 EXCLUSIONS — return is_event=false if ANY of these apply:
   - It is a question ("where is it?", "who knows?"), personal discussion, or service offer
   - It is an event cancellation ("cancelled", "won't be happening", "отмена")
   - location_name is null after checking BOTH text AND chat name
   STRICT RULE: An event without a location is NOT an event. DOUBLE-CHECK: before returning is_event=true, verify that location_name is NOT null/empty.
10. IMPORTANT: extract ONLY ONE object (the nearest/most relevant event).
11. 🚨 GEO-FILTER: This app is ONLY for Koh Phangan. Return is_event=false if the event is explicitly on Koh Tao, Koh Samui, Samui, Bangkok, Pattaya, Chiang Mai, Bophut, Fisherman's Village Samui, or any other location clearly NOT on Koh Phangan.

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

        required_bilingual = ["title", "summary", "description"]
        for field in required_bilingual:
            val = result.get(field)
            if not isinstance(val, dict):
                val_str = str(val) if val is not None else "N/A"
                if val_str.lower() in ("none", "null", ""):
                    val_str = "N/A"
                result[field] = {"en": val_str, "ru": val_str}
            else:
                if not val.get("en"): val["en"] = "N/A"
                if not val.get("ru"): val["ru"] = "N/A"

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
