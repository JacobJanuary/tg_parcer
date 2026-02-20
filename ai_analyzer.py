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
    is_event: bool = Field(description="True если текст содержит реальное оффлайн-мероприятие")

class EventResult(BaseModel):
    is_event: bool = Field(description="True если это реальный ивент")
    title: Optional[str] = Field(description="Краткое цепляющее название до 30 символов")
    category: Optional[str] = Field(description="Одна из 5 категорий: Party, Sport, Business, Education, Chill")
    date: Optional[str] = Field(description="Дата в формате YYYY-MM-DD, если указана. Иначе null")
    time: Optional[str] = Field(description="Время в формате HH:MM, если указано. Иначе null")
    location_name: Optional[str] = Field(description="Название заведения для Google Maps. Иначе null")
    price_thb: Optional[int] = Field(description="Цена в батах (0 если бесплатно). Иначе null")
    summary: Optional[str] = Field(description="Суть в 1 предложение до 80 символов")
    description: Optional[str] = Field(description="Анонс мероприятия для афиши, 2-4 предложения")


# ─── Prompts ───

PRESCREEN_PROMPT = """Определи, содержит ли это сообщение из Telegram информацию о РЕАЛЬНОМ ОФФЛАЙН-МЕРОПРИЯТИИ (вечеринка, концерт, йога, митап, спорт, мастер-класс, нетворкинг, фестиваль, экскурсия, медитация, ретрит и т.д.).

НЕ считаются ивентами (возвращай is_event=false):
- Продажа/покупка: «Продам байк», «куплю iPhone», «б/у мебель»
- Аренда: «Сдам виллу», «ищу квартиру», «аренда байка»
- Обмен валют: «обмен USDT», «курс бата», «p2p»
- Услуги: «массаж», «трансфер», «клининг», «ноготочки»
- Вопросы/обсуждения: «подскажите, где проходит?», «кто знает?», «мы туда идем», общая разговорная болтовня
- Реклама каналов/ботов и онлайн-вебинары
- 🚨 КРИТИЧЕСКИ ВАЖНО: Анонсы, в которых НЕТ никаких указаний на физическое место (ни прямого адреса "Moo 5", ни брендированного названия заведения "AUM", "Prana", "Catch", "Osho", "Orion". Например "место в ЛС" или "приглашаю на группу" без локации) — это НЕ ивенты."""


EXTRACT_PROMPT = """Ты — AI-ассистент геолокационного приложения на Пхукете/Пангане.
Извлеки данные об ОФФЛАЙН-МЕРОПРИЯТИИ из текста.

ПРАВИЛА:
1. Категория: одна из "Party", "Sport", "Business", "Education", "Chill".
2. Цена (price_thb): число в батах, 0 если бесплатно. Иначе null.
3. Локация (location_name): точное название заведения для Google Maps. 🚨 ВАЖНО: Если прямого адреса нет, но само событие имеет брендированное имя (например "AUM DAY", "тренировки в Prana", "Ошо медитация", "Orion Healing Center"), извлекай бренд ("AUM", "Prana", "Osho", "Orion") как location_name. Иначе null.
4. Дата: "сегодня" = {today}, "завтра" = следующий день. Иначе null.
5. Title: краткое цепляющее название до 30 символов.
6. Summary: суть в 1 предложение до 80 символов.
7. Description: привлекательный анонс мероприятия для афиши, 2-4 предложения, до 500 символов. Передай атмосферу, укажи что будет и почему стоит прийти.
8. 🚨 ИСКЛЮЧЕНИЯ: Если это вопрос ("где проходит?"), личное обсуждение ("я перепутала"), предложение услуг (массаж) ИЛИ ЕСЛИ location_name РАВЕН null и его невозможно вывести из текста — возвращай is_event = false. СТРОГОЕ ПРАВИЛО: Ивент без локации (даже подразумеваемой) не является ивентом.
9. ВАЖНО: извлекай ТОЛЬКО ОДИН объект (самый ближайший/релевантный ивент)."""



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

        user_prompt = f"Чат: {chat_title}\n\nСообщение:\n{text[:1000]}"

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
        user_prompt = f"Чат: {chat_title}\n\nСообщение:\n{text[:2000]}"

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
