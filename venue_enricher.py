"""
Venue Enricher — обогащение площадок через Gemini + Google Search grounding.

Двухуровневая логика:
  1. VenueCache — JSON-кэш (data/venues.json), lookup по нормализованному имени
  2. VenueEnricher — Gemini 3 Flash + google_search → GPS, Maps, Instagram

Использование:
    enricher = VenueEnricher()
    venue_data = await enricher.enrich("AUM Sound Healing Center")
    enricher.close()
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from utils import RateLimiter

import httpx
from google import genai
from google.genai import types
import warnings

warnings.filterwarnings(
    "ignore",
    message="TOO_MANY_TOOL_CALLS is not a valid FinishReason",
    category=UserWarning,
)

import config
from db import Database

logger = logging.getLogger(__name__)

# Путь к кэшу площадок
VENUES_PATH = os.path.join(os.path.dirname(__file__), "data", "venues.json")

# ─── Промпт для поиска площадки ───

# ─── Pydantic Схема для Структурированного Вывода ───
from pydantic import BaseModel, Field
from typing import Optional

class VenueData(BaseModel):
    found: bool = Field(description="True если локация найдена с точными координатами")
    name: Optional[str] = Field(description="Официальное название на Google Maps. Иначе null")
    lat: Optional[float] = Field(description="Широта (latitude). Иначе null")
    lng: Optional[float] = Field(description="Долгота (longitude). Иначе null")
    google_maps_url: Optional[str] = Field(description="Ссылка на Google Maps. Иначе null")
    address: Optional[str] = Field(description="Физический адрес из Google Maps. Иначе null")


# ─── Промпт для поиска площадки ───

VENUE_PROMPT = """Find "{name}" on Google Maps, Koh Phangan island, Thailand.
If not on Koh Phangan, check Koh Samui or Koh Tao.
The name may have typos (e.g. "PlunkTone" = "PlankTone", "Кафе13" = "Cafe 13").

Reply STRICTLY with a valid JSON object matching this schema:
{{
  "found": boolean,
  "name": "string or null",
  "lat": float or null,
  "lng": float or null,
  "google_maps_url": "string or null",
  "address": "string or null"
}}
CRITICAL RULES:
1. Do not wrap code in markdown blocks (e.g. ```json).
2. Use DOUBLE quotes for all property names and strings.
3. ABSOLUTELY NO comments inside the JSON.
4. ABSOLUTELY NO trailing commas."""

# Суффиксы-локации для удаления
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

# Известные алиасы (нормализованные)
VENUE_ALIASES = {
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

_CLEAN_RE = re.compile(r"[^a-zA-Zа-яА-ЯёЁ0-9 ]")

CYRILLIC_TO_LATIN = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
    'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
    'я': 'ya'
}

def transliterate_ru(text: str) -> str:
    """Преобразование кириллицы в латиницу (Fallback-механизм)."""
    res = []
    for char in text:
        low_char = char.lower()
        if low_char in CYRILLIC_TO_LATIN:
            if char.isupper():
                res.append(CYRILLIC_TO_LATIN[low_char].capitalize())
            else:
                res.append(CYRILLIC_TO_LATIN[low_char])
        else:
            res.append(char)
    return "".join(res)

def _normalize_venue_name(name: str) -> str:
    """Нормализация имени площадки для lookup в кэше.

    1. lowercase + strip
    2. Убирает суффиксы-локации (', Koh Phangan' etc.)
    3. Убирает пунктуацию
    4. Коллапсирует пробелы
    5. Применяет alias-таблицу
    """
    name = name.lower().strip()
    # Strip location suffixes
    for suffix in _LOCATION_SUFFIXES:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    name = _CLEAN_RE.sub("", name)
    name = " ".join(name.split())  # collapse whitespace
    return VENUE_ALIASES.get(name, name)


# ─── Venue Cache ───

class VenueCache:
    """Memory-кэш площадок + PostgreSQL persistence."""

    def __init__(self, db=None):
        self.db = db  # Database instance
        self._data: dict[str, dict] = {}

    async def load_from_pg(self):
        """Загрузить все venues (aliases) из PostgreSQL в memory cache."""
        if not self.db:
            return
        try:
            rows = await self.db.pool.fetch("""
                SELECT va.query, v.name, v.lat, v.lng, v.address, va.venue_id
                FROM venue_aliases va
                LEFT JOIN venues v ON va.venue_id = v.id
            """)
            for r in rows:
                key = _normalize_venue_name(r["query"])
                data = dict(r)
                if data.get("venue_id") is None:
                    data["found"] = False
                else:
                    data["found"] = True
                self._data[key] = data
            logger.info(f"VenueCache: загружено {len(self._data)} алиасов из PG")
        except Exception as e:
            logger.warning(f"VenueCache: ошибка загрузки из PG: {e}")

    def get(self, venue_name: str) -> dict | None:
        """Lookup площадки по имени (нормализованному)."""
        key = _normalize_venue_name(venue_name)
        return self._data.get(key)

    async def aget(self, venue_name: str) -> dict | None:
        """Async lookup: сначала memory, потом PG."""
        key = _normalize_venue_name(venue_name)
        # Memory cache first
        cached = self._data.get(key)
        if cached is not None:
            return cached
        # PG fallback
        if self.db:
            try:
                row = await self.db.get_venue(venue_name)
                if row:
                    result = dict(row)
                    self._data[key] = result  # warm memory cache
                    return result
            except Exception:
                pass
        return None

    def put(self, venue_name: str, data: dict):
        """Сохранить данные площадки (memory only)."""
        key = _normalize_venue_name(venue_name)
        data["_cached_at"] = datetime.now().isoformat()
        data["_original_query"] = venue_name
        self._data[key] = data

    async def aput(self, venue_name: str, data: dict):
        """Async save: memory + PG."""
        self.put(venue_name, data)  # memory
        if self.db:
            try:
                await self.db.upsert_venue(venue_name, data)
            except Exception as e:
                logger.debug(f"VenueCache PG put error: {e}")

    def __len__(self):
        return len(self._data)



# ─── Venue Enricher ───

class VenueEnricher:
    """
    Обогащает площадки через Gemini + Google Search grounding.

    Поток:
      1. Нормализует имя площадки
      2. Проверяет VenueCache
      3. Если miss — запрос к Gemini с google_search tool
      4. Парсит JSON, сохраняет в кэш
    """

    def __init__(self, db=None):
        gemini_key, gemini_proxy = config.validate_gemini()

        http_options = {"timeout": 60_000}

        self._http_client = None
        if gemini_proxy:
            self._http_client = httpx.Client(
                proxy=gemini_proxy,
                timeout=60.0,
            )

        self.client = genai.Client(
            api_key=gemini_key,
            http_options=http_options,
        )

        if self._http_client:
            self.client._api_client._httpx_client = self._http_client

        self.model = "gemini-2.5-flash"
        self.fallback_model = "gemini-2.5-flash-lite"

        self.cache = VenueCache(db=db)
        self.limiter = RateLimiter(max_requests=100, period=60.0)

        # Статистика
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "enriched": 0,
            "not_found": 0,
            "errors": 0,
            "fallbacks": 0,
        }

    @staticmethod
    def _is_transient(error: Exception) -> bool:
        """Проверяет, является ли ошибка временной (стоит retry)."""
        s = str(error)
        return any(x in s for x in (
            "504", "499", "503", "500", "429",
            "DEADLINE_EXCEEDED", "CANCELLED",
            "timed out", "timeout", "ResourceExhausted", "Quota",
            "ServerError", "TOO_MANY_TOOL_CALLS",
        ))

    async def _call_google_maps_api(self, venue_name: str) -> dict | None:
        """Резервный вызов Google Places API (Text Search)."""
        api_key = config.GOOGLE_MAPS_API_KEY
        if not api_key:
            return None
            
        query = f"{venue_name} Koh Phangan"
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params={"query": query, "key": api_key})
                data = resp.json()
                
                if data.get("status") == "OK" and data.get("results"):
                    best = data["results"][0]
                    loc = best.get("geometry", {}).get("location", {})
                    lat, lng = loc.get("lat"), loc.get("lng")
                    
                    if lat and lng:
                        place_id = best.get("place_id", "")
                        maps_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else ""
                        logger.info(f"🗺️ Google Maps API Fallback SUCCESS: {venue_name} -> {best.get('name')}")
                        return {
                            "found": True,
                            "name": best.get("name", venue_name),
                            "lat": float(lat), "lng": float(lng),
                            "google_maps_url": maps_url,
                            "address": best.get("formatted_address", "")
                        }
        except Exception as e:
            logger.error(f"Google Maps fallback error for '{venue_name}': {e}")
        return None

    async def _call_gemini(self, model: str, prompt: str) -> str:
        """Один вызов Gemini с google_search tool, retry до 3 раз.

        Обрабатывает TOO_MANY_TOOL_CALLS finish_reason — извлекает
        частичный текст из response если он есть.
        """
        grounding_tool = types.Tool(google_search=types.GoogleSearch())
        max_retries = 3
        backoff = 2

        for attempt in range(1, max_retries + 1):
            try:
                response = await asyncio.to_thread(
                    self.client.models.generate_content,
                    model=model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[grounding_tool],
                        temperature=0.1,
                        max_output_tokens=8192,
                    ),
                )

                # Извлекаем текст из response
                text = None
                finish_reason = ""
                if response.candidates:
                    finish_reason = str(response.candidates[0].finish_reason)
                
                if response.text:
                    text = response.text.strip()
                elif response.candidates:
                    for part in (response.candidates[0].content.parts or []):
                        if hasattr(part, 'text') and part.text:
                            text = part.text.strip()
                            break

                if text:
                    # Убираем возможный маркдаун
                    text = text.replace("```json", "").replace("```", "").strip()
                    
                    # Иногда Gemini отвечает двумя одинаковыми JSON подряд (Extra data error). 
                    # Разделяем их и берем только первый.
                    import re
                    if re.search(r'\}\s*\{', text):
                        text = re.split(r'\}\s*\{', text)[0] + '}'
                        
                    match = re.search(r'(\{.*\})', text, re.DOTALL)
                    if match:
                        text = match.group(1).strip()
                if not text:
                    if "TOO_MANY_TOOL_CALLS" in finish_reason:
                        raise ValueError("TOO_MANY_TOOL_CALLS: model looped on tools")
                    raise ValueError(f"Empty response from {model} (finish_reason: {finish_reason})")

                try:
                    import json
                    result = json.loads(text)
                except json.JSONDecodeError as je:
                    # Резервный парсинг для одинарных кавычек и лишних запятых
                    import ast
                    try:
                        loose_text = text.replace('true', 'True').replace('false', 'False').replace('null', 'None')
                        result = ast.literal_eval(loose_text)
                    except Exception:
                        # 1. Проверяем, может модель просто ответила текстом
                        lower_text = text.lower()
                        if "no results" in lower_text or "not found" in lower_text or "not find" in lower_text:
                            return {"found": False}
                            
                        # 2. Emergency Fallback: Regex extraction (для обрезанных JSON)
                        import re
                        m_name = re.search(r'["\']?name["\']?\s*:\s*["\']([^"\']+)["\']', loose_text)
                        m_lat = re.search(r'["\']?lat["\']?\s*:\s*([-\d\.]+)', text)
                        m_lng = re.search(r'["\']?lng["\']?\s*:\s*([-\d\.]+)', text)
                        
                        if m_lat and m_lng:
                            logger.info("Regex extraction successfully reconstructed the truncated JSON coordinates!")
                            return {
                                "found": True,
                                "name": m_name.group(1).strip() if m_name else "",
                                "lat": float(m_lat.group(1)),
                                "lng": float(m_lng.group(1)),
                                "google_maps_url": "",
                                "address": ""
                            }
                            
                        # Если ничего не помогло:
                        logger.warning(f"VenueEnricher completely failed to parse JSON structure:\n{text}")
                        raise je # прокидаем оригинальную ошибку json наверх (вызовет retry)
                
                return result

            except Exception as e:
                if self._is_transient(e) and attempt < max_retries:
                    wait = backoff * (2 ** (attempt - 1))
                    logger.warning(
                        f"⏳ {model} attempt {attempt}/{max_retries} "
                        f"failed ({type(e).__name__}), retry in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

    async def enrich(self, venue_name: str) -> dict | None:
        """
        Обогатить площадку: вернуть dict с lat, lng, maps.
        Возвращает None если площадка не найдена или TBD.
        """
        if not venue_name or venue_name in ("TBD", "N/A", ""):
            return None

        # 1. Cache lookup (async — memory + PG)
        cached = await self.cache.aget(venue_name)
        if cached is not None:
            self.stats["cache_hits"] += 1
            return cached if cached.get("found", True) else None

        self.stats["cache_misses"] += 1

        # 2. Gemini + Google Search
        await self.limiter.acquire()

        # Build execution pipeline: (search_string, model)
        attempts_to_make = [
            (venue_name, self.model)
        ]

        import re
        
        # 1. Fallback: Add 'Koh Phangan' to English/Mixed names if not present
        if not re.search(r'phangan|панган', venue_name, re.IGNORECASE):
            phangan_hint = f"{venue_name} Koh Phangan"
            attempts_to_make.append((phangan_hint, self.model))

        # 2. Fallback: Handle Cyrillic
        if re.search(r'[А-Яа-яЁё]', venue_name):
            translit = transliterate_ru(venue_name).strip()
            translit_hint = f"{translit} Koh Phangan"
            logger.info(f"🔄 Cyrillic detected in '{venue_name}'. Added fallback: '{translit_hint}'")
            attempts_to_make.append((translit_hint, self.model))
            
        # 3. Final Fallback: use lite model
        attempts_to_make.append((venue_name, self.fallback_model))

        for search_name, model in attempts_to_make:
            prompt = VENUE_PROMPT.format(name=search_name)
            try:
                result = await self._call_gemini(model, prompt)

                if model != self.model:
                    self.stats["fallbacks"] += 1

                if result.get("found", True):
                    # Валидация координат
                    lat = result.get("lat")
                    lng = result.get("lng")
                    if lat and lng and isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                        venue_data = {
                            "found": True,
                            "name": result.get("name", venue_name),
                            "lat": float(lat),
                            "lng": float(lng),
                            "google_maps_url": result.get("google_maps_url", ""),
                            "address": result.get("address", ""),
                        }
                        await self.cache.aput(venue_name, venue_data)
                        self.stats["enriched"] += 1
                        logger.info(
                            f"✅ Venue enriched: {venue_name} (via '{search_name}') → "
                            f"({venue_data['lat']}, {venue_data['lng']})"
                        )
                        return venue_data

                # If found=False, we DO NOT violently cache and return None right away. 
                # We continue the loop to try the next item in `attempts_to_make`.
                # If we've exhausted all attempts, we will cache the failure at the very end.
                logger.info(f"⚠️ Search for '{search_name}' with {model} yielded no coordinates. Trying next fallback...")

            except json.JSONDecodeError as e:
                logger.warning(f"VenueEnricher JSON error for '{search_name}': {e}")
                self.stats["errors"] += 1
                continue

            except Exception as e:
                if model != self.fallback_model:
                    logger.warning(f"💥 {model} failed for '{search_name}' ({e}) → trying next fallback")
                    self.stats["fallbacks"] += 1
                else:
                    logger.error(f"VenueEnricher error for '{search_name}': {e}")
                    self.stats["errors"] += 1
                continue

        # 3. Ultimate Fallback: Google Maps API
        if config.GOOGLE_MAPS_API_KEY:
            maps_result = await self._call_google_maps_api(venue_name)
            if maps_result:
                self.stats["fallbacks"] += 1
                await self.cache.aput(venue_name, maps_result)
                self.stats["enriched"] += 1
                return maps_result

        # Exhausted all attempts
        await self.cache.aput(venue_name, {"found": False})
        self.stats["not_found"] += 1
        return None

    async def enrich_event(self, event: dict) -> dict:
        """
        Обогатить ивент данными о площадке.
        Добавляет ключ 'venue' в event dict.
        """
        location = event.get("location_name", "")
        venue_data = await self.enrich(location)

        if venue_data:
            event["venue"] = {
                "found": True,
                "name": venue_data.get("name", location),
                "lat": venue_data.get("lat"),
                "lng": venue_data.get("lng"),
                "google_maps_url": venue_data.get("google_maps_url", ""),
                "instagram_url": venue_data.get("instagram_url"),
                "address": venue_data.get("address", ""),
            }

        return event

    def close(self):
        """Освобождение ресурсов."""
        if self._http_client:
            self._http_client.close()
