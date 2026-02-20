"""
Pre-filter модуль: отсеивает мусорные сообщения до вызова AI.

Многоступенчатая фильтрация:
  1. Blacklist — слова-маркеры спама/рекламы
  2. Длина — слишком короткие сообщения
  3. Whitelist boost — ключевые слова ивентов
  4. Паттерны дат/времени
  5. Паттерны мест
  6. Наличие медиа (фото-флаер)
"""

import re
from dataclasses import dataclass


@dataclass
class FilterResult:
    """Результат фильтрации сообщения."""
    passed: bool
    score: int
    reason: str


# ─── Blacklist: спам, реклама, услуги ───

BLACKLIST_WORDS = [
    # Недвижимость
    "сдам", "сниму", "аренда", "вилла", "кондо", "квартир", "комнат",
    "жильё", "жилье", "апартамент", "резиденс", "долгосрок", "краткосрок",
    # Визы и документы
    "visa", "виза", "разрешение на работу", "work permit", "extension",
    # Крипта и обмен
    "usdt", "крипт", "биткоин", "btc", "eth", "обмен", "меняю", "курс валют",
    "exchange rate", "p2p",
    # Транспорт
    "байк", "nmax", "скутер", "мотобайк", "аренда байк", "rent bike",
    # Услуги
    "ноготочки", "массаж", "маникюр", "педикюр", "наращивание", "эпиляци",
    "трансфер", "такси", "доставка", "клининг", "стирка", "уборка",
    "ремонт", "сантехник", "электрик",
    # Косметология
    "реконструкц", "лифтинг", "фотосесс", "bbl", "ботокс", "филлер",
    # Продажа
    "продам", "куплю", "продаю", "б/у", "торг",
    # Работа
    "ищу работу", "вакансия", "требуется", "зарплата",
]

# Компилируем в один паттерн для скорости
_blacklist_pattern = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in BLACKLIST_WORDS) + r")",
    re.IGNORECASE,
)


# ─── Whitelist: маркеры ивентов ───

WHITELIST_WORDS = [
    # Русские
    "ивент", "мероприятие", "вечеринка", "тусовка", "тусовк", "митап",
    "нетворкинг", "встреча", "сходка", "движ", "движух",
    "спорт", "йога", "серфинг", "волейбол", "футбол", "бег",
    "мастер-класс", "воркшоп", "лекция", "семинар",
    "концерт", "фестиваль", "вечер", "открытие",
    "вход", "билет", "регистрация", "вход свободный",
    "приглашаем", "приходите", "ждём", "ждем", "присоединяйтесь",
    "добро пожаловать",
    # English
    "event", "party", "meetup", "networking", "gathering",
    "workshop", "masterclass", "lecture", "seminar",
    "concert", "festival", "opening", "dj", "live music",
    "ticket", "free entry", "registration", "rsvp",
    "join us", "welcome", "come join",
    "sunset", "beach party", "pool party", "rooftop",
]

_whitelist_pattern = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in WHITELIST_WORDS) + r")",
    re.IGNORECASE,
)


# ─── Паттерны дат и времени ───

DATE_TIME_PATTERNS = [
    r"\d{1,2}[./]\d{1,2}(?:[./]\d{2,4})?",           # 15.02, 15/02/2025
    r"\d{1,2}\s*(?:января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)",
    r"\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}",
    r"\bв\s+\d{1,2}[:.]\d{2}\b",                       # в 19:00, в 20.30
    r"\b(?:at|from)\s+\d{1,2}[:.]\d{2}\b",             # at 19:00, from 20:30
    r"\d{1,2}[:.]\d{2}\s*(?:-|–|—)\s*\d{1,2}[:.]\d{2}", # 19:00 - 23:00
    r"\b(?:сегодня|завтра|послезавтра)\b",              # сегодня, завтра
    r"\b(?:today|tomorrow)\b",
    r"\b(?:в\s+)?(?:понедельник|вторник|сред[уы]|четверг|пятниц[уы]|суббот[уы]|воскресень[ея])\b",
    r"\b(?:on\s+)?(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
]

_datetime_pattern = re.compile(
    "|".join(DATE_TIME_PATTERNS),
    re.IGNORECASE,
)


# ─── Паттерны мест ───

LOCATION_PATTERNS = [
    r"📍",
    r"\b(?:beach\s*club|bar|café|cafe|кафе|бар|ресторан|restaurant)\b",
    r"\b(?:coworking|коворкинг|hub|хаб|space|пространство)\b",
    r"\b(?:клуб|club|pool|бассейн|rooftop|крыша)\b",
    r"\b(?:адрес|address|место|location|venue|площадка)\b",
    r"\b(?:google\s*maps|goo\.gl|maps\.app)\b",
]

_location_pattern = re.compile(
    "|".join(LOCATION_PATTERNS),
    re.IGNORECASE,
)


# ─── Минимальная длина ───

MIN_TEXT_LENGTH = 80


# ─── Порог для прохождения ───

SCORE_THRESHOLD = 2


def _strip_urls(text: str) -> str:
    """Удаляет URL из текста (для подсчёта длины)."""
    return re.sub(r"https?://\S+", "", text)


def check(text: str, has_media: bool = False) -> FilterResult:
    """
    Проверка сообщения через все фильтры.

    Args:
        text: Текст сообщения.
        has_media: Есть ли прикреплённое фото/видео.

    Returns:
        FilterResult с результатом проверки.
    """
    if not text:
        return FilterResult(passed=False, score=0, reason="empty")

    # 1. Blacklist — немедленный drop
    bl_match = _blacklist_pattern.search(text)
    if bl_match:
        return FilterResult(passed=False, score=-1, reason=f"blacklist: «{bl_match.group()}»")

    # 2. Длина (без URL)
    clean_text = _strip_urls(text)
    if len(clean_text.strip()) < MIN_TEXT_LENGTH:
        return FilterResult(passed=False, score=0, reason=f"too_short: {len(clean_text.strip())} chars")

    # 3-6. Скоринг
    score = 0
    reasons = []

    # Whitelist слова
    wl_matches = _whitelist_pattern.findall(text)
    if wl_matches:
        score += min(len(wl_matches), 3)  # Макс +3 за whitelist
        reasons.append(f"whitelist({len(wl_matches)}): {', '.join(set(wl_matches[:3]))}")

    # Дата/время
    dt_matches = _datetime_pattern.findall(text)
    if dt_matches:
        score += 2
        reasons.append(f"datetime({len(dt_matches)})")

    # Место
    loc_matches = _location_pattern.findall(text)
    if loc_matches:
        score += 1
        reasons.append(f"location({len(loc_matches)})")

    # Медиа
    if has_media:
        score += 1
        reasons.append("has_media")

    passed = score >= SCORE_THRESHOLD
    reason_str = "; ".join(reasons) if reasons else "no_signals"

    return FilterResult(
        passed=passed,
        score=score,
        reason=f"score={score}/{SCORE_THRESHOLD} [{reason_str}]",
    )


def check_batch(messages: list[dict]) -> dict:
    """
    Прогон пачки сообщений через фильтр со статистикой.

    Args:
        messages: Список словарей с ключами 'text' и 'media_type'.

    Returns:
        Словарь со статистикой и списками прошедших/отсеянных.
    """
    passed = []
    dropped = []

    for msg in messages:
        text = msg.get("text", "")
        has_media = bool(msg.get("media_type"))
        result = check(text, has_media)
        msg["_filter"] = {
            "passed": result.passed,
            "score": result.score,
            "reason": result.reason,
        }
        if result.passed:
            passed.append(msg)
        else:
            dropped.append(msg)

    return {
        "total": len(messages),
        "passed": len(passed),
        "dropped": len(dropped),
        "pass_rate": f"{len(passed)/len(messages)*100:.1f}%" if messages else "0%",
        "passed_messages": passed,
        "dropped_messages": dropped,
    }
