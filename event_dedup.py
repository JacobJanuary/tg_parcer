import re
import logging
from db import Database

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ0-9]+")

class EventDedup:
    """
    Двухуровневая дедупликация:
      1. Exact hash — title+date+location (быстрый отсев)
      2. Fuzzy tokens — Jaccard similarity >= 0.6 при совпадающей дате
    """

    SIMILARITY_THRESHOLD = 0.6

    def __init__(self):
        self._exact: set[str] = set()
        self._events: list[dict] = []

    async def load_from_db(self, db: Database, days: int = 14):
        """Pre-loads recent events from PostgreSQL to enable cross-source deduplication."""
        try:
            # We get both TODOTODAY and LISTENER events
            rows = await db.pool.fetch(f"""
                SELECT title->>'en' as title_en, title->>'ru' as title_ru, event_date, location_name
                FROM events
                WHERE detected_at >= NOW() - INTERVAL '{days} days'
            """)
            for r in rows:
                ev_time = str(r["event_date"])[:10] if r["event_date"] else "" 
                title = r["title_en"] or r["title_ru"] or ""
                ev = {
                    "title": title,
                    "date": ev_time,
                    "location_name": r["location_name"] or ""
                }
                
                key = self._exact_key(ev)
                self._exact.add(key)
                
                self._events.append({
                    "title": ev["title"],
                    "date": ev["date"],
                    "tokens": self._tokenize(ev["title"]),
                })
            logger.info(f"🛡️ EventDedup loaded {len(self._events)} recent events from DB.")
        except Exception as e:
            logger.error(f"Failed to load deduplication history: {e}")

    @staticmethod
    def _normalize(text) -> str:
        if isinstance(text, dict):
            text = text.get("en", "") or text.get("ru", "") or ""
        return str(text or "").lower().strip()

    @staticmethod
    def _tokenize(title) -> set[str]:
        if isinstance(title, dict):
            title = title.get("en", "") or title.get("ru", "") or ""
        tokens = _TOKEN_RE.findall(str(title).lower())
        return {t[:5] for t in tokens if len(t) > 1}

    @staticmethod
    def _jaccard(a: set, b: set) -> float:
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)
        
    @staticmethod
    def _parse_date(d: str) -> str:
        d = str(d).strip()
        if not d or d == "TBD":
            return "TBD"
        
        # If it's already YYYY-MM-DD (from DB)
        if re.match(r"^\d{4}-\d{2}-\d{2}", d):
            return d[:10]
            
        from dateutil import parser
        try:
            # fuzzy=True robustly ignores "Tuesday," "th", "nd" around the date tokens
            parsed = parser.parse(d, fuzzy=True)
            return parsed.strftime("%Y-%m-%d")
        except:
             return d[:10]

    def _exact_key(self, ev: dict) -> str:
        date_str = self._parse_date(ev.get("date", ""))
        return (
            self._normalize(ev.get("title", ""))
            + "|" + date_str
            + "|" + self._normalize(ev.get("location_name", ""))
        )

    def _dates_compatible(self, d1: str, d2: str) -> bool:
        pd1 = self._parse_date(d1)
        pd2 = self._parse_date(d2)
        
        if pd1 == "TBD" or pd2 == "TBD" or not pd1 or not pd2:
            return True
        return pd1 == pd2

    def _times_compatible(self, t1: str, t2: str) -> bool:
        t1 = str(t1).strip()
        t2 = str(t2).strip()
        if not t1 or not t2 or t1 == "TBD" or t2 == "TBD" or t1 == "None" or t2 == "None":
            return True
            
        # extract first 5 chars e.g. 18:00
        return t1[:5] == t2[:5]

    def is_duplicate(self, event: dict) -> bool:
        """Returns True if the event is a duplicate."""
        key = self._exact_key(event)
        if key in self._exact:
            return True
        self._exact.add(key)

        tokens = self._tokenize(event.get("title", ""))
        date = str(event.get("date", ""))[:10]
        time = str(event.get("time", ""))[:5]

        for stored in self._events:
            if not self._dates_compatible(date, stored["date"]):
                continue
                
            stored_time = stored.get("time", "")
            if not self._times_compatible(time, stored_time):
                continue
                
            sim = self._jaccard(tokens, stored.get("tokens", set()))
            if sim >= self.SIMILARITY_THRESHOLD:
                logger.info(
                    f"🛑 Pre-flight fuzzy dedup caught: «{event.get('title')}» ≈ «{stored['title']}» (sim={sim:.2f})"
                )
                return True

        self._events.append({
            "title": event.get("title", ""),
            "date": date,
            "time": time,
            "tokens": tokens,
        })
        return False
