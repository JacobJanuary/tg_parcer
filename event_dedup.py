import re
import logging
from db import Database

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ0-9]+")

class EventDedup:
    """
    4-tier deduplication:
      1. Exact hash — title+date+location (fast reject)
      2. Venue-aware fuzzy title — Jaccard >= 0.40 when same venue+time
      3. Summary cross-check — summary Jaccard >= 0.50 when venue+time match
      4. Gemini Flash Lite — binary "same event?" question for edge cases
    Falls back to global fuzzy title (Jaccard >= 0.60) when venue/time don't match.
    """

    SIMILARITY_THRESHOLD = 0.60       # global fuzzy (no venue match)
    VENUE_SIMILARITY_THRESHOLD = 0.40 # lower threshold when same venue+time
    SUMMARY_THRESHOLD = 0.50          # summary check for venue+time match
    VENUE_MATCH_THRESHOLD = 0.50      # how similar venues must be to count as "same"

    def __init__(self):
        self._exact: set[str] = set()
        self._events: list[dict] = []

    async def load_from_db(self, db: Database, days: int = 14):
        """Pre-loads recent events from PostgreSQL to enable cross-source deduplication."""
        try:
            rows = await db.pool.fetch(f"""
                SELECT title->>'en' as title_en, title->>'ru' as title_ru,
                       event_date, event_time, location_name,
                       summary->>'en' as summary_en
                FROM events
                WHERE detected_at >= NOW() - INTERVAL '{days} days'
            """)
            for r in rows:
                ev_time = str(r["event_date"])[:10] if r["event_date"] else ""
                title = r["title_en"] or r["title_ru"] or ""
                ev = {
                    "title": title,
                    "date": ev_time,
                    "time": r.get("event_time") or "",
                    "location_name": r["location_name"] or ""
                }

                key = self._exact_key(ev)
                self._exact.add(key)

                self._events.append({
                    "title": ev["title"],
                    "date": ev["date"],
                    "time": ev["time"],
                    "tokens": self._tokenize(ev["title"]),
                    "location_name": ev["location_name"],
                    "loc_tokens": self._tokenize(ev["location_name"]),
                    "summary_tokens": self._tokenize(r.get("summary_en") or ""),
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

        if re.match(r"^\d{4}-\d{2}-\d{2}", d):
            return d[:10]

        lower_d = d.lower()
        from datetime import datetime, timedelta
        if lower_d == "today":
            return datetime.today().strftime("%Y-%m-%d")
        elif lower_d == "tomorrow":
            return (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

        from dateutil import parser
        try:
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

        return t1[:5] == t2[:5]

    def _venues_match(self, loc1_tokens: set, loc2_tokens: set) -> bool:
        """Check if two venues are the same based on token overlap."""
        sim = self._jaccard(loc1_tokens, loc2_tokens)
        return sim >= self.VENUE_MATCH_THRESHOLD

    async def _ask_gemini_lite(self, title_a: str, title_b: str,
                                summary_a: str, summary_b: str,
                                venue: str) -> bool:
        """Quick binary check via Gemini Flash Lite: are these the same event?"""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            from google import genai

            client = genai.Client()
            prompt = (
                f"Are these two listings for the SAME event? Answer ONLY 'Yes' or 'No'.\n\n"
                f"Event A: {title_a}\nSummary: {summary_a}\n\n"
                f"Event B: {title_b}\nSummary: {summary_b}\n\n"
                f"Both are at: {venue}"
            )
            response = await client.aio.models.generate_content(
                model="gemini-2.0-flash-lite",
                contents=prompt,
            )
            answer = response.text.strip().lower()
            is_same = answer.startswith("yes")
            logger.info(f"🤖 Gemini Lite dedup: '{title_a[:30]}' vs '{title_b[:30]}' → {answer}")
            return is_same
        except Exception as e:
            logger.warning(f"Gemini Lite dedup failed: {e}, defaulting to NOT duplicate")
            return False

    async def is_duplicate(self, event: dict) -> bool:
        """
        Returns True if the event is a duplicate.
        4-tier check:
          T1: exact hash
          T2: venue-aware fuzzy title (threshold 0.40 when same venue+time)
          T3: summary cross-check (threshold 0.50 when same venue+time, ANY title sim)
          T4: Gemini Flash Lite for edge cases (title ≥ 0.15 or summary ≥ 0.30)
        Falls back to global fuzzy (threshold 0.60) when venue/time don't match.
        """
        # --- Tier 1: Exact hash ---
        key = self._exact_key(event)
        if key in self._exact:
            return True
        self._exact.add(key)

        tokens = self._tokenize(event.get("title", ""))
        date = str(event.get("date", ""))[:10]
        time = str(event.get("time", ""))[:5]
        loc_name = self._normalize(event.get("location_name", ""))
        loc_tokens = self._tokenize(loc_name)
        summary_text = ""
        summary = event.get("summary")
        if isinstance(summary, dict):
            summary_text = summary.get("en", "") or summary.get("ru", "") or ""
        elif isinstance(summary, str):
            summary_text = summary
        summary_tokens = self._tokenize(summary_text)

        for stored in self._events:
            if not self._dates_compatible(date, stored["date"]):
                continue

            stored_time = stored.get("time", "")
            same_time = self._times_compatible(time, stored_time)
            same_venue = self._venues_match(loc_tokens, stored.get("loc_tokens", set()))

            title_sim = self._jaccard(tokens, stored.get("tokens", set()))

            if same_time and same_venue:
                # --- Tier 2: Venue-aware fuzzy title ---
                if title_sim >= self.VENUE_SIMILARITY_THRESHOLD:
                    logger.info(
                        f"🛑 Pre-flight fuzzy dedup caught: «{event.get('title')}» "
                        f"≈ «{stored['title']}» (sim={title_sim:.2f}, same venue+time)"
                    )
                    return True

                # --- Tier 3: Summary cross-check (no title gate!) ---
                # When same venue+time, summary alone is a strong signal
                sum_sim = self._jaccard(summary_tokens, stored.get("summary_tokens", set()))
                if sum_sim >= self.SUMMARY_THRESHOLD:
                    logger.info(
                        f"🛑 Summary dedup caught: «{event.get('title')}» "
                        f"≈ «{stored['title']}» (title={title_sim:.2f}, summary={sum_sim:.2f})"
                    )
                    return True

                # --- Tier 4: Gemini Flash Lite for edge cases ---
                # When title or summary partially overlap but neither is conclusive
                if title_sim >= 0.15 or sum_sim >= 0.30:
                    try:
                        is_same = await self._ask_gemini_lite(
                            str(event.get("title", "")),
                            stored.get("title", ""),
                            summary_text,
                            stored.get("summary_text", ""),
                            loc_name,
                        )
                        if is_same:
                            logger.info(
                                f"🛑 Gemini dedup caught: «{event.get('title')}» "
                                f"≈ «{stored['title']}» (AI confirmed)"
                            )
                            return True
                    except Exception:
                        pass
            else:
                # Global fuzzy (no venue match) — original behavior
                if not same_time:
                    continue
                if title_sim >= self.SIMILARITY_THRESHOLD:
                    logger.info(
                        f"🛑 Pre-flight fuzzy dedup caught: «{event.get('title')}» "
                        f"≈ «{stored['title']}» (sim={title_sim:.2f})"
                    )
                    return True

        self._events.append({
            "title": self._normalize(event.get("title", "")),
            "date": date,
            "time": time,
            "tokens": tokens,
            "location_name": loc_name,
            "loc_tokens": loc_tokens,
            "summary_tokens": summary_tokens,
            "summary_text": summary_text,
        })
        return False
