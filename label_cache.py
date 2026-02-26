"""
Local aria-label cache for todo_scraper.
Stores SHA-256 hashes of already-processed aria-label strings
to avoid redundant Gemini API calls on subsequent runs.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CACHE_PATH = os.path.join(BASE_DIR, "data", "todo_label_cache.json")


class LabelCache:
    """File-backed aria-label cache.

    Format: { "sha256_hash": "YYYY-MM-DD", ... }
    Value is the date when the label was last seen.
    Entries older than TTL_DAYS are pruned on load.
    """

    TTL_DAYS = 14

    def __init__(self, path: str = DEFAULT_CACHE_PATH):
        self._path = path
        self._data: dict[str, str] = {}

    # ── public API ──────────────────────────────────

    def load(self) -> None:
        """Load cache from disk and prune stale entries."""
        if not os.path.exists(self._path):
            logger.info("📦 Label cache file not found — starting fresh")
            self._data = {}
            return

        try:
            with open(self._path, "r") as f:
                self._data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"⚠️ Label cache corrupted, resetting: {e}")
            self._data = {}
            return

        # Prune entries older than TTL_DAYS
        cutoff = (datetime.utcnow() - timedelta(days=self.TTL_DAYS)).strftime("%Y-%m-%d")
        before = len(self._data)
        self._data = {k: v for k, v in self._data.items() if v >= cutoff}
        pruned = before - len(self._data)
        if pruned:
            logger.info(f"🧹 Label cache pruned {pruned} stale entries (>{self.TTL_DAYS}d)")
        logger.info(f"📦 Label cache loaded: {len(self._data)} entries")

    def contains(self, aria_label: str) -> bool:
        """Check if this aria-label was already processed."""
        return self._hash(aria_label) in self._data

    def add(self, aria_label: str) -> None:
        """Mark this aria-label as processed (today's date)."""
        self._data[self._hash(aria_label)] = datetime.utcnow().strftime("%Y-%m-%d")

    def save(self) -> None:
        """Persist cache to disk."""
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(self._data, f)

    def __len__(self) -> int:
        return len(self._data)

    # ── internals ───────────────────────────────────

    @staticmethod
    def _hash(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
