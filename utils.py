"""
Общие утилиты проекта.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Простой rate limiter: не более N запросов за period секунд."""

    def __init__(self, max_requests: int = 10, period: float = 60.0):
        self.max_requests = max_requests
        self.period = period
        self._timestamps: list[float] = []

    async def acquire(self):
        """Ждём, если превысили лимит."""
        now = asyncio.get_event_loop().time()
        self._timestamps = [t for t in self._timestamps if now - t < self.period]

        if len(self._timestamps) >= self.max_requests:
            wait_time = self.period - (now - self._timestamps[0])
            if wait_time > 0:
                logger.info(f"Rate limit: ждём {wait_time:.1f}с")
                await asyncio.sleep(wait_time)

        self._timestamps.append(asyncio.get_event_loop().time())


def db_retry(max_retries: int = 3, base_delay: float = 1.0):
    """Декоратор для retry при ошибках соединения с БД."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (OSError, ConnectionError) as e:
                    last_exc = e
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"DB connection error (attempt {attempt + 1}/{max_retries}): {e}, retry in {delay:.1f}s")
                    await asyncio.sleep(delay)
                except Exception:
                    raise  # Не ретраим бизнес-ошибки
            raise last_exc
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator
