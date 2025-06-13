import time
from typing import Any, Dict, Optional


class InMemoryCache:
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self.store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self.store.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > self.ttl:
            del self.store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self.store[key] = (time.time(), value)


class RvuCache:
    """Cache RVU data looked up from a database."""

    def __init__(self, db: "BaseDatabase", ttl: int = 3600):
        from ..db.base import BaseDatabase
        self.db = db
        self.cache = InMemoryCache(ttl)

    async def get(self, code: str) -> Optional[Dict[str, Any]]:
        item = self.cache.get(code)
        if item:
            return item
        rows = await self.db.fetch(
            "SELECT * FROM rvu_data WHERE procedure_code = $1", code
        )
        if rows:
            self.cache.set(code, rows[0])
            return rows[0]
        return None

