import json
import time
from typing import Any, Dict, Iterable, Optional

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - redis optional
    aioredis = None


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


class DistributedCache:
    """Simple Redis-based distributed cache."""

    def __init__(self, url: str, ttl: int = 3600) -> None:
        self.url = url
        self.ttl = ttl
        self.client = aioredis.from_url(url) if aioredis else None

    async def get(self, key: str) -> Optional[Any]:
        if not self.client:
            return None
        raw = await self.client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any) -> None:
        if not self.client:
            return
        await self.client.set(key, json.dumps(value), ex=self.ttl)


class RvuCache:
    """Cache RVU data looked up from a database."""

    def __init__(
        self,
        db: "BaseDatabase",
        ttl: int = 3600,
        distributed: Optional[DistributedCache] = None,
    ) -> None:
        from ..db.base import BaseDatabase

        self.db = db
        self.cache = InMemoryCache(ttl)
        self.distributed = distributed

    async def get(self, code: str) -> Optional[Dict[str, Any]]:
        item = self.cache.get(code)
        if not item and self.distributed:
            item = await self.distributed.get(code)
            if item:
                self.cache.set(code, item)
        if item:
            return item

        rows = await self.db.fetch(
            "SELECT * FROM rvu_data WHERE procedure_code = $1",
            code,
        )
        if rows:
            self.cache.set(code, rows[0])
            if self.distributed:
                await self.distributed.set(code, rows[0])
            return rows[0]
        return None

    async def warm_cache(self, codes: Iterable[str]) -> None:
        for code in codes:
            try:
                await self.get(code)
            except Exception:
                continue
