import json
import time
from collections import Counter, deque
from typing import Any, Dict, Iterable, Optional

from ..monitoring.metrics import metrics

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

    def delete(self, key: str) -> None:
        self.store.pop(key, None)


class DistributedCache:
    """Simple Redis-based distributed cache."""

    def __init__(self, url: str, ttl: int = 3600) -> None:
        self.url = url
        self.ttl = ttl
        try:
            self.client = aioredis.from_url(url) if aioredis else None
        except Exception:
            self.client = None

    async def get(self, key: str) -> Optional[Any]:
        if not self.client:
            return None
        try:
            raw = await self.client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def set(self, key: str, value: Any) -> None:
        if not self.client:
            return
        try:
            await self.client.set(key, json.dumps(value), ex=self.ttl)
        except Exception:
            pass

    async def delete(self, key: str) -> None:
        if not self.client:
            return
        try:
            await self.client.delete(key)
        except Exception:
            pass


class RvuCache:
    """Cache RVU data looked up from a database."""

    def __init__(
        self,
        db: "BaseDatabase",
        ttl: int = 3600,
        distributed: Optional[DistributedCache] = None,
        predictive_ahead: int = 2,
    ) -> None:
        from ..db.base import BaseDatabase

        self.db = db
        self.cache = InMemoryCache(ttl)
        self.distributed = distributed
        self.predictive_ahead = predictive_ahead
        self.access_history: deque[str] = deque(maxlen=100)
        self.hits = 0
        self.misses = 0

    async def get(
        self, code: str, *, _prefetch: bool = False
    ) -> Optional[Dict[str, Any]]:
        item = self.cache.get(code)
        if not item and self.distributed:
            item = await self.distributed.get(code)
            if item:
                self.cache.set(code, item)
        if item:
            self.hits += 1
            metrics.inc("rvu_cache_hits")
            if not _prefetch:
                self.access_history.append(code)
                if len(self.access_history) % 10 == 0:
                    await self._prefetch_trending()
            self._update_ratio()
            return item

        self.misses += 1
        metrics.inc("rvu_cache_misses")
        rows = await self.db.fetch(
            "SELECT * FROM rvu_data WHERE procedure_code = $1",
            code,
        )
        if rows:
            self.cache.set(code, rows[0])
            if self.distributed:
                await self.distributed.set(code, rows[0])
            if not _prefetch:
                self.access_history.append(code)
                await self._predictive_warm(code)
                if len(self.access_history) % 10 == 0:
                    await self._prefetch_trending()
            self._update_ratio()
            return rows[0]
        self._update_ratio()
        return None

    async def get_many(
        self, codes: Iterable[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Retrieve multiple RVU records with a single database query."""
        unique = [c for c in set(codes) if c]
        result: Dict[str, Optional[Dict[str, Any]]] = {c: None for c in unique}
        missing: list[str] = []
        for code in unique:
            item = self.cache.get(code)
            if not item and self.distributed:
                item = await self.distributed.get(code)
                if item:
                    self.cache.set(code, item)
            if item:
                self.hits += 1
                metrics.inc("rvu_cache_hits")
                result[code] = item
            else:
                missing.append(code)
                self.misses += 1
                metrics.inc("rvu_cache_misses")
        if missing:
            placeholders = ", ".join(f"${i+1}" for i in range(len(missing)))
            rows = await self.db.fetch(
                f"SELECT * FROM rvu_data WHERE procedure_code IN ({placeholders})",
                *missing,
            )
            for row in rows:
                code = row.get("procedure_code")
                if not code:
                    continue
                result[code] = row
                self.cache.set(code, row)
                if self.distributed:
                    await self.distributed.set(code, row)
        self._update_ratio()
        return result

    async def warm_cache(self, codes: Iterable[str]) -> None:
        for code in codes:
            try:
                await self.get(code, _prefetch=True)
            except Exception:
                continue

    async def invalidate(self, code: str) -> None:
        self.cache.delete(code)
        if self.distributed:
            await self.distributed.delete(code)

    def _update_ratio(self) -> None:
        total = self.hits + self.misses
        if total:
            metrics.set("rvu_cache_hit_ratio", self.hits / total)
            metrics.set("rvu_cache_miss_ratio", self.misses / total)

    async def _prefetch_trending(self) -> None:
        if not self.access_history:
            return
        common = Counter(self.access_history).most_common(1)[0][0]
        await self._predictive_warm(common)

    @staticmethod
    def _split_code(code: str) -> tuple[str, int] | None:
        prefix = ""
        number = ""
        for ch in code:
            if ch.isdigit():
                number += ch
            else:
                prefix += ch
        if not number:
            return None
        try:
            return prefix, int(number)
        except ValueError:
            return None

    async def _predictive_warm(self, code: str) -> None:
        parsed = self._split_code(code)
        if not parsed:
            return
        prefix, num = parsed
        for i in range(1, self.predictive_ahead + 1):
            next_code = f"{prefix}{num + i}"
            if self.cache.get(next_code) is None:
                try:
                    await self.get(next_code, _prefetch=True)
                except Exception:
                    continue
