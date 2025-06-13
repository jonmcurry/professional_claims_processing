try:
    import asyncpg
except Exception:  # pragma: no cover - allow missing dependency in tests
    asyncpg = None
from typing import Iterable, Any
import time

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from ..utils.cache import InMemoryCache
from ..utils.errors import DatabaseConnectionError, QueryError, CircuitBreakerOpenError

from .base import BaseDatabase
from ..config.config import PostgresConfig
from ..monitoring.metrics import metrics


class PostgresDatabase(BaseDatabase):
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None
        self.replica_pool: asyncpg.pool.Pool | None = None
        self.circuit_breaker = CircuitBreaker()
        # Simple in-memory cache for query results
        self.query_cache = InMemoryCache(ttl=60)

    async def connect(self) -> None:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        try:
            self.pool = await asyncpg.create_pool(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                min_size=self.cfg.min_pool_size,
                max_size=self.cfg.max_pool_size,
            )
            if self.cfg.replica_host:
                self.replica_pool = await asyncpg.create_pool(
                    host=self.cfg.replica_host,
                    port=self.cfg.replica_port or self.cfg.port,
                    user=self.cfg.user,
                    password=self.cfg.password,
                    database=self.cfg.database,
                    min_size=self.cfg.min_pool_size,
                    max_size=self.cfg.max_pool_size,
                )
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise DatabaseConnectionError(str(e)) from e
        await self.circuit_breaker.record_success()

    async def _ensure_pool(self) -> None:
        if not self.pool:
            await self.connect()
        if self.cfg.replica_host and not self.replica_pool:
            await self.connect()

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        cache_key = query + str(params)
        cached = self.query_cache.get(cache_key)
        if cached is not None:
            return cached
        await self._ensure_pool()
        assert self.pool
        try:
            pool = self.replica_pool or self.pool
            assert pool
            start = time.perf_counter()
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            await self.circuit_breaker.record_success()
            result = [dict(row) for row in rows]
            self.query_cache.set(cache_key, result)
            return result
        except asyncpg.PostgresError:
            await self.circuit_breaker.record_failure()
            await self.connect()
            pool = self.replica_pool or self.pool
            assert pool
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            await self.circuit_breaker.record_success()
            result = [dict(row) for row in rows]
            self.query_cache.set(cache_key, result)
            return result
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e

    async def execute(self, query: str, *params: Any) -> int:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        await self._ensure_pool()
        assert self.pool
        try:
            start = time.perf_counter()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            await self.circuit_breaker.record_success()
            return int(result.split(" ")[-1])
        except asyncpg.PostgresError:
            await self.circuit_breaker.record_failure()
            await self.connect()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            await self.circuit_breaker.record_success()
            return int(result.split(" ")[-1])
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e

    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
    ) -> int:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        await self._ensure_pool()
        assert self.pool
        params_list = list(params_seq)
        if concurrency > 1 and len(params_list) > 0:
            chunk_size = len(params_list) // concurrency + 1
            chunks = [
                params_list[i : i + chunk_size]
                for i in range(0, len(params_list), chunk_size)
            ]

            async def run_chunk(chunk: list[Iterable[Any]]) -> int:
                async with self.pool.acquire() as conn:
                    await conn.executemany(query, chunk)
                return len(chunk)

            results = await asyncio.gather(*[run_chunk(c) for c in chunks])
            return sum(results)

        try:
            start = time.perf_counter()
            async with self.pool.acquire() as conn:
                await conn.executemany(query, params_list)
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            await self.circuit_breaker.record_success()
        except asyncpg.PostgresError:
            await self.circuit_breaker.record_failure()
            await self.connect()
            async with self.pool.acquire() as conn:
                await conn.executemany(query, params_list)
            await self.circuit_breaker.record_success()
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        return len(params_list)

    async def health_check(self) -> bool:
        if not await self.circuit_breaker.allow():
            return False
        try:
            await self.fetch("SELECT 1")
            await self.circuit_breaker.record_success()
            return True
        except DatabaseError:
            await self.circuit_breaker.record_failure()
            return False

    def report_pool_status(self) -> None:
        if not self.pool:
            return
        try:
            in_use = self.pool._working_conn_count  # type: ignore[attr-defined]
            max_size = self.pool._maxsize  # type: ignore[attr-defined]
            metrics.set("postgres_pool_in_use", float(in_use))
            metrics.set("postgres_pool_max", float(max_size))
        except Exception:
            pass

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
        if self.replica_pool:
            await self.replica_pool.close()
