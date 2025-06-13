import asyncpg
from typing import Iterable, Any

import time

from ..utils.errors import DatabaseConnectionError, QueryError

from .base import BaseDatabase
from ..config.config import PostgresConfig
from ..monitoring.metrics import metrics


class PostgresDatabase(BaseDatabase):
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None
        self.replica_pool: asyncpg.pool.Pool | None = None

    async def connect(self) -> None:
        try:
            self.pool = await asyncpg.create_pool(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                min_size=5,
                max_size=20,
            )
            if self.cfg.replica_host:
                self.replica_pool = await asyncpg.create_pool(
                    host=self.cfg.replica_host,
                    port=self.cfg.replica_port or self.cfg.port,
                    user=self.cfg.user,
                    password=self.cfg.password,
                    database=self.cfg.database,
                    min_size=5,
                    max_size=20,
                )
        except Exception as e:
            raise DatabaseConnectionError(str(e)) from e

    async def _ensure_pool(self) -> None:
        if not self.pool:
            await self.connect()
        if self.cfg.replica_host and not self.replica_pool:
            await self.connect()

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
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
            return [dict(row) for row in rows]
        except asyncpg.PostgresError:
            await self.connect()
            pool = self.replica_pool or self.pool
            assert pool
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]
        except Exception as e:
            raise QueryError(str(e)) from e

    async def execute(self, query: str, *params: Any) -> int:
        await self._ensure_pool()
        assert self.pool
        try:
            start = time.perf_counter()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            return int(result.split(" ")[-1])
        except asyncpg.PostgresError:
            await self.connect()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            return int(result.split(" ")[-1])
        except Exception as e:
            raise QueryError(str(e)) from e

    async def execute_many(
        self, query: str, params_seq: Iterable[Iterable[Any]]
    ) -> int:
        await self._ensure_pool()
        assert self.pool
        params_list = list(params_seq)
        try:
            start = time.perf_counter()
            async with self.pool.acquire() as conn:
                await conn.executemany(query, params_list)
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
        except asyncpg.PostgresError:
            await self.connect()
            async with self.pool.acquire() as conn:
                await conn.executemany(query, params_list)
        except Exception as e:
            raise QueryError(str(e)) from e
        return len(params_list)

    async def health_check(self) -> bool:
        try:
            await self.fetch("SELECT 1")
            return True
        except DatabaseError:
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
