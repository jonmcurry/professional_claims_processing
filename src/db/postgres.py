import asyncpg
from typing import Iterable, Any

from ..utils.errors import DatabaseConnectionError, QueryError

from .base import BaseDatabase
from ..config.config import PostgresConfig


class PostgresDatabase(BaseDatabase):
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None

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
        except Exception as e:
            raise DatabaseConnectionError(str(e)) from e

    async def _ensure_pool(self) -> None:
        if not self.pool:
            await self.connect()

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        await self._ensure_pool()
        assert self.pool
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except asyncpg.PostgresError:
            await self.connect()
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except Exception as e:
            raise QueryError(str(e)) from e

    async def execute(self, query: str, *params: Any) -> int:
        await self._ensure_pool()
        assert self.pool
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
                return int(result.split(" ")[-1])
        except asyncpg.PostgresError:
            await self.connect()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
                return int(result.split(" ")[-1])
        except Exception as e:
            raise QueryError(str(e)) from e

    async def execute_many(self, query: str, params_seq: Iterable[Iterable[Any]]) -> int:
        await self._ensure_pool()
        assert self.pool
        params_list = list(params_seq)
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(query, params_list)
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

