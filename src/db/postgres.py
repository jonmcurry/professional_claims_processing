import asyncpg
from typing import Iterable, Any

from .base import BaseDatabase
from ..config.config import PostgresConfig


class PostgresDatabase(BaseDatabase):
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=self.cfg.database,
            min_size=5,
            max_size=20,
        )

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        assert self.pool
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    async def execute(self, query: str, *params: Any) -> int:
        assert self.pool
        async with self.pool.acquire() as conn:
            result = await conn.execute(query, *params)
            return int(result.split(" ")[-1])
