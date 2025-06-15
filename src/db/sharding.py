from __future__ import annotations

from typing import Any, Callable, Iterable, List

from .base import BaseDatabase


class ShardedDatabase(BaseDatabase):
    """Simple hash-based sharding wrapper."""

    def __init__(
        self, shards: List[BaseDatabase], shard_func: Callable[[Any], int]
    ) -> None:
        self.shards = shards
        self.shard_func = shard_func

    def _get_shard(self, key: Any) -> BaseDatabase:
        idx = self.shard_func(key) % len(self.shards)
        return self.shards[idx]

    async def fetch(self, query: str, shard_key: Any, *params: Any) -> Iterable[dict]:
        db = self._get_shard(shard_key)
        return await db.fetch(query, *params)

    async def execute(self, query: str, shard_key: Any, *params: Any) -> int:
        db = self._get_shard(shard_key)
        return await db.execute(query, *params)

    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        shard_key_index: int = 0,
    ) -> int:
        count = 0
        for params in params_seq:
            params_list = list(params)
            key = params_list[shard_key_index]
            count += await self.execute(query, key, *params_list)
        return count

    async def close(self) -> None:
        for db in self.shards:
            await db.close()
