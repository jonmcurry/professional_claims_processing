from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Dict, Iterable

from ..config.config import AppConfig
from .pipeline import ClaimsPipeline


class ShardedClaimsProcessor:
    """Manage multiple ClaimsPipeline instances for sharded processing."""

    def __init__(self, shard_configs: Dict[int, AppConfig]) -> None:
        self.shards: Dict[int, ClaimsPipeline] = {}
        for shard_id, cfg in shard_configs.items():
            self.shards[shard_id] = ClaimsPipeline(cfg)

    def get_shard_id(self, facility_id: str | None) -> int:
        """Return the shard id for a given facility."""
        if facility_id is None:
            return 0
        return hash(facility_id) % len(self.shards)

    async def process_claims_sharded(self, claims: Iterable[Dict[str, Any]]) -> None:
        """Process claims by routing them to their facility shard."""
        facility_groups: Dict[int, list[Dict[str, Any]]] = defaultdict(list)
        for claim in claims:
            facility_id = claim.get("facility_id")
            shard_id = self.get_shard_id(facility_id)
            facility_groups[shard_id].append(claim)

        tasks = [
            self.shards[shard_id].process_claims_batch(shard_claims)
            for shard_id, shard_claims in facility_groups.items()
        ]
        if tasks:
            await asyncio.gather(*tasks)
