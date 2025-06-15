import asyncio
import sys
import types

sys.modules.setdefault("asyncpg", types.ModuleType("asyncpg"))

from src.processing.sharded import ShardedClaimsProcessor


class DummyPipeline:
    def __init__(self, cfg):
        self.batches = []

    async def process_claims_batch(self, claims):
        self.batches.append(list(claims))


def test_process_claims_sharded(monkeypatch):
    # Replace ClaimsPipeline with DummyPipeline
    monkeypatch.setattr(
        "src.processing.sharded.ClaimsPipeline", lambda cfg: DummyPipeline(cfg)
    )
    configs = {0: object(), 1: object()}
    proc = ShardedClaimsProcessor(configs)

    # Map facilities deterministically
    def shard_for_facility(self, facility_id):
        return 0 if facility_id == "A" else 1

    monkeypatch.setattr(
        ShardedClaimsProcessor, "get_shard_id", shard_for_facility, raising=False
    )

    claims = [
        {"facility_id": "A", "claim_id": "1"},
        {"facility_id": "B", "claim_id": "2"},
        {"facility_id": "A", "claim_id": "3"},
    ]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(proc.process_claims_sharded(claims))
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    assert proc.shards[0].batches == [
        [{"facility_id": "A", "claim_id": "1"}, {"facility_id": "A", "claim_id": "3"}]
    ]
    assert proc.shards[1].batches == [[{"facility_id": "B", "claim_id": "2"}]]
