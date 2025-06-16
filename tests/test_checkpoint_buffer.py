import asyncio

import pytest

from src.services.claim_service import ClaimService


class DummyPG:
    def __init__(self):
        self.recorded = []

    async def execute_many(self, query, params_seq, *, concurrency=1):
        self.recorded.extend(list(params_seq))
        return len(self.recorded)

    async def execute(self, query, *params):
        self.recorded.append(params)
        return 1


class DummySQL:
    pass


@pytest.mark.asyncio
async def test_checkpoint_buffer_flushes():
    pg = DummyPG()
    service = ClaimService(pg, DummySQL(), checkpoint_buffer_size=5)
    for i in range(7):
        await service.record_checkpoint(str(i), "start")
    await service.flush_checkpoints()
    assert len(pg.recorded) == 7
