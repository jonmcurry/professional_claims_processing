import pytest

from src.utils.cache import RvuCache


class DummyDB:
    async def fetch(self, query: str, *params):
        return [{"procedure_code": p, "total_rvu": 1.0} for p in params]


@pytest.mark.asyncio
async def test_warm_cache():
    cache = RvuCache(DummyDB())
    await cache.warm_cache(["A"])
    data = await cache.get("A")
    assert data["total_rvu"] == 1.0


@pytest.mark.asyncio
async def test_get_many():
    cache = RvuCache(DummyDB())
    res = await cache.get_many(["A", "B"])
    assert res["A"]["total_rvu"] == 1.0
    assert res["B"]["total_rvu"] == 1.0
