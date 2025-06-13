import pytest
from src.utils.cache import RvuCache

class DummyDB:
    async def fetch(self, query: str, *params):
        if params[0] == "A":
            return [{"procedure_code": "A", "total_rvu": 1.0}]
        return []

@pytest.mark.asyncio
async def test_warm_cache():
    cache = RvuCache(DummyDB())
    await cache.warm_cache(["A"])
    data = await cache.get("A")
    assert data["total_rvu"] == 1.0
