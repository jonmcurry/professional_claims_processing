import pytest

from src.utils.cache import OptimizedRvuCache


class DummyDB:
    def __init__(self):
        self.calls = []

    async def fetch(self, query: str, *params):
        codes = list(params[0]) if params else []
        self.calls.append(codes)
        return [{"procedure_code": c, "total_rvu": 1.0} for c in codes]


class DummyPipe:
    def __init__(self, store):
        self.store = store
        self.keys = []

    def get(self, key):
        self.keys.append(key)

    async def execute(self):
        return [self.store.get(k) for k in self.keys]


class DummyRedis:
    def __init__(self):
        self.store = {}

    def pipeline(self):
        return DummyPipe(self.store)

    async def set(self, key, value, ex=None):
        self.store[key] = value

    async def delete(self, key):
        self.store.pop(key, None)


@pytest.mark.asyncio
async def test_get_many_uses_caches():
    db = DummyDB()
    redis = DummyRedis()
    cache = OptimizedRvuCache("redis://", db, redis_client=redis)
    res = await cache.get_many(["A", "B"])
    assert res["A"]["total_rvu"] == 1.0
    assert res["B"]["total_rvu"] == 1.0
    assert db.calls == [["A", "B"]]

    # second call should not hit the db
    res = await cache.get_many(["A", "B"])
    assert db.calls == [["A", "B"]]
    assert res["A"]["total_rvu"] == 1.0
    assert res["B"]["total_rvu"] == 1.0
