import pytest
from fastapi.testclient import TestClient
from src.web.app import create_app
from src.config.config import create_default_config

class DummyDB:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return []

    async def execute(self, query: str, *params):
        return 1

    async def health_check(self):
        return True


class DummyPG(DummyDB):
    pass


class DummyRedis:
    async def health_check(self):
        return True

@pytest.fixture
def client():
    app = create_app(
        sql_db=DummyDB(),
        pg_db=DummyPG(),
        redis_cache=DummyRedis(),
        cfg=create_default_config(),
        api_key="test",
    )
    with TestClient(app) as client:
        yield client

def test_trace_id_header(client):
    traceparent = "00-1234567890abcdef1234567890abcdef-abcdef1234567890-01"
    resp = client.get(
        "/health",
        headers={"X-API-Key": "test", "traceparent": traceparent},
    )
    assert resp.status_code == 200
    assert resp.headers["X-Trace-ID"] == "1234567890abcdef1234567890abcdef"
