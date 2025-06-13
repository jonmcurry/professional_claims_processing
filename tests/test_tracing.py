import pytest
from fastapi.testclient import TestClient
from src.web.app import create_app

class DummyDB:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return []

    async def execute(self, query: str, *params):
        return 1

    async def health_check(self):
        return True

@pytest.fixture
def client():
    app = create_app(sql_db=DummyDB(), api_key="test")
    with TestClient(app) as client:
        yield client

def test_trace_id_header(client):
    resp = client.get("/health", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert "X-Trace-ID" in resp.headers
    assert resp.headers["X-Trace-ID"]
