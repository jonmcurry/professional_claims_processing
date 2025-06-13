import pytest
from fastapi.testclient import TestClient
from src.web.app import create_app
from src.web.status import processing_status

class DummyDB:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return [{"claim_id": "1", "failure_reason": "bad data", "failed_at": "now"}]

    async def execute(self, query: str, *params):
        return 1

@pytest.fixture
def client():
    app = create_app(sql_db=DummyDB())
    with TestClient(app) as client:
        yield client


def test_failed_claims_endpoint(client):
    resp = client.get("/api/failed_claims")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["claim_id"] == "1"


def test_status_endpoint(client):
    processing_status["processed"] = 5
    processing_status["failed"] = 2
    resp = client.get("/status")
    assert resp.status_code == 200
    assert resp.json() == {"processed": 5, "failed": 2}
