import pytest
from fastapi.testclient import TestClient
from src.web.app import create_app
from src.web.status import processing_status
from src.monitoring.metrics import metrics

class DummyDB:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return [{"claim_id": "1", "failure_reason": "bad data", "failed_at": "now"}]

    async def execute(self, query: str, *params):
        return 1

    async def health_check(self):
        return True

class DummyPG(DummyDB):
    pass


@pytest.fixture
def client():
    app = create_app(sql_db=DummyDB(), pg_db=DummyPG(), api_key="test")
    with TestClient(app) as client:
        yield client


def test_failed_claims_endpoint(client):
    resp = client.get("/api/failed_claims", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["claim_id"] == "1"


def test_status_endpoint(client):
    processing_status["processed"] = 5
    processing_status["failed"] = 2
    resp = client.get("/status", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"processed": 5, "failed": 2}


def test_health_endpoint(client):
    resp = client.get("/health", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"sqlserver": True}


def test_readiness_endpoint(client):
    resp = client.get("/readiness", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"postgres": True, "sqlserver": True}


def test_metrics_endpoint(client):
    metrics.set("claims_processed", 0)
    metrics.inc("claims_processed", 3)
    resp = client.get("/metrics", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["claims_processed"] == 3
