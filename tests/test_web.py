import asyncio
import logging

import pytest

from fastapi.testclient import TestClient
from src.config.config import create_default_config
from src.monitoring.metrics import metrics
from src.web.app import create_app
from src.web.status import batch_status, processing_status


class DummyDB:
    def __init__(self):
        self.executed = []

    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return [{"claim_id": "1", "failure_reason": "bad data", "failed_at": "now"}]

    async def execute(self, query: str, *params):
        self.executed.append((query, params))
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
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = create_app(
        sql_db=DummyDB(),
        pg_db=DummyPG(),
        redis_cache=DummyRedis(),
        cfg=create_default_config(),
        api_key="test",
        rate_limit_per_sec=1,
    )
    with TestClient(app) as client:
        yield client
    loop.close()
    asyncio.set_event_loop(asyncio.new_event_loop())


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
    data = resp.json()
    assert data["processing"] == {"processed": 5, "failed": 2}


def test_batch_status_endpoint(client):
    batch_status["batch_id"] = "1"
    batch_status["total"] = 10
    resp = client.get("/batch_status", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["batch_id"] == "1"


def test_health_endpoint(client):
    resp = client.get("/health", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"sqlserver": True, "redis": True}


def test_readiness_endpoint(client):
    resp = client.get("/readiness", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"postgres": True, "sqlserver": True, "redis": True}


def test_metrics_endpoint(client):
    metrics.set("claims_processed", 0)
    metrics.inc("claims_processed", 3)
    resp = client.get("/metrics", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["claims_processed"] == 3


def test_auditor_role_allowed_on_user_endpoints(client):
    resp = client.get(
        "/status", headers={"X-API-Key": "test", "X-User-Role": "auditor"}
    )
    assert resp.status_code == 200


def test_auditor_role_denied_on_admin_endpoint(client):
    resp = client.get(
        "/metrics", headers={"X-API-Key": "test", "X-User-Role": "auditor"}
    )
    assert resp.status_code == 403


def test_rate_limit(client):
    resp1 = client.get("/liveness")
    assert resp1.status_code == 200
    resp2 = client.get("/liveness")
    assert resp2.status_code == 429


def test_request_logging_middleware(client, caplog):
    with caplog.at_level(logging.INFO):
        resp = client.get("/liveness")
    assert resp.status_code == 200
    assert any(
        r.message == "request" and getattr(r, "path", None) == "/liveness"
        for r in caplog.records
    )
    assert any(
        r.message == "response" and getattr(r, "status", None) == 200
        for r in caplog.records
    )


def test_invalid_api_key_audited(client, monkeypatch):
    recorded = []

    async def fake_record(db, table, record_id, operation, **kwargs):
        recorded.append(
            {
                "table": table,
                "record_id": record_id,
                "operation": operation,
                "values": kwargs.get("new_values"),
            }
        )

    monkeypatch.setattr("src.utils.audit.record_audit_event", fake_record)
    monkeypatch.setattr("src.web.app.record_audit_event", fake_record)

    resp = client.get("/status", headers={"X-API-Key": "bad"})
    assert resp.status_code == 401
    assert recorded and recorded[0]["operation"] == "invalid_api_key"
    assert recorded[0]["values"]["path"] == "/status"


def test_forbidden_role_audited(client, monkeypatch):
    recorded = []

    async def fake_record(db, table, record_id, operation, **kwargs):
        recorded.append(
            {
                "table": table,
                "record_id": record_id,
                "operation": operation,
                "values": kwargs.get("new_values"),
            }
        )

    monkeypatch.setattr("src.utils.audit.record_audit_event", fake_record)
    monkeypatch.setattr("src.web.app.record_audit_event", fake_record)

    resp = client.get(
        "/metrics", headers={"X-API-Key": "test", "X-User-Role": "auditor"}
    )
    assert resp.status_code == 403
    assert recorded and recorded[0]["operation"] == "unauthorized"
    assert recorded[0]["values"]["role"] == "auditor"


USER_ENDPOINTS = [
    "/api/failed_claims",
    "/status",
    "/batch_status",
    "/health",
    "/readiness",
]

ADMIN_ENDPOINTS = [
    "/metrics",
    "/profiling/start",
    "/profiling/stop",
]


@pytest.mark.parametrize("path", USER_ENDPOINTS)
@pytest.mark.parametrize(
    "role,expected",
    [
        (None, 200),
        ("auditor", 200),
        ("user", 200),
        ("admin", 200),
        ("bad", 403),
    ],
)
def test_user_endpoint_permissions(client, path, role, expected):
    headers = {"X-API-Key": "test"}
    if role is not None:
        headers["X-User-Role"] = role
    resp = client.get(path, headers=headers)
    assert resp.status_code == expected


@pytest.mark.parametrize("path", ADMIN_ENDPOINTS)
@pytest.mark.parametrize(
    "role,expected",
    [
        (None, 200),
        ("admin", 200),
        ("user", 403),
        ("auditor", 403),
        ("bad", 403),
    ],
)
def test_admin_endpoint_permissions(client, path, role, expected):
    headers = {"X-API-Key": "test"}
    if role is not None:
        headers["X-User-Role"] = role
    resp = client.get(path, headers=headers)
    assert resp.status_code == expected


def test_assign_failed_claim(client):
    headers = {"X-API-Key": "test", "X-User-Role": "user"}
    resp = client.post(
        "/api/assign_failed_claim",
        json={"claim_id": "1", "user": "u"},
        headers=headers,
    )
    assert resp.status_code == 200


def test_resolve_failed_claim(client):
    headers = {"X-API-Key": "test", "X-User-Role": "user"}
    resp = client.post(
        "/api/resolve_failed_claim",
        json={"claim_id": "1", "action": "retry", "notes": "done"},
        headers=headers,
    )
    assert resp.status_code == 200
