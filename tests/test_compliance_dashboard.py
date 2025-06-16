import asyncio
import os

import pytest

from fastapi.testclient import TestClient
from src.config.config import create_default_config
from src.web.app import create_app


class DummyDB:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        if "audit_log" in query:
            return [{"count": 5}]
        if "daily_processing_summary" in query:
            return []
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
def dashboard_client(tmp_path):
    os.environ["ARCHIVE_PATH"] = str(tmp_path)
    (tmp_path / "claims_2024_01_01.jsonl").write_text("sample")
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
    os.environ.pop("ARCHIVE_PATH")


def test_dashboard_endpoint(dashboard_client):
    resp = dashboard_client.get("/compliance/dashboard", headers={"X-API-Key": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_audit_events"] == 5
    assert data["archive_files"] == 1
    assert "failure_patterns" in data
    assert "processing_trends" in data
    assert "revenue_impact" in data
