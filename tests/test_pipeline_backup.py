import asyncio
import json
import sys
import types

import pytest

sys.modules.setdefault("asyncpg", types.ModuleType("asyncpg"))
sys.modules.setdefault("joblib", types.ModuleType("joblib"))

from src.config.config import (AppConfig, CacheConfig, ModelConfig,
                               PostgresConfig, ProcessingConfig,
                               SecurityConfig, SQLServerConfig)
from src.processing.pipeline import ClaimsPipeline
from src.rules.engine import RulesEngine
from src.services.claim_service import ClaimService


class DummyDB:
    def __init__(self):
        self.ok = False

    async def connect(self):
        pass

    async def health_check(self):
        return self.ok


class DummyService:
    def __init__(self):
        self.inserted = []

    async def insert_claims(self, rows, concurrency=1):
        self.inserted.extend(list(rows))
        return len(self.inserted)


class DummyModel:
    def __init__(self):
        self.calls = 0

    def predict(self, claim):
        self.calls += 1
        return 1


class DummyRedis:
    async def health_check(self):
        return True


@pytest.mark.asyncio
async def test_backup_mode_and_recovery(tmp_path, monkeypatch):
    cfg = AppConfig(
        postgres=PostgresConfig("", 0, "", "", ""),
        sqlserver=SQLServerConfig("", 0, "", "", ""),
        processing=ProcessingConfig(batch_size=1),
        security=SecurityConfig(api_key="k"),
        cache=CacheConfig(),
        model=ModelConfig(path="m"),
    )
    pipeline = ClaimsPipeline(cfg)
    pipeline.pg = DummyDB()
    pipeline.sql = DummyDB()
    pipeline.distributed_cache = DummyRedis()
    pipeline.model = DummyModel()
    pipeline.rules_engine = RulesEngine([])
    pipeline.validator = None
    pipeline.service = DummyService()
    pipeline.local_queue_path = tmp_path / "queue.jsonl"

    await pipeline.process_claim({"claim_id": "1"})
    assert pipeline.mode == "backup"
    assert pipeline.local_queue_path.exists()
    with open(pipeline.local_queue_path) as f:
        data = json.loads(f.readline())
    assert data["claim_id"] == "1"
    assert pipeline.model.calls == 0

    pipeline.pg.ok = True
    pipeline.sql.ok = True
    await pipeline._check_services_health()
    assert pipeline.mode == "normal"

    await pipeline.process_claim(
        {"claim_id": "2", "patient_account_number": "2", "facility_id": "F"}
    )
    assert pipeline.model.calls == 1
    assert pipeline.service.inserted
