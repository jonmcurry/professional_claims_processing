import asyncio
import sys
import types

import pytest

# Minimal stubs used by existing integration tests
sys.modules.setdefault("asyncpg", types.ModuleType("asyncpg"))
sys.modules.setdefault("joblib", types.ModuleType("joblib"))


from src.analysis.error_pattern_detector import ErrorPatternDetector
from src.config.config import (AppConfig, CacheConfig, ModelConfig,
                               PostgresConfig, ProcessingConfig,
                               SecurityConfig, SQLServerConfig)
from src.processing.pipeline import ClaimsPipeline
from src.rules.engine import RulesEngine
from src.services.claim_service import ClaimService
from src.validation.validator import ClaimValidator


class DummyPostgres:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return [
            {
                "claim_id": "1",
                "patient_account_number": "111",
                "facility_id": "F1",
                "procedure_code": "P1",
                "financial_class": "A",
            }
        ]

    async def execute_many(self, query, params_seq, concurrency=1):
        return len(list(params_seq))


class DummySQL:
    def __init__(self):
        self.inserted = []

    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        return []

    async def execute_many(self, query, params_seq, concurrency=1):
        self.inserted.extend(list(params_seq))
        return len(self.inserted)

    async def bulk_insert_tvp(self, table, columns, rows):
        self.inserted.extend(list(rows))
        return len(self.inserted)


async def noop(*args, **kwargs):
    pass


class DummyModel:
    def predict(self, claim):
        return 1


class DummyRvuCache:
    async def warm_cache(self, codes):
        pass

    async def get_many(self, codes):
        return {c: {"total_rvu": 1} for c in codes}


@pytest.mark.asyncio
async def test_pipeline_triggers_error_detector(monkeypatch):
    cfg = AppConfig(
        postgres=PostgresConfig("", 0, "", "", ""),
        sqlserver=SQLServerConfig("", 0, "", "", ""),
        processing=ProcessingConfig(batch_size=1),
        security=SecurityConfig(api_key="k"),
        cache=CacheConfig(),
        model=ModelConfig(path="model.joblib"),
    )
    pipeline = ClaimsPipeline(cfg)
    pipeline.pg = DummyPostgres()
    pipeline.sql = DummySQL()
    pipeline.model = DummyModel()
    pipeline.validator = ClaimValidator({"F1"}, {"A"})
    pipeline.rules_engine = RulesEngine([])
    pipeline.service = ClaimService(pipeline.pg, pipeline.sql)
    pipeline.rvu_cache = DummyRvuCache()
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)

    called = False

    async def fake_check():
        nonlocal called
        called = True

    pipeline.error_detector = ErrorPatternDetector(
        pipeline.sql, threshold=1, check_interval=0
    )
    monkeypatch.setattr(pipeline.error_detector, "maybe_check", fake_check)

    await pipeline.process_batch()
    assert called
