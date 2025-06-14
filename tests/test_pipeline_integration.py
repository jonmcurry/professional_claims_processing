import pytest
import sys
import types
import asyncio

sys.modules.setdefault("asyncpg", types.ModuleType("asyncpg"))
sys.modules.setdefault("joblib", types.ModuleType("joblib"))
durable_module = types.ModuleType("durable")
durable_lang = types.ModuleType("lang")
durable_lang.ruleset = lambda name: (lambda func: func)
durable_lang.when_all = lambda cond: (lambda func: func)
durable_lang.m = object()
durable_lang.post = lambda name, data: None
setattr(durable_module, "lang", durable_lang)
sys.modules.setdefault("durable", durable_module)
sys.modules.setdefault("durable.lang", durable_lang)

from src.processing.pipeline import ClaimsPipeline
from src.config.config import (
    AppConfig,
    PostgresConfig,
    SQLServerConfig,
    ProcessingConfig,
    SecurityConfig,
    CacheConfig,
    ModelConfig,
)
from src.services.claim_service import ClaimService
from src.rules.engine import RulesEngine
from src.validation.validator import ClaimValidator
from src.web.status import processing_status


class DummyPostgres:
    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        if "rvu_data" in query and params:
            return [{"procedure_code": params[0], "total_rvu": 1}]
        if "rvu_data" in query:
            return [{"procedure_code": "X2"}]
        return [
            {
                "claim_id": "1",
                "patient_account_number": "111",
                "facility_id": "F1",
                "procedure_code": "P1",
                "financial_class": "A",
            }
        ]

    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        return len(list(params_seq))


class DummySQL:
    def __init__(self):
        self.inserted = []

    async def connect(self):
        pass

    async def fetch(self, query: str, *params):
        if "FROM facilities" in query:
            return [{"facility_id": "F1"}]
        if "FROM facility_financial_classes" in query:
            return [{"financial_class_id": "A"}]
        return []

    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        items = list(params_seq)
        self.inserted.extend(items)
        return len(items)

    async def bulk_insert_tvp(self, table, columns, rows):
        items = list(rows)
        self.inserted.extend(items)
        return len(items)

    async def prepare(self, query: str):
        pass


async def noop(*args, **kwargs):
    pass


class DummyModel:
    def predict(self, claim):
        return 1


class DummyRvuCache:
    def __init__(self):
        self.prefetched: list[set[str]] = []

    async def warm_cache(self, codes):
        self.prefetched.append(set(codes))

    async def get(self, code):
        return {"total_rvu": 1}

    async def get_many(self, codes):
        return {c: {"total_rvu": 1} for c in codes}


def test_pipeline_process_batch(monkeypatch):
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
    pipeline.rules_engine = RulesEngine([])
    pipeline.validator = ClaimValidator({"F1"}, {"A"})
    pipeline.service = ClaimService(pipeline.pg, pipeline.sql)
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    processing_status["processed"] = 0
    processing_status["failed"] = 0
    from src.monitoring.metrics import metrics
    metrics.reset("claims_processed")
    metrics.reset("claims_failed")
    metrics.reset("revenue_impact")
    metrics.reset("sla_violations")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(pipeline.process_batch())
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())
    assert processing_status["processed"] == 1
    assert pipeline.sql.inserted == [("111", "F1")]
    assert metrics.get("batch_processing_rate_per_sec") > 0
    assert metrics.get("claims_processed_per_hour") == 1
    assert metrics.get("revenue_impact") > 0
    assert metrics.get("revenue_impact_per_hour") == metrics.get("revenue_impact")
    assert metrics.get("sla_violations") == 0


def test_batch_prefetches_rvu(monkeypatch):
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
    pipeline.rules_engine = RulesEngine([])
    pipeline.validator = ClaimValidator({"F1"}, {"A"})
    pipeline.service = ClaimService(pipeline.pg, pipeline.sql)
    pipeline.rvu_cache = DummyRvuCache()
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(pipeline.process_batch())
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    assert {"P1"} in pipeline.rvu_cache.prefetched


def test_process_claims_batch(monkeypatch):
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
    pipeline.rules_engine = RulesEngine([])
    pipeline.validator = ClaimValidator({"F1"}, {"A"})
    pipeline.service = ClaimService(pipeline.pg, pipeline.sql)
    pipeline.rvu_cache = DummyRvuCache()
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    claims = [
        {
            "claim_id": "1",
            "patient_account_number": "111",
            "facility_id": "F1",
            "procedure_code": "P1",
            "financial_class": "A",
        }
    ]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        res = loop.run_until_complete(pipeline.process_claims_batch(claims))
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())
    assert res == [("111", "F1")]
    assert pipeline.sql.inserted == [("111", "F1")]


class DummyFilterModel:
    def __init__(self, path: str, version: str = "1"):
        self.path = path
        self.version = version

    def predict(self, claim):
        return 0


class DummyModelMonitor:
    def __init__(self, version: str):
        self.version = version

    def record_prediction(self, label: int) -> None:
        pass


@pytest.mark.asyncio
async def test_pipeline_startup(monkeypatch):
    cfg = AppConfig(
        postgres=PostgresConfig("", 0, "", "", ""),
        sqlserver=SQLServerConfig("", 0, "", "", ""),
        processing=ProcessingConfig(batch_size=1),
        security=SecurityConfig(api_key="k"),
        cache=CacheConfig(warm_rvu_codes=["X1"]),
        model=ModelConfig(path="model.joblib"),
    )
    pipeline = ClaimsPipeline(cfg)

    class DummyPG(DummyPostgres):
        async def fetch(self, query: str, *params):
            if "rvu_data" in query and params:
                return [{"procedure_code": params[0], "total_rvu": 1}]
            if "rvu_data" in query:
                return [{"procedure_code": "X2"}]
            return await super().fetch(query, *params)

    pipeline.pg = DummyPG()
    pipeline.sql = DummySQL()
    monkeypatch.setattr("src.models.filter_model.FilterModel", DummyFilterModel)
    monkeypatch.setattr("src.models.monitor.ModelMonitor", DummyModelMonitor)

    await pipeline.startup()
    assert pipeline.model
    assert pipeline.rvu_cache.get("X1") is not None
    assert pipeline.rvu_cache.get("X2") is not None
    assert "F1" in pipeline.validator.valid_facilities
    assert "A" in pipeline.validator.valid_financial_classes
