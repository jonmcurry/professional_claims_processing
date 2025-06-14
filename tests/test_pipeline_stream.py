import asyncio
import sys
import types
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
from src.config.config import AppConfig, PostgresConfig, SQLServerConfig, ProcessingConfig, SecurityConfig, CacheConfig, ModelConfig
from src.services.claim_service import ClaimService
from src.rules.engine import RulesEngine
from src.validation.validator import ClaimValidator

class DummyPostgres:
    async def connect(self):
        pass

class DummySQL:
    def __init__(self):
        self.inserted = []

    async def connect(self):
        pass

    async def execute(self, query, *params):
        return 1

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
    def __init__(self):
        self.prefetched: list[set[str]] = []

    async def warm_cache(self, codes):
        self.prefetched.append(set(codes))

    async def get(self, code):
        return {"total_rvu": 1}

async def fetch_claims(batch_size, offset=0, priority=False):
    if offset == 0:
        return [
            {
                "claim_id": "1",
                "patient_account_number": "111",
                "facility_id": "F1",
                "procedure_code": "P1",
                "financial_class": "A",
            }
        ]
    if offset == batch_size:
        return [
            {
                "claim_id": "2",
                "patient_account_number": "222",
                "facility_id": "F1",
                "procedure_code": "P1",
                "financial_class": "A",
            }
        ]
    return []


def test_process_stream(monkeypatch):
    cfg = AppConfig(
        postgres=PostgresConfig("", 0, "", "", ""),
        sqlserver=SQLServerConfig("", 0, "", "", ""),
        processing=ProcessingConfig(batch_size=1, max_workers=1),
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
    pipeline.service.fetch_claims = fetch_claims
    pipeline.rvu_cache = DummyRvuCache()
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    monkeypatch.setattr("src.processing.pipeline.record_audit_event", noop)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(pipeline.process_stream())
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())

    assert pipeline.sql.inserted == [("111", "F1"), ("222", "F1")]
    assert pipeline.rvu_cache.prefetched == [{"P1"}, {"P1"}]
