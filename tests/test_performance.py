import time
import asyncio
import sys
import types

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

    async def fetch(self, query: str, *params):
        return [{
            "claim_id": "1",
            "patient_account_number": "111",
            "facility_id": "F1",
            "procedure_code": "P1",
            "financial_class": "A"
        }]

    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        return len(list(params_seq))

class DummySQL:
    def __init__(self):
        self.inserted = []

    async def connect(self):
        pass

    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        items = list(params_seq)
        self.inserted.extend(items)
        return len(items)

    async def prepare(self, query: str):
        pass

class DummyModel:
    def predict(self, claim):
        return 1

def test_pipeline_batch_performance():
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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        start = time.perf_counter()
        loop.run_until_complete(pipeline.process_batch())
        duration = time.perf_counter() - start
    finally:
        loop.close()
        asyncio.set_event_loop(asyncio.new_event_loop())
    assert duration < 0.5

