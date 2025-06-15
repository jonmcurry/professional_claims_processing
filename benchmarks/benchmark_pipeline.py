import asyncio
import time

from src.config.config import (AppConfig, CacheConfig, ModelConfig,
                               PostgresConfig, ProcessingConfig,
                               SecurityConfig, SQLServerConfig)
from src.processing.pipeline import ClaimsPipeline
from src.rules.engine import RulesEngine
from src.services.claim_service import ClaimService
from src.validation.validator import ClaimValidator


class DummyPostgres:
    async def fetch(self, query: str, *params):
        return [
            {
                "claim_id": "1",
                "patient_account_number": "111",
                "facility_id": "F1",
                "procedure_code": "P1",
            }
        ]

    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        return len(list(params_seq))


class DummySQL:
    async def execute(self, query: str, *params):
        return 1

    async def execute_many(self, query: str, params_seq):
        return len(list(params_seq))


class DummyModel:
    def predict(self, claim):
        return 1


def create_pipeline():
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
    pipeline.validator = ClaimValidator({"F1"}, set())
    pipeline.service = ClaimService(pipeline.pg, pipeline.sql)
    return pipeline


async def run(iterations: int = 1000) -> float:
    pipeline = create_pipeline()
    claim = {
        "claim_id": "1",
        "patient_account_number": "111",
        "facility_id": "F1",
        "procedure_code": "P1",
    }
    start = time.perf_counter()
    for _ in range(iterations):
        await pipeline.process_claim(claim)
    return time.perf_counter() - start


if __name__ == "__main__":
    duration = asyncio.run(run())
    print(f"Processed 1000 claims in {duration:.4f}s")
