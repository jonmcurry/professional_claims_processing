import asyncio
from typing import List, Dict, Any

from ..config.config import AppConfig
from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..models.filter_model import FilterModel
from ..rules.engine import RulesEngine, Rule
from ..validation.validator import ClaimValidator
from ..utils.cache import InMemoryCache
from ..utils.logging import setup_logging, RequestContextFilter


class ClaimsPipeline:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.logger = setup_logging()
        self.pg = PostgresDatabase(cfg.postgres)
        self.sql = SQLServerDatabase(cfg.sqlserver)
        self.model: FilterModel | None = None
        self.rules_engine: RulesEngine | None = None
        self.validator: ClaimValidator | None = None
        self.cache = InMemoryCache()

    async def startup(self) -> None:
        await asyncio.gather(self.pg.connect(), self.sql.connect())
        # Connection pool warming
        self.model = FilterModel("model.joblib")
        self.rules_engine = RulesEngine([])
        self.validator = ClaimValidator(set(), set())

    async def process_batch(self) -> None:
        request_filter = RequestContextFilter()
        self.logger.addFilter(request_filter)

        claims = await self.pg.fetch("SELECT * FROM claims LIMIT $1", self.cfg.processing.batch_size)
        tasks = [self.process_claim(claim) for claim in claims]
        await asyncio.gather(*tasks)

    async def process_claim(self, claim: Dict[str, Any]) -> None:
        assert self.rules_engine and self.validator and self.model
        validation_errors = self.validator.validate(claim)
        rule_errors = self.rules_engine.evaluate(claim)
        if validation_errors or rule_errors:
            self.logger.error(f"Claim {claim['claim_id']} failed validation", extra={"request_id": claim.get("correlation_id")})
            return
        prediction = self.model.predict(claim)
        claim["filter_number"] = prediction
        # Insert into SQL Server
        await self.sql.execute("INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)", claim["patient_account_number"], claim["facility_id"])
        self.logger.info(f"Processed claim {claim['claim_id']}", extra={"request_id": claim.get("correlation_id")})
