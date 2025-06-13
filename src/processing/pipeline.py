import asyncio
import json
from typing import List, Dict, Any

from ..config.config import AppConfig
from ..security.compliance import encrypt_text
from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..models.filter_model import FilterModel
from ..rules.engine import Rule
from ..rules.durable_engine import DurableRulesEngine
from ..validation.validator import ClaimValidator
from ..utils.cache import InMemoryCache, RvuCache
from ..utils.logging import setup_logging, RequestContextFilter
from .repair import ClaimRepairSuggester
from ..web.status import processing_status


class ClaimsPipeline:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.logger = setup_logging()
        self.pg = PostgresDatabase(cfg.postgres)
        self.sql = SQLServerDatabase(cfg.sqlserver)
        self.encryption_key = cfg.security.encryption_key
        self.model: FilterModel | None = None
        self.rules_engine: DurableRulesEngine | None = None
        self.validator: ClaimValidator | None = None
        self.cache = InMemoryCache()
        self.rvu_cache: RvuCache | None = None
        self.repair_suggester = ClaimRepairSuggester()

    async def startup(self) -> None:
        await asyncio.gather(self.pg.connect(), self.sql.connect())
        # Connection pool warming
        await asyncio.gather(
            self.pg.fetch("SELECT 1"),
            self.sql.execute("SELECT 1")
        )
        # Prepare frequently used statements
        await self.sql.prepare(
            "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)"
        )
        await self.sql.prepare(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, processing_stage, failed_at, original_data, repair_suggestions) VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)"
        )
        self.model = FilterModel("model.joblib")
        self.rules_engine = DurableRulesEngine([])
        self.validator = ClaimValidator(set(), set())
        self.rvu_cache = RvuCache(self.pg)

    async def process_batch(self) -> None:
        request_filter = RequestContextFilter()
        self.logger.addFilter(request_filter)

        processing_status["processed"] = 0
        processing_status["failed"] = 0
        claims = await self.pg.fetch(
            "SELECT * FROM claims LIMIT $1", self.cfg.processing.batch_size
        )
        tasks = [self.process_claim(claim) for claim in claims]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r]
        if valid:
            await self.sql.execute_many(
                "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)",
                valid,
            )
            processing_status["processed"] += len(valid)

    async def process_claim(self, claim: Dict[str, Any]) -> tuple[str, str] | None:
        assert self.rules_engine and self.validator and self.model
        validation_errors = self.validator.validate(claim)
        rule_errors = self.rules_engine.evaluate(claim)
        if validation_errors or rule_errors:
            processing_status["failed"] += 1
            suggestions = self.repair_suggester.suggest(validation_errors + rule_errors)
            await self.record_failed_claim(claim, "validation", suggestions)
            self.logger.error(
                f"Claim {claim['claim_id']} failed validation",
                extra={"request_id": claim.get("correlation_id")},
            )
            return None
        prediction = self.model.predict(claim)
        claim["filter_number"] = prediction
        if self.rvu_cache:
            rvu = await self.rvu_cache.get(claim.get("procedure_code", ""))
            if rvu:
                claim["rvu_value"] = rvu.get("total_rvu")
        self.logger.info(
            f"Processed claim {claim['claim_id']}",
            extra={"request_id": claim.get("correlation_id")},
        )
        return (claim["patient_account_number"], claim["facility_id"])

    async def record_failed_claim(self, claim: Dict[str, Any], reason: str, suggestions: str) -> None:
        await self.sql.execute(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, processing_stage, failed_at, original_data, repair_suggestions)"
            " VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)",
            claim.get("claim_id"),
            claim.get("facility_id"),
            claim.get("patient_account_number"),
            reason,
            "validation",
            encrypt_text(json.dumps(claim), self.encryption_key) if self.encryption_key else json.dumps(claim),
            suggestions,
        )

