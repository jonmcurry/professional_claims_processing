import asyncio
import time
import os
from typing import Dict, Any

from ..config.config import AppConfig
from ..security.compliance import mask_claim_data
from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..models.filter_model import FilterModel
from ..rules.durable_engine import DurableRulesEngine
from ..validation.validator import ClaimValidator
from ..utils.cache import DistributedCache, InMemoryCache, RvuCache
from ..utils.retries import retry_async
from ..utils.logging import setup_logging, RequestContextFilter
from ..utils.tracing import start_trace, start_span
from ..utils.errors import ErrorCategory, categorize_exception
from ..utils.audit import record_audit_event
from .repair import ClaimRepairSuggester
from ..services.claim_service import ClaimService
from ..web.status import processing_status
from ..monitoring.metrics import metrics


class ClaimsPipeline:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.logger = setup_logging(cfg.logging)
        self.request_filter: RequestContextFilter | None = None
        self.pg = PostgresDatabase(cfg.postgres)
        self.sql = SQLServerDatabase(cfg.sqlserver)
        self.encryption_key = cfg.security.encryption_key
        self.model: FilterModel | None = None
        self.rules_engine: DurableRulesEngine | None = None
        self.validator: ClaimValidator | None = None
        self.cache = InMemoryCache()
        self.rvu_cache: RvuCache | None = None
        self.distributed_cache: DistributedCache | None = None
        self.repair_suggester = ClaimRepairSuggester()
        self.service = ClaimService(self.pg, self.sql, self.encryption_key)

    async def startup(self) -> None:
        await asyncio.gather(self.pg.connect(), self.sql.connect())
        # Connection pool warming
        await asyncio.gather(self.pg.fetch("SELECT 1"), self.sql.execute("SELECT 1"))
        # Prepare frequently used statements
        await self.sql.prepare(
            "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)"
        )
        await self.sql.prepare(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, processing_stage, failed_at, original_data, repair_suggestions) VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)"
        )
        self.model = FilterModel(self.cfg.model.path)
        self.rules_engine = DurableRulesEngine([])
        self.validator = ClaimValidator(set(), set())
        if self.cfg.cache.redis_url:
            self.distributed_cache = DistributedCache(self.cfg.cache.redis_url)
        self.rvu_cache = RvuCache(self.pg, distributed=self.distributed_cache)
        if self.cfg.cache.warm_rvu_codes:
            await self.rvu_cache.warm_cache(self.cfg.cache.warm_rvu_codes)

    def calculate_batch_size(self) -> int:
        """Adjust batch size based on system load."""
        base = self.cfg.processing.batch_size
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0
        if load > 4:
            return max(1, base // 2)
        if load < 1:
            return base * 2
        return base

    async def _checkpoint(self, claim_id: str, stage: str) -> None:
        """Record a processing checkpoint."""
        try:
            await self.service.record_checkpoint(claim_id, stage)
        except Exception:
            self.logger.debug(
                "checkpoint failed", extra={"claim_id": claim_id, "stage": stage}
            )

    async def process_batch(self) -> None:
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        start_trace()
        start_time = time.perf_counter()

        await self.service.reprocess_dead_letter()

        processing_status["processed"] = 0
        processing_status["failed"] = 0
        metrics.set("claims_processed", 0)
        metrics.set("claims_failed", 0)
        batch_size = self.calculate_batch_size()
        claims = await self.service.fetch_claims(batch_size, priority=True)
        tasks = [self.process_claim(claim) for claim in claims]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r]
        if valid:
            try:
                await self.service.insert_claims(valid)
                processing_status["processed"] += len(valid)
                metrics.inc("claims_processed", len(valid))
            except Exception as exc:
                category = categorize_exception(exc)
                for acct, facility in valid:
                    await self.service.enqueue_dead_letter(
                        {"patient_account_number": acct, "facility_id": facility},
                        str(exc),
                    )
                self.logger.error(
                    "Batch insert failed",
                    exc_info=exc,
                    extra={"category": category.value},
                )
                metrics.inc("claims_failed", len(valid))

        duration = time.perf_counter() - start_time
        self.logger.info(
            "Batch complete",
            extra={
                "event": "batch_complete",
                "processed": processing_status["processed"],
                "failed": processing_status["failed"],
                "duration_sec": round(duration, 4),
            },
        )

    async def process_stream(self) -> None:
        """Continuously process claims from the source stream."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        start_trace()
        async for claim in self.service.stream_claims(
            self.calculate_batch_size(), priority=True
        ):
            await self.process_claim(claim)

    @retry_async()
    async def process_claim(self, claim: Dict[str, Any]) -> tuple[str, str] | None:
        assert self.rules_engine and self.validator and self.model
        with start_span():
            await self._checkpoint(claim.get("claim_id", ""), "start")
            validation_errors = self.validator.validate(claim)
            rule_errors = self.rules_engine.evaluate(claim)
            await self._checkpoint(claim.get("claim_id", ""), "validated")
            if validation_errors or rule_errors:
                processing_status["failed"] += 1
                metrics.inc("claims_failed")
                suggestions = self.repair_suggester.suggest(
                    validation_errors + rule_errors
                )
                await self.service.record_failed_claim(
                    claim,
                    "validation",
                    suggestions,
                    category=ErrorCategory.VALIDATION.value,
                )
                await self._checkpoint(claim.get("claim_id", ""), "failed")
                self.logger.error(
                    f"Claim {claim['claim_id']} failed validation",
                    extra={
                        "event": "claim_failed",
                        "claim_id": claim.get("claim_id"),
                        "reason": "validation",
                        "claim": mask_claim_data(claim),
                    },
                )
                return None
            prediction = self.model.predict(claim)
            await self._checkpoint(claim.get("claim_id", ""), "predicted")
            claim["filter_number"] = prediction
            if self.rvu_cache:
                rvu = await self.rvu_cache.get(claim.get("procedure_code", ""))
                if rvu:
                    claim["rvu_value"] = rvu.get("total_rvu")
            self.logger.info(
                f"Processed claim {claim['claim_id']}",
                extra={
                    "event": "claim_processed",
                    "claim_id": claim.get("claim_id"),
                    "prediction": prediction,
                    "claim": mask_claim_data(claim),
                },
            )
            await record_audit_event(
                self.sql,
                "claims",
                claim["claim_id"],
                "insert",
                new_values=claim,
            )
            await self._checkpoint(claim.get("claim_id", ""), "completed")
            return (claim["patient_account_number"], claim["facility_id"])
