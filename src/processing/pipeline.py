import asyncio
import contextlib
import os
import time
import contextlib
from typing import Any, Dict, Iterable
from typing import Any, Dict

from ..config.config import AppConfig
from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..models.ab_test import ABTestModel
from ..models.filter_model import FilterModel
from ..models.monitor import ModelMonitor
from ..monitoring import pool_monitor, resource_monitor
from ..monitoring.metrics import metrics
from ..rules.durable_engine import DurableRulesEngine
from ..security.compliance import mask_claim_data
from ..services.claim_service import ClaimService
from ..utils.audit import record_audit_event
from ..utils.cache import DistributedCache, InMemoryCache, RvuCache
from ..utils.errors import ErrorCategory, categorize_exception
from ..utils.logging import RequestContextFilter, setup_logging
from ..utils.retries import retry_async
from ..utils.tracing import start_span, start_trace
from ..validation.validator import ClaimValidator
from ..web.status import batch_status, processing_status
from .repair import ClaimRepairSuggester


class ClaimsPipeline:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.features = cfg.features
        self.logger = setup_logging(cfg.logging)
        self.request_filter: RequestContextFilter | None = None
        self.pg = PostgresDatabase(cfg.postgres)
        self.sql = SQLServerDatabase(cfg.sqlserver)
        self.encryption_key = cfg.security.encryption_key
        self.model: FilterModel | ABTestModel | None = None
        self.model_monitor: ModelMonitor | None = None
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
        await self.pg.prepare(
            "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)"
        )
        await self.pg.prepare(
            "INSERT INTO dead_letter_queue (claim_id, reason, data) VALUES ($1, $2, $3)"
        )
        if self.cfg.model.ab_test_path:
            model_a = FilterModel(self.cfg.model.path, self.cfg.model.version)
            model_b = FilterModel(
                self.cfg.model.ab_test_path, self.cfg.model.version + "b"
            )
            self.model = ABTestModel(model_a, model_b, self.cfg.model.ab_test_ratio)
        else:
            self.model = FilterModel(self.cfg.model.path, self.cfg.model.version)
        if self.features.enable_model_monitor:
            self.model_monitor = ModelMonitor(self.cfg.model.version)
        self.rules_engine = DurableRulesEngine([])
        self.validator = ClaimValidator(set(), set())
        if self.features.enable_cache and self.cfg.cache.redis_url:
            self.distributed_cache = DistributedCache(self.cfg.cache.redis_url)
        self.rvu_cache = RvuCache(
            self.pg,
            distributed=(
                self.distributed_cache if self.features.enable_cache else None
            ),
            predictive_ahead=self.cfg.cache.predictive_ahead,
        )
        if self.features.enable_cache and self.cfg.cache.warm_rvu_codes:
            await self.rvu_cache.warm_cache(self.cfg.cache.warm_rvu_codes)
        resource_monitor.start()
        pool_monitor.start(self.pg, self.sql)

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
                "checkpoint failed",
                extra={"claim_id": claim_id, "stage": stage},
            )

    async def _prefetch_rvu(self, claims: Iterable[Dict[str, Any]]) -> None:
        """Bulk prefetch RVU data for a collection of claims."""
        if not self.rvu_cache:
            return
        codes = {
            claim.get("procedure_code")
            for claim in claims
            if claim.get("procedure_code")
        }
        if codes:
            await self.rvu_cache.warm_cache(codes)

    async def process_batch(self) -> None:
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        start_trace()
        start_time = time.perf_counter()
        batch_status["batch_id"] = str(int(start_time * 1000))
        batch_status["start_time"] = start_time
        batch_status["end_time"] = None

        await self.service.reprocess_dead_letter()

        processing_status["processed"] = 0
        processing_status["failed"] = 0
        metrics.set("claims_processed", 0)
        metrics.set("claims_failed", 0)
        batch_size = self.calculate_batch_size()
        metrics.set("dynamic_batch_size", batch_size)
        claims = await self.service.fetch_claims(batch_size, priority=True)
        batch_status["total"] = len(claims)

        await self._prefetch_rvu(claims)
        tasks = [self.process_claim(claim) for claim in claims]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r]
        if valid:

        # Validate all claims in parallel
        validated = await asyncio.gather(
            *(self._validate_stage(claim) for claim in claims)
        )
        to_predict = [c for c in validated if c is not None]

        # Predict on valid claims in parallel
        predicted = await asyncio.gather(
            *(self._predict_stage(claim) for claim in to_predict)
        )

        if predicted:

            try:
                await self.service.insert_claims(
                    predicted, concurrency=self.cfg.processing.insert_workers
                )
                processing_status["processed"] += len(predicted)
                metrics.inc("claims_processed", len(predicted))
            except Exception as exc:
                category = categorize_exception(exc)
                for acct, facility in predicted:
                    await self.service.enqueue_dead_letter(
                        {
                            "patient_account_number": acct,
                            "facility_id": facility,
                        },
                        str(exc),
                    )
                self.logger.error(
                    "Batch insert failed",
                    exc_info=exc,
                    extra={"category": category.value},
                )
                metrics.inc("claims_failed", len(predicted))
                metrics.inc(f"errors_{category.value}", len(predicted))

        duration = time.perf_counter() - start_time
        if duration > 0:
            rate = processing_status["processed"] / duration
        else:
            rate = 0.0
        metrics.set("batch_processing_rate_per_sec", rate)
        metrics.set("last_batch_duration_ms", duration * 1000)
        batch_status["end_time"] = time.perf_counter()
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
        """Continuously process claims using a staged parallel pipeline."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        start_trace()
        batch_size = self.calculate_batch_size()

        # Queues provide backpressure between pipeline stages
        max_qsize = batch_size * 2
        fetch_to_validate: asyncio.Queue[Dict[str, Any] | None] = asyncio.Queue(
            maxsize=max_qsize
        )
        validate_to_predict: asyncio.Queue[Dict[str, Any] | None] = asyncio.Queue(
            maxsize=max_qsize
        )
        predict_to_insert: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue(
            maxsize=max_qsize
        )

        validate_workers = self.cfg.processing.max_workers
        predict_workers = self.cfg.processing.max_workers
        insert_workers = self.cfg.processing.insert_workers

        async def fetcher() -> None:
            offset = 0
            while True:
                batch = await self.service.fetch_claims(batch_size, offset=offset, priority=True)
                if not batch:
                    break
                await self._prefetch_rvu(batch)
                for claim in batch:
                    await fetch_to_validate.put(claim)
                offset += batch_size
            for _ in range(validate_workers):
                await fetch_to_validate.put(None)  # type: ignore

        async def validate_worker() -> None:
            while True:
                claim = await fetch_to_validate.get()
                if claim is None:
                    fetch_to_validate.task_done()
                    await validate_to_predict.put(None)
                    break
                res = await self._validate_stage(claim)
                fetch_to_validate.task_done()
                if res is not None:
                    await validate_to_predict.put(res)

        predict_done = 0

        async def predict_worker() -> None:
            nonlocal predict_done
            while True:
                claim = await validate_to_predict.get()
                if claim is None:
                    validate_to_predict.task_done()
                    predict_done += 1
                    if predict_done == predict_workers:
                        for _ in range(insert_workers):
                            await predict_to_insert.put(None)
                    break
                res = await self._predict_stage(claim)
                validate_to_predict.task_done()
                await predict_to_insert.put(res)

        async def insert_worker() -> None:
            buffer: list[tuple[str, str]] = []
            while True:
                item = await predict_to_insert.get()
                if item is None:
                    predict_to_insert.task_done()
                    if buffer:
                        await self.service.insert_claims(
                            buffer, concurrency=self.cfg.processing.insert_workers
                        )
                        processing_status["processed"] += len(buffer)
                        metrics.inc("claims_processed", len(buffer))
                    break
                buffer.append(item)
                predict_to_insert.task_done()
                if len(buffer) >= batch_size:
                    await self.service.insert_claims(
                        buffer, concurrency=self.cfg.processing.insert_workers
                    )
                    processing_status["processed"] += len(buffer)
                    metrics.inc("claims_processed", len(buffer))
                    buffer = []

        tasks = [asyncio.create_task(fetcher())]
        tasks += [
            asyncio.create_task(validate_worker()) for _ in range(validate_workers)
        ]
        tasks += [asyncio.create_task(predict_worker()) for _ in range(predict_workers)]
        tasks += [asyncio.create_task(insert_worker()) for _ in range(insert_workers)]

        await asyncio.gather(*tasks)
        await fetch_to_validate.join()
        await validate_to_predict.join()
        await predict_to_insert.join()

    @retry_async()
    async def process_claim(self, claim: Dict[str, Any]) -> tuple[str, str] | None:
        assert self.rules_engine and self.validator and self.model
        with start_span():
            await self._checkpoint(claim.get("claim_id", ""), "start")
            validation_errors = await self.validator.validate(claim)
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
            if self.model_monitor:
                self.model_monitor.record_prediction(prediction)
            await self._checkpoint(claim.get("claim_id", ""), "predicted")
            claim["filter_number"] = prediction
            if self.rvu_cache:
                rvu = await self.rvu_cache.get(claim.get("procedure_code", ""))
                if rvu:
                    claim["rvu_value"] = rvu.get("total_rvu")
                    units = float(claim.get("units", 1) or 1)
                    conv = float(self.cfg.processing.conversion_factor)
                    try:
                        rvu_total = float(rvu.get("total_rvu", 0))
                        claim["reimbursement_amount"] = rvu_total * units * conv
                    except Exception:
                        claim["reimbursement_amount"] = None
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

    async def _validate_stage(self, claim: Dict[str, Any]) -> Dict[str, Any] | None:
        """Run validation and rules stage."""
        assert self.validator and self.rules_engine
        await self._checkpoint(claim.get("claim_id", ""), "start")
        validation_errors = await self.validator.validate(claim)
        rule_errors = self.rules_engine.evaluate(claim)
        await self._checkpoint(claim.get("claim_id", ""), "validated")
        if validation_errors or rule_errors:
            processing_status["failed"] += 1
            metrics.inc("claims_failed")
            suggestions = self.repair_suggester.suggest(validation_errors + rule_errors)
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
        return claim

    async def _predict_stage(self, claim: Dict[str, Any]) -> tuple[str, str]:
        """Run prediction and enrichment stage."""
        assert self.model
        prediction = self.model.predict(claim)
        if self.model_monitor:
            self.model_monitor.record_prediction(prediction)
        await self._checkpoint(claim.get("claim_id", ""), "predicted")
        claim["filter_number"] = prediction
        if self.rvu_cache:
            rvu = await self.rvu_cache.get(claim.get("procedure_code", ""))
            if rvu:
                claim["rvu_value"] = rvu.get("total_rvu")
                units = float(claim.get("units", 1) or 1)
                conv = float(self.cfg.processing.conversion_factor)
                try:
                    rvu_total = float(rvu.get("total_rvu", 0))
                    claim["reimbursement_amount"] = rvu_total * units * conv
                except Exception:
                    claim["reimbursement_amount"] = None
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

    async def process_partial_claim(
        self, claim_id: str, updates: Dict[str, Any]
    ) -> None:
        """Process an amendment/correction for an existing claim."""
        await self.service.amend_claim(claim_id, updates)
