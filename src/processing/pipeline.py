import asyncio
import contextlib
import os
import time
from typing import Any, Dict, Iterable, List

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
        facilities, classes = await self.service.load_validation_sets()
        self.validator = ClaimValidator(facilities, classes)
        top_codes = await self.service.top_rvu_codes()
        await self.rvu_cache.warm_cache(top_codes)
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

    async def _checkpoint_batch(self, claim_ids: List[str], stage: str) -> None:
        """Record processing checkpoints for a batch of claims."""
        try:
            checkpoint_data = [(claim_id, stage) for claim_id in claim_ids]
            await self.pg.execute_many(
                "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)",
                checkpoint_data,
                concurrency=self.cfg.processing.insert_workers
            )
        except Exception:
            self.logger.debug(
                "batch checkpoint failed",
                extra={"claim_count": len(claim_ids), "stage": stage},
            )

    async def _prefetch_rvu_batch(self, claims: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Bulk prefetch RVU data for a collection of claims."""
        if not self.rvu_cache:
            return {}
        
        # Extract all procedure codes from claims and line items
        codes = set()
        for claim in claims:
            if claim.get("procedure_code"):
                codes.add(claim.get("procedure_code"))
            for line_item in claim.get("line_items", []):
                if line_item.get("procedure_code"):
                    codes.add(line_item.get("procedure_code"))
        
        if codes:
            return await self.rvu_cache.get_many(list(codes))
        return {}

    async def process_batch_vectorized(self) -> None:
        """True vectorized batch processing with bulk operations."""
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
        
        # Use larger batch size for vectorized processing
        batch_size = max(self.calculate_batch_size() * 4, 5000)  # Minimum 5000 for vectorization
        metrics.set("dynamic_batch_size", batch_size)
        
        # Fetch claims in larger batches
        claims = await self.service.fetch_claims(batch_size, priority=True)
        batch_status["total"] = len(claims)
        
        if not claims:
            batch_status["end_time"] = time.perf_counter()
            return

        # VECTORIZED PROCESSING PIPELINE
        
        # 1. Bulk RVU prefetch for entire batch
        claim_ids = [c.get("claim_id", "") for c in claims]
        await self._checkpoint_batch(claim_ids, "start")
        
        rvu_map = await self._prefetch_rvu_batch(claims)
        
        # 2. Vectorized validation - process all claims at once
        validation_results = await self.validator.validate_batch(claims) if self.validator else {}
        
        # 3. Vectorized rules evaluation - batch process
        rules_results = self.rules_engine.evaluate_batch(claims) if self.rules_engine else {}
        
        # 4. Vectorized ML inference - single batch prediction
        predictions: List[int] = []
        if self.model and hasattr(self.model, "predict_batch"):
            predictions = self.model.predict_batch(claims)
        elif self.model:
            # Fallback to individual predictions but in parallel
            prediction_tasks = [asyncio.create_task(self._predict_single(claim)) for claim in claims]
            predictions = await asyncio.gather(*prediction_tasks)
        else:
            predictions = [0] * len(claims)
        
        # 5. Vectorized processing results
        valid_claims = []
        failed_claims = []
        
        for i, (claim, prediction) in enumerate(zip(claims, predictions)):
            claim_id = claim.get("claim_id", "")
            v_errors = validation_results.get(claim_id, [])
            r_errors = rules_results.get(claim_id, [])
            
            if v_errors or r_errors:
                failed_claims.append({
                    "claim": claim,
                    "errors": v_errors + r_errors,
                    "prediction": prediction
                })
            else:
                # Enrich claim with prediction and RVU data
                enriched_claim = self._enrich_claim_with_rvu(claim, prediction, rvu_map)
                valid_claims.append(enriched_claim)
                
                if self.model_monitor:
                    self.model_monitor.record_prediction(prediction)
        
        # 6. Bulk process failed claims
        if failed_claims:
            await self._process_failed_claims_batch(failed_claims)
        
        # 7. Bulk process valid claims
        if valid_claims:
            await self._process_valid_claims_batch(valid_claims)
        
        # 8. Bulk checkpoint completion
        valid_claim_ids = [c.get("claim_id", "") for c in valid_claims]
        failed_claim_ids = [f["claim"].get("claim_id", "") for f in failed_claims]
        
        if valid_claim_ids:
            await self._checkpoint_batch(valid_claim_ids, "completed")
        if failed_claim_ids:
            await self._checkpoint_batch(failed_claim_ids, "failed")
        
        # Update metrics
        processing_status["processed"] += len(valid_claims)
        processing_status["failed"] += len(failed_claims)
        metrics.inc("claims_processed", len(valid_claims))
        metrics.inc("claims_failed", len(failed_claims))
        
        duration = time.perf_counter() - start_time
        rate = len(claims) / duration if duration > 0 else 0.0
        metrics.set("batch_processing_rate_per_sec", rate)
        metrics.set("last_batch_duration_ms", duration * 1000)
        batch_status["end_time"] = time.perf_counter()
        
        self.logger.info(
            "Vectorized batch complete",
            extra={
                "event": "batch_complete",
                "total_claims": len(claims),
                "processed": len(valid_claims),
                "failed": len(failed_claims),
                "duration_sec": round(duration, 4),
                "rate_per_sec": round(rate, 2),
                "vectorized": True
            },
        )
        
        await self.service.flush_checkpoints()

    async def _predict_single(self, claim: Dict[str, Any]) -> int:
        """Single claim prediction for models without batch support."""
        return self.model.predict(claim) if self.model else 0

    def _enrich_claim_with_rvu(self, claim: Dict[str, Any], prediction: int, rvu_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Enrich a single claim with prediction and RVU data."""
        enriched_claim = claim.copy()
        enriched_claim["filter_number"] = prediction
        
        # Process main claim procedure code
        main_code = claim.get("procedure_code")
        if main_code and main_code in rvu_map:
            rvu = rvu_map[main_code]
            enriched_claim["rvu_value"] = rvu.get("total_rvu")
            units = float(claim.get("units", 1) or 1)
            conv = float(self.cfg.processing.conversion_factor)
            try:
                rvu_total = float(rvu.get("total_rvu", 0))
                enriched_claim["reimbursement_amount"] = rvu_total * units * conv
            except Exception:
                enriched_claim["reimbursement_amount"] = None
        
        # Process line items
        for line_item in enriched_claim.get("line_items", []):
            line_code = line_item.get("procedure_code")
            if line_code and line_code in rvu_map:
                rvu = rvu_map[line_code]
                line_item["rvu_value"] = rvu.get("total_rvu")
                units = float(line_item.get("units", 1) or 1)
                conv = float(self.cfg.processing.conversion_factor)
                try:
                    rvu_total = float(rvu.get("total_rvu", 0))
                    line_item["reimbursement_amount"] = rvu_total * units * conv
                except Exception:
                    line_item["reimbursement_amount"] = None
        
        return enriched_claim

    async def _process_failed_claims_batch(self, failed_claims: List[Dict[str, Any]]) -> None:
        """Process a batch of failed claims."""
        failed_claim_data = []
        
        for failed_claim in failed_claims:
            claim = failed_claim["claim"]
            errors = failed_claim["errors"]
            
            suggestions = self.repair_suggester.suggest(errors)
            
            failed_claim_data.append((
                claim.get("claim_id"),
                claim.get("facility_id"),
                claim.get("patient_account_number"),
                "validation",
                ErrorCategory.VALIDATION.value,
                "validation",
                str(claim),
                suggestions
            ))
        
        # Bulk insert failed claims
        if failed_claim_data:
            await self.sql.execute_many(
                "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, failure_category, processing_stage, failed_at, original_data, repair_suggestions)"
                " VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)",
                failed_claim_data,
                concurrency=self.cfg.processing.insert_workers
            )
            
            # Bulk audit logging
            audit_tasks = []
            for failed_claim in failed_claims:
                claim = failed_claim["claim"]
                audit_tasks.append(
                    record_audit_event(
                        self.sql,
                        "failed_claims",
                        claim.get("claim_id", ""),
                        "insert",
                        new_values=claim,
                        reason="validation"
                    )
                )
            
            await asyncio.gather(*audit_tasks)

    async def _process_valid_claims_batch(self, valid_claims: List[Dict[str, Any]]) -> None:
        """Process a batch of valid claims."""
        # Prepare data for bulk insert
        claim_rows = []
        for claim in valid_claims:
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            if patient_acct and facility_id:
                claim_rows.append((patient_acct, facility_id))
        
        if claim_rows:
            try:
                await self.service.insert_claims(
                    claim_rows, 
                    concurrency=self.cfg.processing.insert_workers
                )
                
                # Bulk audit logging
                audit_tasks = []
                for claim in valid_claims:
                    audit_tasks.append(
                        record_audit_event(
                            self.sql,
                            "claims",
                            claim.get("claim_id", ""),
                            "insert",
                            new_values=claim,
                        )
                    )
                
                await asyncio.gather(*audit_tasks)
                
            except Exception as exc:
                category = categorize_exception(exc)
                
                # Bulk enqueue to dead letter queue
                dead_letter_tasks = []
                for claim in valid_claims:
                    dead_letter_tasks.append(
                        self.service.enqueue_dead_letter(claim, str(exc))
                    )
                
                await asyncio.gather(*dead_letter_tasks)
                
                self.logger.error(
                    "Bulk insert failed",
                    exc_info=exc,
                    extra={"category": category.value, "claim_count": len(valid_claims)},
                )
                metrics.inc("claims_failed", len(valid_claims))
                metrics.inc(f"errors_{category.value}", len(valid_claims))

    # Keep all existing methods for backward compatibility
    async def process_batch(self) -> None:
        """Legacy batch processing - delegates to vectorized version."""
        await self.process_batch_vectorized()

    async def process_stream(self) -> None:
        """Enhanced stream processing with vectorized batches."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        
        start_trace()
        batch_size = max(self.calculate_batch_size() * 2, 2000)  # Larger batches for streaming
        
        offset = 0
        while True:
            batch = await self.service.fetch_claims(
                batch_size, offset=offset, priority=True
            )
            if not batch:
                break
            
            # Process each batch using vectorized processing
            await self._process_batch_vectorized_internal(batch)
            offset += batch_size
        
        await self.service.flush_checkpoints()

    async def _process_batch_vectorized_internal(self, claims: List[Dict[str, Any]]) -> None:
        """Internal vectorized processing for a given batch of claims."""
        if not claims:
            return
        
        # Bulk RVU prefetch
        rvu_map = await self._prefetch_rvu_batch(claims)
        
        # Vectorized operations
        validation_results = await self.validator.validate_batch(claims) if self.validator else {}
        rules_results = self.rules_engine.evaluate_batch(claims) if self.rules_engine else {}
        
        # Batch ML inference
        predictions: List[int] = []
        if self.model and hasattr(self.model, "predict_batch"):
            predictions = self.model.predict_batch(claims)
        elif self.model:
            prediction_tasks = [asyncio.create_task(self._predict_single(claim)) for claim in claims]
            predictions = await asyncio.gather(*prediction_tasks)
        else:
            predictions = [0] * len(claims)
        
        # Process results
        valid_claims = []
        failed_claims = []
        
        for claim, prediction in zip(claims, predictions):
            claim_id = claim.get("claim_id", "")
            v_errors = validation_results.get(claim_id, [])
            r_errors = rules_results.get(claim_id, [])
            
            if v_errors or r_errors:
                failed_claims.append({
                    "claim": claim,
                    "errors": v_errors + r_errors,
                    "prediction": prediction
                })
            else:
                enriched_claim = self._enrich_claim_with_rvu(claim, prediction, rvu_map)
                valid_claims.append(enriched_claim)
                
                if self.model_monitor:
                    self.model_monitor.record_prediction(prediction)
        
        # Bulk processing
        if failed_claims:
            await self._process_failed_claims_batch(failed_claims)
        if valid_claims:
            await self._process_valid_claims_batch(valid_claims)
        
        # Update metrics
        processing_status["processed"] += len(valid_claims)
        processing_status["failed"] += len(failed_claims)
        metrics.inc("claims_processed", len(valid_claims))
        metrics.inc("claims_failed", len(failed_claims))

    # ... (keep all other existing methods unchanged)
    async def process_stream_parallel(self) -> None:
        """Process claims using true parallel stages controlled by semaphores."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        start_trace()
        batch_size = self.calculate_batch_size()

        validation_pool = asyncio.Semaphore(50)
        rules_pool = asyncio.Semaphore(50)
        ml_pool = asyncio.Semaphore(25)
        insert_pool = asyncio.Semaphore(10)

        async def insert_row(row: tuple[str, str]) -> None:
            async with insert_pool:
                await self.service.insert_claims(
                    [row], concurrency=self.cfg.processing.insert_workers
                )
                processing_status["processed"] += 1
                metrics.inc("claims_processed")

        async def handle_claim(claim: Dict[str, Any]) -> None:
            res = await self.validate_claim_async(claim, validation_pool)
            if res is None:
                return
            res = await self.evaluate_rules_async(res, rules_pool)
            if res is None:
                return
            row = await self.predict_async(res, ml_pool)
            await insert_row(row)

        async def process_batch_parallel(claims_batch: list[Dict[str, Any]]) -> None:
            await asyncio.gather(*(handle_claim(c) for c in claims_batch))

        offset = 0
        while True:
            batch = await self.service.fetch_claims(
                batch_size, offset=offset, priority=True
            )
            if not batch:
                break
            await self._prefetch_rvu_batch(batch)
            await process_batch_parallel(batch)
            offset += batch_size

        await self.service.flush_checkpoints()

    @retry_async()
    async def process_claim(self, claim: Dict[str, Any]) -> tuple[str, str] | None:
        assert self.rules_engine and self.validator and self.model
        with start_span():
            await self._checkpoint(claim.get("claim_id", ""), "start")
            validation_errors = await self.validator.validate(claim)
            if validation_errors:
                await self._checkpoint(claim.get("claim_id", ""), "failed")
                processing_status["failed"] += 1
                metrics.inc("claims_failed")
                suggestions = self.repair_suggester.suggest(validation_errors)
                await self.service.record_failed_claim(
                    claim,
                    "validation",
                    suggestions,
                    category=ErrorCategory.VALIDATION.value,
                )
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
            rule_errors = self.rules_engine.evaluate(claim)
            await self._checkpoint(claim.get("claim_id", ""), "validated")
            if rule_errors:
                processing_status["failed"] += 1
                metrics.inc("claims_failed")
                suggestions = self.repair_suggester.suggest(rule_errors)
                await self.service.record_failed_claim(
                    claim,
                    "rules",
                    suggestions,
                    category=ErrorCategory.VALIDATION.value,
                )
                await self._checkpoint(claim.get("claim_id", ""), "failed")
                self.logger.error(
                    f"Claim {claim['claim_id']} failed rules",
                    extra={
                        "event": "claim_failed",
                        "claim_id": claim.get("claim_id"),
                        "reason": "rules",
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

    async def process_claims_batch(
        self, claims: list[Dict[str, Any]]
    ) -> list[tuple[str, str]]:
        """Process an already fetched batch of claims using vectorized operations."""
        return await self._process_batch_vectorized_internal(claims)

    async def process_partial_claim(
        self, claim_id: str, updates: Dict[str, Any]
    ) -> None:
        """Process an amendment/correction for an existing claim."""
        await self.service.amend_claim(claim_id, updates)

    async def stream_claims_batches(
        self, batch_size: int, *, priority: bool = True
    ) -> Iterable[list[Dict[str, Any]]]:
        """Yield batches of claims for streaming processing."""
        offset = 0
        while True:
            batch = await self.service.fetch_claims(
                batch_size, offset=offset, priority=priority
            )
            if not batch:
                break
            yield batch
            offset += batch_size

    async def process_claims_batch_optimized(
        self, claims: list[Dict[str, Any]]
    ) -> list[tuple[str, str]]:
        """Optimized wrapper around vectorized batch processing for large batches."""
        await self._process_batch_vectorized_internal(claims)
        
        # Return processed claim identifiers
        valid_rows = []
        for claim in claims:
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            if patient_acct and facility_id:
                valid_rows.append((patient_acct, facility_id))
        
        return valid_rows

    async def process_stream_optimized(self) -> None:
        """Stream claims in batches with concurrency backpressure using vectorized processing."""
        batch_size = 10000  # Large batches for maximum vectorization
        max_concurrent_batches = 5  # Reduced concurrency for larger batches
        semaphore = asyncio.Semaphore(max_concurrent_batches)

        async def process_batch_with_backpressure(batch: list[Dict[str, Any]]) -> None:
            async with semaphore:
                await self._process_batch_vectorized_internal(batch)

        async for batch in self.stream_claims_batches(batch_size):
            # Create task but don't await immediately to allow overlap
            asyncio.create_task(process_batch_with_backpressure(batch))
        
        # Wait for all remaining tasks to complete
        await asyncio.sleep(0.1)  # Allow final tasks to start
        while len(asyncio.all_tasks()) > 1:  # Wait for completion
            await asyncio.sleep(0.1)_account_number"], claim["facility_id"])

    async def _checkpoint(self, claim_id: str, stage: str) -> None:
        """Record a processing checkpoint."""
        try:
            await self.service.record_checkpoint(claim_id, stage, buffered=True)
        except Exception:
            self.logger.debug(
                "checkpoint failed",
                extra={"claim_id": claim_id, "stage": stage},
            )

    async def validate_claim_async(
        self, claim: Dict[str, Any], sem: asyncio.Semaphore
    ) -> Dict[str, Any] | None:
        async with sem:
            return await self._validate_stage(claim)

    async def evaluate_rules_async(
        self, claim: Dict[str, Any], sem: asyncio.Semaphore
    ) -> Dict[str, Any] | None:
        if claim is None:
            return None
        async with sem:
            return await self._rules_stage(claim)

    async def predict_async(
        self, claim: Dict[str, Any], sem: asyncio.Semaphore
    ) -> tuple[str, str]:
        async with sem:
            return await self._predict_stage(claim)

    async def _validate_stage(self, claim: Dict[str, Any]) -> Dict[str, Any] | None:
        """Run the validation stage."""
        assert self.validator
        await self._checkpoint(claim.get("claim_id", ""), "start")
        errors = await self.validator.validate(claim)
        if errors:
            processing_status["failed"] += 1
            metrics.inc("claims_failed")
            suggestions = self.repair_suggester.suggest(errors)
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

    async def _rules_stage(self, claim: Dict[str, Any]) -> Dict[str, Any] | None:
        """Run the rules evaluation stage."""
        assert self.rules_engine
        rule_errors = self.rules_engine.evaluate(claim)
        await self._checkpoint(claim.get("claim_id", ""), "validated")
        if rule_errors:
            processing_status["failed"] += 1
            metrics.inc("claims_failed")
            suggestions = self.repair_suggester.suggest(rule_errors)
            await self.service.record_failed_claim(
                claim,
                "rules",
                suggestions,
                category=ErrorCategory.VALIDATION.value,
            )
            await self._checkpoint(claim.get("claim_id", ""), "failed")
            self.logger.error(
                f"Claim {claim['claim_id']} failed rules",
                extra={
                    "event": "claim_failed",
                    "claim_id": claim.get("claim_id"),
                    "reason": "rules",
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

    async def process_claims_batch(
        self, claims: list[Dict[str, Any]]
    ) -> list[tuple[str, str]]:
        """Process an already fetched batch of claims using vectorized operations."""
        valid_rows = []
        
        # Use internal vectorized processing
        await self._process_batch_vectorized_internal(claims)
        
        # Return processed claim identifiers
        for claim in claims:
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            if patient_acct and facility_id:
                valid_rows.append((patient_acct, facility_id))
        
        return valid_rows

    async def process_partial_claim(
        self, claim_id: str, updates: Dict[str, Any]
    ) -> None:
        """Process an amendment/correction for an existing claim."""
        await self.service.amend_claim(claim_id, updates)

    async def stream_claims_batches(
        self, batch_size: int, *, priority: bool = True
    ) -> Iterable[list[Dict[str, Any]]]:
        """Yield batches of claims for streaming processing."""
        offset = 0
        while True:
            batch = await self.service.fetch_claims(
                batch_size, offset=offset, priority=priority
            )
            if not batch:
                break
            yield batch
            offset += batch_size

    async def process_claims_batch_optimized(
        self, claims: list[Dict[str, Any]]
    ) -> list[tuple[str, str]]:
        """Optimized wrapper around vectorized batch processing for large batches."""
        await self._process_batch_vectorized_internal(claims)
        
        # Return processed claim identifiers
        valid_rows = []
        for claim in claims:
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            if patient_acct and facility_id:
                valid_rows.append((patient_acct, facility_id))
        
        return valid_rows

    async def process_stream_optimized(self) -> None:
        """Stream claims in batches with concurrency backpressure using vectorized processing."""
        batch_size = 10000  # Large batches for maximum vectorization
        max_concurrent_batches = 5  # Reduced concurrency for larger batches
        semaphore = asyncio.Semaphore(max_concurrent_batches)

        async def process_batch_with_backpressure(batch: list[Dict[str, Any]]) -> None:
            async with semaphore:
                await self._process_batch_vectorized_internal(batch)

        async for batch in self.stream_claims_batches(batch_size):
            # Create task but don't await immediately to allow overlap
            asyncio.create_task(process_batch_with_backpressure(batch))
        
        # Wait for all remaining tasks to complete
        await asyncio.sleep(0.1)  # Allow final tasks to start
        while len(asyncio.all_tasks()) > 1:  # Wait for completion
            await asyncio.sleep(0.1)

    async def process_ultra_high_volume(self) -> None:
        """Ultra-high volume processing optimized for 100K+ claims."""
        start_trace()
        start_time = time.perf_counter()
        
        # Use maximum batch sizes for ultra-high volume
        batch_size = 20000  # Very large batches
        max_concurrent_batches = 3  # Fewer concurrent batches to manage memory
        semaphore = asyncio.Semaphore(max_concurrent_batches)
        
        processing_status["processed"] = 0
        processing_status["failed"] = 0
        metrics.set("claims_processed", 0)
        metrics.set("claims_failed", 0)
        
        total_processed = 0
        
        async def process_ultra_batch(batch: list[Dict[str, Any]]) -> None:
            nonlocal total_processed
            async with semaphore:
                try:
                    await self._process_batch_vectorized_internal(batch)
                    total_processed += len(batch)
                    
                    # Log progress for ultra-high volume
                    if total_processed % 50000 == 0:
                        elapsed = time.perf_counter() - start_time
                        rate = total_processed / elapsed if elapsed > 0 else 0
                        self.logger.info(
                            f"Ultra-high volume progress: {total_processed} claims processed, "
                            f"rate: {rate:.2f} claims/sec"
                        )
                except Exception as exc:
                    self.logger.error(
                        f"Ultra batch processing failed for batch of {len(batch)} claims",
                        exc_info=exc
                    )

        # Process in ultra-large batches
        async for batch in self.stream_claims_batches(batch_size):
            asyncio.create_task(process_ultra_batch(batch))
        
        # Wait for completion with progress monitoring
        last_count = 0
        while True:
            await asyncio.sleep(1.0)
            current_tasks = [t for t in asyncio.all_tasks() if not t.done()]
            if len(current_tasks) <= 1:  # Only main task remaining
                break
            
            # Progress monitoring
            if total_processed != last_count:
                last_count = total_processed
                elapsed = time.perf_counter() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                metrics.set("ultra_volume_rate_per_sec", rate)
        
        # Final statistics
        total_duration = time.perf_counter() - start_time
        final_rate = total_processed / total_duration if total_duration > 0 else 0
        
        self.logger.info(
            "Ultra-high volume processing complete",
            extra={
                "total_processed": total_processed,
                "duration_sec": round(total_duration, 2),
                "final_rate_per_sec": round(final_rate, 2),
                "target_achieved": final_rate >= 6667  # 100K/15sec target
            }
        )
        
        await self.service.flush_checkpoints()

    async def benchmark_processing_speed(self, test_batch_size: int = 10000) -> Dict[str, float]:
        """Benchmark processing speed with different configurations."""
        # Generate test data
        test_claims = []
        for i in range(test_batch_size):
            test_claims.append({
                "claim_id": f"test_{i}",
                "patient_account_number": f"acct_{i}",
                "facility_id": "F1",
                "procedure_code": "TEST",
                "financial_class": "TEST",
                "date_of_birth": "1990-01-01",
                "service_from_date": "2024-01-01",
                "service_to_date": "2024-01-02",
                "primary_diagnosis": "TEST"
            })
        
        results = {}
        
        # Benchmark vectorized processing
        start_time = time.perf_counter()
        await self._process_batch_vectorized_internal(test_claims)
        vectorized_time = time.perf_counter() - start_time
        vectorized_rate = len(test_claims) / vectorized_time
        
        results["vectorized_processing"] = {
            "duration_sec": vectorized_time,
            "rate_per_sec": vectorized_rate,
            "batch_size": len(test_claims)
        }
        
        # Benchmark individual processing (sample)
        sample_size = min(100, len(test_claims))
        sample_claims = test_claims[:sample_size]
        
        start_time = time.perf_counter()
        for claim in sample_claims:
            await self.process_claim(claim)
        individual_time = time.perf_counter() - start_time
        individual_rate = sample_size / individual_time
        
        results["individual_processing"] = {
            "duration_sec": individual_time,
            "rate_per_sec": individual_rate,
            "batch_size": sample_size
        }
        
        # Calculate performance improvement
        if individual_rate > 0:
            improvement_factor = vectorized_rate / individual_rate
            results["performance_improvement"] = {
                "speedup_factor": improvement_factor,
                "efficiency_gain_percent": (improvement_factor - 1) * 100
            }
        
        return results

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        return {
            "configuration": {
                "batch_size": self.cfg.processing.batch_size,
                "max_workers": self.cfg.processing.max_workers,
                "insert_workers": self.cfg.processing.insert_workers,
                "vectorized_processing": True
            },
            "processing_status": dict(processing_status),
            "batch_status": dict(batch_status),
            "features_enabled": {
                "cache": self.features.enable_cache,
                "model_monitor": self.features.enable_model_monitor,
                "distributed_cache": self.distributed_cache is not None,
                "rvu_cache": self.rvu_cache is not None
            },
            "components": {
                "model_type": type(self.model).__name__ if self.model else None,
                "model_version": getattr(self.model, "version", None),
                "rules_count": len(self.rules_engine.rules) if self.rules_engine else 0,
                "validator_facilities": len(self.validator.valid_facilities) if self.validator else 0,
                "validator_classes": len(self.validator.valid_financial_classes) if self.validator else 0
            }
        }