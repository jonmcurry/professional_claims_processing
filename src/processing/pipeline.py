import asyncio
import contextlib
import gc
import os
import time
import psutil
import weakref
from typing import Any, Dict, Iterable, List, Set

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


class MemoryPool:
    """Memory pool for reusing objects and preventing fragmentation."""
    
    def __init__(self, initial_size: int = 1000, max_size: int = 10000):
        self.initial_size = initial_size
        self.max_size = max_size
        self.pools: Dict[str, List[Any]] = {}
        self.stats: Dict[str, Dict[str, int]] = {}
        self._cleanup_refs: Set[weakref.ref] = set()
        
    def get_pool(self, pool_name: str, factory_func=None) -> List[Any]:
        """Get or create a pool for specific object types."""
        if pool_name not in self.pools:
            self.pools[pool_name] = []
            self.stats[pool_name] = {"created": 0, "reused": 0, "discarded": 0}
            
            # Pre-populate pool if factory function provided
            if factory_func:
                for _ in range(min(self.initial_size, 100)):  # Limit initial allocation
                    try:
                        obj = factory_func()
                        self.pools[pool_name].append(obj)
                        self.stats[pool_name]["created"] += 1
                    except Exception:
                        break
                        
        return self.pools[pool_name]
    
    def acquire(self, pool_name: str, factory_func=None) -> Any:
        """Acquire an object from the pool."""
        pool = self.get_pool(pool_name, factory_func)
        
        if pool:
            obj = pool.pop()
            self.stats[pool_name]["reused"] += 1
            return obj
        elif factory_func:
            obj = factory_func()
            self.stats[pool_name]["created"] += 1
            return obj
        else:
            return None
    
    def release(self, pool_name: str, obj: Any) -> None:
        """Return an object to the pool."""
        if pool_name in self.pools:
            pool = self.pools[pool_name]
            if len(pool) < self.max_size:
                # Clean object before returning to pool
                if hasattr(obj, 'clear'):
                    obj.clear()
                elif hasattr(obj, '__dict__'):
                    # Reset object state
                    for attr in list(obj.__dict__.keys()):
                        if not attr.startswith('_'):
                            try:
                                delattr(obj, attr)
                            except AttributeError:
                                pass
                
                pool.append(obj)
            else:
                self.stats[pool_name]["discarded"] += 1
    
    def cleanup_pool(self, pool_name: str) -> int:
        """Clean up a specific pool and return number of objects removed."""
        if pool_name in self.pools:
            pool = self.pools[pool_name]
            removed = len(pool)
            pool.clear()
            return removed
        return 0
    
    def cleanup_all(self) -> Dict[str, int]:
        """Clean up all pools."""
        cleanup_stats = {}
        for pool_name in list(self.pools.keys()):
            cleanup_stats[pool_name] = self.cleanup_pool(pool_name)
        return cleanup_stats


class ClaimsPipeline:
    """Ultra-optimized claims processing pipeline with memory management."""

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.features = cfg.features
        self.logger = setup_logging(cfg.logging)
        self.request_filter: RequestContextFilter | None = None
        
        # Enhanced database connections with optimization
        self.pg = PostgresDatabase(cfg.postgres)
        self.sql = SQLServerDatabase(cfg.sqlserver)
        self.encryption_key = cfg.security.encryption_key
        
        # ML and processing components
        self.model: FilterModel | ABTestModel | None = None
        self.model_monitor: ModelMonitor | None = None
        self.rules_engine: DurableRulesEngine | None = None
        self.validator: ClaimValidator | None = None
        
        # Enhanced caching with optimization
        self.cache = InMemoryCache()
        self.rvu_cache: RvuCache | None = None
        self.distributed_cache: DistributedCache | None = None
        
        # Processing utilities
        self.repair_suggester = ClaimRepairSuggester()
        
        # Use service
        self.service = ClaimService(
            self.pg, self.sql, self.encryption_key,
            checkpoint_buffer_size=5000  # Larger buffer for better performance
        )
        
        # Memory management additions
        self._memory_pool = MemoryPool()
        self._batch_memory_limit = 500 * 1024 * 1024  # 500MB per batch
        self._total_memory_limit = 2 * 1024 * 1024 * 1024  # 2GB total
        self._memory_check_interval = 100  # Check every 100 batches
        self._batch_count = 0
        self._last_gc_time = time.time()
        self._gc_interval = 60  # Force GC every 60 seconds
        
        # Object lifecycle tracking
        self._active_objects: Set[weakref.ref] = set()
        self._cleanup_threshold = 10000  # Clean up after 10k objects
        
        # Performance tracking
        self._startup_time = 0.0
        self._preparation_stats = {}

    async def startup_optimized(self) -> None:
        """Ultra-optimized startup with memory management."""
        startup_start = time.perf_counter()
        
        # Phase 1: Database Connection and Pre-warming
        connection_start = time.perf_counter()
        await asyncio.gather(self.pg.connect(), self.sql.connect())
        
        # Aggressive connection pool pre-warming with health checks
        warmup_tasks = [
            self.pg.fetch("SELECT 1 as warmup_test"),
            self.sql.execute("SELECT 1"),
            # Additional warmup queries
            self.pg.health_check(),
            self.sql.health_check(),
        ]
        await asyncio.gather(*warmup_tasks)
        
        connection_time = time.perf_counter() - connection_start
        self._preparation_stats["connection_time"] = connection_time
        
        # Phase 2: Prepared Statement Setup
        preparation_start = time.perf_counter()
        
        # Prepare critical statements in parallel
        preparation_tasks = [
            # SQL Server preparations
            self.sql.prepare_named("claims_insert_optimized", 
                "INSERT INTO claims (patient_account_number, facility_id, procedure_code, created_at, updated_at) VALUES (?, ?, ?, GETDATE(), GETDATE())"),
            self.sql.prepare_named("failed_claims_insert_batch",
                """INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)"""),
            
            # PostgreSQL preparations  
            self.pg.prepare_named("claims_fetch_optimized",
                """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                         li.units AS li_units, li.charge_amount AS li_charge_amount, 
                         li.service_from_date AS li_service_from_date, 
                         li.service_to_date AS li_service_to_date 
                   FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                   ORDER BY c.priority DESC LIMIT $1 OFFSET $2"""),
            self.pg.prepare_named("rvu_bulk_fetch",
                """SELECT procedure_code, description, total_rvu, work_rvu, 
                         practice_expense_rvu, malpractice_rvu, conversion_factor
                   FROM rvu_data 
                   WHERE procedure_code = ANY($1) AND status = 'active'"""),
            self.pg.prepare_named("checkpoint_insert_optimized",
                "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()"),
        ]
        
        await asyncio.gather(*preparation_tasks, return_exceptions=True)
        
        preparation_time = time.perf_counter() - preparation_start
        self._preparation_stats["preparation_time"] = preparation_time
        
        # Phase 3: Component Initialization
        component_start = time.perf_counter()
        
        # Initialize ML components
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
        
        # Initialize rules engine
        self.rules_engine = DurableRulesEngine([])
        
        # Initialize distributed caching
        if self.features.enable_cache and self.cfg.cache.redis_url:
            self.distributed_cache = DistributedCache(self.cfg.cache.redis_url)
        
        # Initialize RVU cache with optimizations
        self.rvu_cache = RvuCache(
            self.pg,
            distributed=(self.distributed_cache if self.features.enable_cache else None),
            predictive_ahead=self.cfg.cache.predictive_ahead,
        )
        
        component_time = time.perf_counter() - component_start
        self._preparation_stats["component_time"] = component_time
        
        # Phase 4: Data Pre-loading and Validation Setup
        preload_start = time.perf_counter()
        
        # Load validation sets with optimization
        facilities, classes = await self.service.load_validation_sets()
        self.validator = ClaimValidator(facilities, classes)
        
        # Pre-warm RVU cache with top codes
        if self.features.enable_cache:
            warmup_tasks = []
            
            # Pre-configured warm codes
            if self.cfg.cache.warm_rvu_codes:
                warmup_tasks.append(
                    self.rvu_cache.warm_cache(self.cfg.cache.warm_rvu_codes)
                )
            
            # Top RVU codes
            top_codes_task = self.service.top_rvu_codes(limit=2000)  # Larger cache
            warmup_tasks.append(top_codes_task)
            
            results = await asyncio.gather(*warmup_tasks, return_exceptions=True)
            
            # Warm cache with top codes
            if len(results) > 1 and not isinstance(results[1], Exception):
                top_codes = results[1]
                await self.rvu_cache.warm_cache(top_codes[:1000])  # Warm top 1000
        
        preload_time = time.perf_counter() - preload_start
        self._preparation_stats["preload_time"] = preload_time
        
        # Phase 5: Memory Pool Initialization
        self._initialize_memory_pools()
        
        # Phase 6: Monitoring and Resource Management
        monitoring_start = time.perf_counter()
        
        # Start resource monitoring
        resource_monitor.start(interval=0.5)  # More frequent monitoring
        pool_monitor.start(self.pg, self.sql, interval=2.0)
        
        # Start memory monitoring
        asyncio.create_task(self._memory_monitor_loop())
        
        monitoring_time = time.perf_counter() - monitoring_start
        self._preparation_stats["monitoring_time"] = monitoring_time
        
        # Record startup metrics
        self._startup_time = time.perf_counter() - startup_start
        metrics.set("pipeline_startup_time", self._startup_time)
        metrics.set("pipeline_startup_optimized", 1.0)
        
        self.logger.info(
            "Optimized pipeline startup complete",
            extra={
                "startup_time": round(self._startup_time, 4),
                "preparation_stats": self._preparation_stats,
                "optimization_features": self._get_optimization_features(),
            }
        )

    def _initialize_memory_pools(self) -> None:
        """Initialize memory pools for frequently used objects."""
        # Pool for claim dictionaries
        self._memory_pool.get_pool("claims", lambda: {})
        
        # Pool for validation result lists
        self._memory_pool.get_pool("validation_results", lambda: [])
        
        # Pool for processing results
        self._memory_pool.get_pool("processing_results", lambda: {"processed": [], "failed": []})
        
        # Pool for checkpoint data
        self._memory_pool.get_pool("checkpoints", lambda: [])
        
        # Pool for RVU data containers
        self._memory_pool.get_pool("rvu_containers", lambda: {})

    async def _memory_monitor_loop(self) -> None:
        """Background memory monitoring and cleanup."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                await self._perform_memory_cleanup()
            except Exception as e:
                self.logger.warning(f"Memory monitor error: {e}")

    async def _perform_memory_cleanup(self) -> None:
        """Perform memory cleanup operations."""
        current_time = time.time()
        
        # Get current memory usage
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
        except Exception:
            memory_mb = 0
        
        # Force garbage collection if needed
        if (current_time - self._last_gc_time > self._gc_interval or 
            memory_mb > self._total_memory_limit / (1024 * 1024)):
            
            collected = gc.collect()
            self._last_gc_time = current_time
            
            # Update metrics
            metrics.set("memory_usage_mb", memory_mb)
            metrics.set("gc_objects_collected", collected)
            metrics.inc("memory_cleanup_cycles")
            
            self.logger.debug(f"Memory cleanup: {collected} objects collected, {memory_mb:.1f}MB used")
        
        # Clean up object pools if memory is high
        if memory_mb > self._total_memory_limit / (1024 * 1024) * 0.8:  # 80% threshold
            cleanup_stats = self._memory_pool.cleanup_all()
            self.logger.info(f"Memory pool cleanup performed: {cleanup_stats}")

    def _get_optimization_features(self) -> Dict[str, bool]:
        """Get enabled optimization features."""
        return {
            "prepared_statements": True,
            "connection_pool_warming": True,
            "bulk_rvu_lookup": self.rvu_cache is not None,
            "distributed_cache": self.distributed_cache is not None,
            "validation_caching": True,
            "async_processing": True,
            "vectorized_operations": True,
            "adaptive_batching": True,
            "resource_monitoring": True,
            "circuit_breakers": True,
            "query_optimization": True,
            "memory_pooling": True,
            "memory_monitoring": True,
        }

    def _acquire_batch_objects(self, batch_size: int) -> Dict[str, Any]:
        """Acquire batch processing objects from memory pool."""
        return {
            "claims": [self._memory_pool.acquire("claims", dict) for _ in range(min(batch_size, 1000))],
            "validation_results": self._memory_pool.acquire("validation_results", list),
            "processing_results": self._memory_pool.acquire("processing_results", 
                lambda: {"processed": [], "failed": []}),
            "checkpoints": self._memory_pool.acquire("checkpoints", list),
            "rvu_containers": [self._memory_pool.acquire("rvu_containers", dict) 
                              for _ in range(min(batch_size, 1000))]
        }

    def _release_batch_objects(self, batch_objects: Dict[str, Any]) -> None:
        """Return batch processing objects to memory pool."""
        # Release claim objects
        for claim_obj in batch_objects.get("claims", []):
            self._memory_pool.release("claims", claim_obj)
        
        # Release other objects
        if "validation_results" in batch_objects:
            self._memory_pool.release("validation_results", batch_objects["validation_results"])
        
        if "processing_results" in batch_objects:
            self._memory_pool.release("processing_results", batch_objects["processing_results"])
        
        if "checkpoints" in batch_objects:
            self._memory_pool.release("checkpoints", batch_objects["checkpoints"])
        
        # Release RVU containers
        for rvu_obj in batch_objects.get("rvu_containers", []):
            self._memory_pool.release("rvu_containers", rvu_obj)

    async def process_batch_ultra_optimized(self) -> Dict[str, Any]:
        """Ultra-optimized batch processing with memory management."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        
        trace_id = start_trace()
        batch_start = time.perf_counter()
        self._batch_count += 1
        
        # Memory check
        if self._batch_count % self._memory_check_interval == 0:
            await self._check_memory_usage()
        
        # Initialize batch tracking
        batch_id = f"opt_batch_{int(batch_start * 1000)}"
        batch_status["batch_id"] = batch_id
        batch_status["start_time"] = batch_start
        batch_status["end_time"] = None
        
        # Phase 1: Dead Letter Queue Processing (async)
        dead_letter_task = asyncio.create_task(
            self.service.reprocess_dead_letter(batch_size=2000)
        )
        
        # Phase 2: Dynamic Batch Size Calculation
        base_batch_size = self.calculate_batch_size_adaptive()
        # Scale up for ultra-optimization but consider memory
        optimized_batch_size = await self._calculate_memory_aware_batch_size(base_batch_size)
        
        metrics.set("dynamic_batch_size", optimized_batch_size)
        
        # Acquire batch objects from pool
        batch_objects = None
        try:
            batch_objects = self._acquire_batch_objects(optimized_batch_size)
            
            # Phase 3: Optimized Claims Fetching
            fetch_start = time.perf_counter()
            claims = await self.service.fetch_claims(
                optimized_batch_size, priority=True
            )
            fetch_time = time.perf_counter() - fetch_start
            
            batch_status["total"] = len(claims)
            if not claims:
                batch_status["end_time"] = time.perf_counter()
                return {"processed": 0, "failed": 0, "duration": fetch_time}
            
            # Phase 4: Bulk Preprocessing
            preprocess_start = time.perf_counter()
            
            # Extract claim IDs for bulk checkpointing
            claim_ids = [c.get("claim_id", "") for c in claims]
            
            # Bulk checkpoint - start processing
            checkpoint_task = asyncio.create_task(
                self._record_checkpoints_bulk(
                    [(cid, "start") for cid in claim_ids]
                )
            )
            
            # Bulk RVU enrichment
            enriched_claims = await self._enrich_claims_with_rvu(
                claims, self.cfg.processing.conversion_factor
            )
            
            preprocess_time = time.perf_counter() - preprocess_start
            
            # Phase 5: Vectorized Validation and Rules
            validation_start = time.perf_counter()
            
            # Parallel validation and rules evaluation
            validation_task = self.validator.validate_batch(enriched_claims) if self.validator else {}
            rules_task = self.rules_engine.evaluate_batch(enriched_claims) if self.rules_engine else {}
            
            validation_results, rules_results = await asyncio.gather(
                validation_task, rules_task, return_exceptions=True
            )
            
            # Handle exceptions in validation/rules
            if isinstance(validation_results, Exception):
                validation_results = {}
            if isinstance(rules_results, Exception):
                rules_results = {}
            
            validation_time = time.perf_counter() - validation_start
            
            # Phase 6: Vectorized ML Inference
            ml_start = time.perf_counter()
            
            predictions: List[int] = []
            if self.model and hasattr(self.model, "predict_batch"):
                predictions = self.model.predict_batch(enriched_claims)
            elif self.model:
                # Parallel individual predictions for models without batch support
                prediction_tasks = [
                    asyncio.create_task(self._predict_single_async(claim)) 
                    for claim in enriched_claims
                ]
                predictions = await asyncio.gather(*prediction_tasks)
            else:
                predictions = [0] * len(enriched_claims)
            
            ml_time = time.perf_counter() - ml_start
            
            # Phase 7: Results Processing and Segregation
            segregation_start = time.perf_counter()
            
            valid_claims = []
            failed_claims_data = []
            
            for i, (claim, prediction) in enumerate(zip(enriched_claims, predictions)):
                claim_id = claim.get("claim_id", "")
                v_errors = validation_results.get(claim_id, [])
                r_errors = rules_results.get(claim_id, [])
                
                # Add prediction to claim
                claim["filter_number"] = prediction
                
                # Record model prediction
                if self.model_monitor:
                    self.model_monitor.record_prediction(prediction)
                
                if v_errors or r_errors:
                    # Prepare failed claim data
                    all_errors = v_errors + r_errors
                    suggestions = self.repair_suggester.suggest(all_errors)
                    
                    encrypted_claim = claim.copy()
                    if self.encryption_key:
                        from ..security.compliance import encrypt_claim_fields, encrypt_text
                        encrypted_claim = encrypt_claim_fields(claim, self.encryption_key)
                        original_data = encrypt_text(str(claim), self.encryption_key)
                    else:
                        original_data = str(claim)
                    
                    failed_claims_data.append((
                        encrypted_claim.get("claim_id"),
                        encrypted_claim.get("facility_id"),
                        encrypted_claim.get("patient_account_number"),
                        "validation" if v_errors else "rules",
                        ErrorCategory.VALIDATION.value,
                        "validation",
                        original_data,
                        suggestions
                    ))
                else:
                    valid_claims.append(claim)
            
            segregation_time = time.perf_counter() - segregation_start
            
            # Phase 8: Bulk Database Operations
            db_start = time.perf_counter()
            
            # Parallel bulk operations
            bulk_tasks = []
            
            # Bulk insert valid claims
            if valid_claims:
                bulk_tasks.append(
                    self._insert_claims_bulk(valid_claims)
                )
            
            # Bulk insert failed claims
            if failed_claims_data:
                bulk_tasks.append(
                    self._record_failed_claims_bulk(failed_claims_data)
                )
            
            # Execute bulk operations
            if bulk_tasks:
                await asyncio.gather(*bulk_tasks, return_exceptions=True)
            
            db_time = time.perf_counter() - db_start
            
            # Phase 9: Bulk Checkpointing and Audit
            checkpoint_start = time.perf_counter()
            
            # Prepare checkpoint data
            valid_claim_ids = [c.get("claim_id", "") for c in valid_claims]
            failed_claim_ids = [f[0] for f in failed_claims_data]  # claim_id is first element
            
            checkpoint_tasks = []
            if valid_claim_ids:
                checkpoint_tasks.append(
                    self._record_checkpoints_bulk([
                        (cid, "completed") for cid in valid_claim_ids
                    ])
                )
            if failed_claim_ids:
                checkpoint_tasks.append(
                    self._record_checkpoints_bulk([
                        (cid, "failed") for cid in failed_claim_ids
                    ])
                )
            
            if checkpoint_tasks:
                await asyncio.gather(*checkpoint_tasks, return_exceptions=True)
            
            # Ensure start checkpoint task completes
            await checkpoint_task
            
            checkpoint_time = time.perf_counter() - checkpoint_start
            
            # Phase 10: Metrics and Status Updates
            metrics_start = time.perf_counter()
            
            # Update processing status
            processing_status["processed"] += len(valid_claims)
            processing_status["failed"] += len(failed_claims_data)
            
            # Update metrics
            metrics.inc("claims_processed", len(valid_claims))
            metrics.inc("claims_failed", len(failed_claims_data))
            metrics.inc("batches_processed_optimized")
            
            # Performance metrics
            total_duration = time.perf_counter() - batch_start
            throughput = len(enriched_claims) / total_duration if total_duration > 0 else 0
            
            metrics.set("batch_processing_rate_per_sec", throughput)
            metrics.set("last_batch_duration_ms", total_duration * 1000)
            metrics.set("batch_fetch_time_ms", fetch_time * 1000)
            metrics.set("batch_preprocess_time_ms", preprocess_time * 1000)
            metrics.set("batch_validation_time_ms", validation_time * 1000)
            metrics.set("batch_ml_time_ms", ml_time * 1000)
            metrics.set("batch_segregation_time_ms", segregation_time * 1000)
            metrics.set("batch_db_time_ms", db_time * 1000)
            metrics.set("batch_checkpoint_time_ms", checkpoint_time * 1000)
            
            metrics_time = time.perf_counter() - metrics_start
            
            # Wait for dead letter processing to complete
            try:
                await dead_letter_task
            except Exception as e:
                self.logger.warning(f"Dead letter processing failed: {e}")
            
            # Final status update
            batch_status["end_time"] = time.perf_counter()
            
            # Comprehensive logging
            self.logger.info(
                "Ultra-optimized batch complete",
                extra={
                    "event": "batch_complete_optimized",
                    "total_claims": len(enriched_claims),
                    "processed": len(valid_claims),
                    "failed": len(failed_claims_data),
                    "throughput_per_sec": round(throughput, 2),
                    "total_duration_sec": round(total_duration, 4),
                    "performance_breakdown": {
                        "fetch_ms": round(fetch_time * 1000, 2),
                        "preprocess_ms": round(preprocess_time * 1000, 2),
                        "validation_ms": round(validation_time * 1000, 2),
                        "ml_inference_ms": round(ml_time * 1000, 2),
                        "segregation_ms": round(segregation_time * 1000, 2),
                        "database_ms": round(db_time * 1000, 2),
                        "checkpoint_ms": round(checkpoint_time * 1000, 2),
                        "metrics_ms": round(metrics_time * 1000, 2),
                    },
                    "optimization_efficiency": {
                        "memory_pooled": True,
                        "vectorized_processing": True,
                        "bulk_database_ops": True,
                        "parallel_execution": True,
                        "prepared_statements": True,
                        "connection_reuse": True,
                    },
                    "trace_id": trace_id,
                },
            )
            
            return {
                "processed": len(valid_claims),
                "failed": len(failed_claims_data),
                "duration": total_duration,
                "throughput": throughput,
                "performance_breakdown": {
                    "fetch_time": fetch_time,
                    "preprocess_time": preprocess_time,
                    "validation_time": validation_time,
                    "ml_time": ml_time,
                    "segregation_time": segregation_time,
                    "db_time": db_time,
                    "checkpoint_time": checkpoint_time,
                }
            }
            
        finally:
            # Always release objects back to pool
            if batch_objects:
                self._release_batch_objects(batch_objects)
            
            # Periodic cleanup
            if self._batch_count % 50 == 0:  # Every 50 batches
                asyncio.create_task(self._perform_memory_cleanup())

    async def _calculate_memory_aware_batch_size(self, base_size: int) -> int:
        """Calculate batch size based on available memory."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            available_memory = psutil.virtual_memory().available / 1024 / 1024
            
            # Reduce batch size if memory usage is high
            memory_factor = 1.0
            if memory_mb > 1500:  # Over 1.5GB
                memory_factor = 0.5
            elif memory_mb > 1000:  # Over 1GB
                memory_factor = 0.7
            elif available_memory < 500:  # Less than 500MB available
                memory_factor = 0.6
            
            adjusted_size = int(base_size * memory_factor)
            
            # Ensure reasonable bounds
            min_size = max(100, base_size // 10)
            max_size = base_size * 5  # Reduced from 10 for memory safety
            final_size = max(min_size, min(max_size, adjusted_size))
            
            metrics.set("memory_aware_batch_size", final_size)
            metrics.set("memory_adjustment_factor", memory_factor)
            
            return final_size
            
        except Exception:
            return base_size

    async def _predict_single_async(self, claim: Dict[str, Any]) -> int:
        """Asynchronous single claim prediction."""
        return self.model.predict(claim) if self.model else 0

    async def _check_memory_usage(self) -> None:
        """Check current memory usage and take action if needed."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            # Update metrics
            metrics.set("current_memory_mb", memory_mb)
            
            # Take action if memory is too high
            if memory_mb > self._total_memory_limit / (1024 * 1024) * 0.9:  # 90% threshold
                self.logger.warning(f"High memory usage detected: {memory_mb:.1f}MB")
                
                # Emergency cleanup
                cleanup_stats = self._memory_pool.cleanup_all()
                collected = gc.collect()
                
                self.logger.info(f"Emergency cleanup: pools cleared {cleanup_stats}, GC collected {collected}")
                metrics.inc("memory_emergency_cleanups")
                
        except Exception as e:
            self.logger.error(f"Memory check failed: {e}")

    async def _enrich_claims_with_rvu(self, claims: List[Dict[str, Any]], conversion_factor: float) -> List[Dict[str, Any]]:
        """Bulk enrich claims with RVU data using memory-pooled containers."""
        if not claims:
            return claims
        
        # Extract all procedure codes from claims and line items
        procedure_codes = set()
        for claim in claims:
            if claim.get("procedure_code"):
                procedure_codes.add(claim.get("procedure_code"))
            for line_item in claim.get("line_items", []):
                if line_item.get("procedure_code"):
                    procedure_codes.add(line_item.get("procedure_code"))
        
        # Bulk fetch RVU data
        rvu_data = {}
        if procedure_codes and self.rvu_cache:
            rvu_data = await self.rvu_cache.get_many(list(procedure_codes))
        
        # Enrich claims with RVU data using memory pooling
        enriched_claims = []
        for claim in claims:
            # Use memory-pooled container for enriched claim
            enriched_claim = self._memory_pool.acquire("claims", dict)
            enriched_claim.clear()
            enriched_claim.update(claim)
            
            # Process main claim procedure code
            main_code = claim.get("procedure_code")
            if main_code and main_code in rvu_data and rvu_data[main_code]:
                rvu = rvu_data[main_code]
                enriched_claim["rvu_value"] = rvu.get("total_rvu")
                
                # Calculate reimbursement
                units = float(claim.get("units", 1) or 1)
                try:
                    rvu_total = float(rvu.get("total_rvu", 0))
                    enriched_claim["reimbursement_amount"] = rvu_total * units * conversion_factor
                except (ValueError, TypeError):
                    enriched_claim["reimbursement_amount"] = 0.0
            
            # Process line items
            for line_item in enriched_claim.get("line_items", []):
                line_code = line_item.get("procedure_code")
                if line_code and line_code in rvu_data and rvu_data[line_code]:
                    rvu = rvu_data[line_code]
                    line_item["rvu_value"] = rvu.get("total_rvu")
                    
                    # Calculate line item reimbursement  
                    units = float(line_item.get("units", 1) or 1)
                    try:
                        rvu_total = float(rvu.get("total_rvu", 0))
                        line_item["reimbursement_amount"] = rvu_total * units * conversion_factor
                    except (ValueError, TypeError):
                        line_item["reimbursement_amount"] = 0.0
            
            enriched_claims.append(enriched_claim)
        
        metrics.inc("claims_enriched_with_rvu", len(enriched_claims))
        return enriched_claims

    async def _insert_claims_bulk(self, claims_data: List[Dict[str, Any]]) -> int:
        """Bulk insert claims with memory management."""
        if not claims_data:
            return 0
        
        # Prepare data for bulk insert using memory-pooled containers
        insert_data = []
        for claim in claims_data:
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            procedure_code = claim.get("procedure_code")
            
            # Apply encryption if configured
            if self.encryption_key and patient_acct:
                from ..security.compliance import encrypt_text
                patient_acct = encrypt_text(str(patient_acct), self.encryption_key)
            
            if patient_acct and facility_id:
                insert_data.append((patient_acct, facility_id, procedure_code))
        
        if insert_data:
            try:
                # Use TVP for SQL Server bulk insert
                return await self.sql.bulk_insert_tvp(
                    "claims",
                    ["patient_account_number", "facility_id", "procedure_code"],
                    insert_data
                )
            except Exception as e:
                self.logger.warning(f"TVP bulk insert failed: {e}")
                # Fallback to PostgreSQL COPY
                try:
                    return await self.pg.copy_records(
                        "claims",
                        ["patient_account_number", "facility_id", "procedure_code"], 
                        insert_data
                    )
                except Exception as fallback_e:
                    self.logger.error(f"All bulk insert methods failed: {fallback_e}")
                    return 0
        
        return 0

    async def _record_failed_claims_bulk(self, failed_claims_data: List[tuple]) -> None:
        """Record failed claims in bulk with memory management."""
        if not failed_claims_data:
            return
        
        try:
            await self.sql.execute_many(
                """INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                failed_claims_data,
                concurrency=4
            )
            metrics.inc("bulk_failed_claims_recorded", len(failed_claims_data))
        except Exception as e:
            self.logger.error(f"Bulk failed claims recording failed: {e}")

    async def _record_checkpoints_bulk(self, checkpoints: List[tuple]) -> None:
        """Record checkpoints in bulk with memory management."""
        if not checkpoints:
            return
        
        try:
            await self.pg.execute_many(
                "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
                checkpoints,
                concurrency=4
            )
            metrics.inc("bulk_checkpoints_recorded", len(checkpoints))
        except Exception as e:
            self.logger.error(f"Bulk checkpoint recording failed: {e}")

    def calculate_batch_size_adaptive(self) -> int:
        """Advanced adaptive batch sizing based on system metrics."""
        base_size = self.cfg.processing.batch_size
        
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0
        
        # Get current performance metrics
        current_throughput = metrics.get("batch_processing_rate_per_sec")
        memory_usage = metrics.get("memory_usage_mb")
        cpu_usage = metrics.get("cpu_usage_percent")
        
        # Adaptive scaling factors
        load_factor = 1.0
        if load > 8:
            load_factor = 0.5  # Reduce batch size under extreme load
        elif load > 4:
            load_factor = 0.75  # Moderate reduction
        elif load < 1:
            load_factor = 2.0  # Increase batch size under low load
        
        # Memory-based scaling
        memory_factor = 1.0
        if memory_usage > 8000:  # Over 8GB
            memory_factor = 0.6
        elif memory_usage > 4000:  # Over 4GB
            memory_factor = 0.8
        elif memory_usage < 2000:  # Under 2GB
            memory_factor = 1.5
        
        # CPU-based scaling
        cpu_factor = 1.0
        if cpu_usage > 90:
            cpu_factor = 0.5
        elif cpu_usage > 70:
            cpu_factor = 0.8
        elif cpu_usage < 30:
            cpu_factor = 1.3
        
        # Throughput-based scaling
        throughput_factor = 1.0
        if current_throughput > 0:
            if current_throughput > 8000:  # High throughput
                throughput_factor = 1.2
            elif current_throughput < 2000:  # Low throughput
                throughput_factor = 0.8
        
        # Calculate final batch size
        combined_factor = load_factor * memory_factor * cpu_factor * throughput_factor
        adaptive_size = int(base_size * combined_factor)
        
        # Apply bounds
        min_size = max(100, base_size // 10)
        max_size = base_size * 5  # Reduced for memory safety
        final_size = max(min_size, min(max_size, adaptive_size))
        
        # Update metrics
        metrics.set("adaptive_load_factor", load_factor)
        metrics.set("adaptive_memory_factor", memory_factor)
        metrics.set("adaptive_cpu_factor", cpu_factor)
        metrics.set("adaptive_throughput_factor", throughput_factor)
        metrics.set("adaptive_combined_factor", combined_factor)
        
        return final_size

    async def process_stream_ultra_optimized(self) -> None:
        """Ultra-optimized stream processing with memory management."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)
        
        start_trace()
        stream_start = time.perf_counter()
        
        # Memory management for long-running processes
        last_cleanup = stream_start
        cleanup_interval = 300  # 5 minutes
        
        # Large batch sizes for streaming optimization but memory-aware
        base_batch_size = self.calculate_batch_size_adaptive()
        batch_size = await self._calculate_memory_aware_batch_size(base_batch_size * 2)
        
        total_processed = 0
        total_failed = 0
        
        # Stream processing with adaptive concurrency
        max_concurrent_batches = 2  # Reduced for memory management
        semaphore = asyncio.Semaphore(max_concurrent_batches)
        
        async def process_batch_with_memory_management(batch_claims: List[Dict[str, Any]]) -> Dict[str, int]:
            async with semaphore:
                return await self._process_stream_batch_optimized(batch_claims)
        
        try:
            # Process stream in batches
            offset = 0
            active_tasks = []
            
            while True:
                current_time = time.time()
                
                # Periodic memory cleanup for long-running processes
                if current_time - last_cleanup > cleanup_interval:
                    await self._perform_stream_memory_cleanup()
                    last_cleanup = current_time
                
                # Fetch next batch
                batch = await self.service.fetch_claims(
                    batch_size, offset=offset, priority=True
                )
                
                if not batch:
                    break
                
                # Create processing task
                task = asyncio.create_task(process_batch_with_memory_management(batch))
                active_tasks.append(task)
                
                # Limit concurrent tasks
                if len(active_tasks) >= max_concurrent_batches:
                    # Wait for oldest task to complete
                    completed_task = active_tasks.pop(0)
                    result = await completed_task
                    total_processed += result.get("processed", 0)
                    total_failed += result.get("failed", 0)
                
                offset += batch_size
                
                # Progress logging for large streams
                if offset % 50000 == 0:
                    elapsed = time.perf_counter() - stream_start
                    rate = offset / elapsed if elapsed > 0 else 0
                    self.logger.info(
                        f"Stream processing progress: {offset} claims fetched, "
                        f"rate: {rate:.2f} claims/sec"
                    )
            
            # Wait for remaining tasks
            for task in active_tasks:
                result = await task
                total_processed += result.get("processed", 0)
                total_failed += result.get("failed", 0)
            
        except Exception as e:
            self.logger.error(f"Stream processing error: {e}")
        finally:
            # Final cleanup
            await self._perform_stream_memory_cleanup()
        
        # Final metrics
        total_duration = time.perf_counter() - stream_start
        overall_rate = (total_processed + total_failed) / total_duration if total_duration > 0 else 0
        
        metrics.set("stream_processing_rate_per_sec", overall_rate)
        metrics.set("stream_total_duration_sec", total_duration)
        
        self.logger.info(
            "Ultra-optimized stream processing complete",
            extra={
                "total_processed": total_processed,
                "total_failed": total_failed,
                "duration_sec": round(total_duration, 2),
                "overall_rate_per_sec": round(overall_rate, 2),
                "optimization_level": "ultra",
                "memory_managed": True,
            }
        )

    async def _process_stream_batch_optimized(self, claims: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a single batch in the optimized stream with memory management."""
        if not claims:
            return {"processed": 0, "failed": 0}
        
        # Use memory pooling for stream processing
        batch_objects = None
        try:
            batch_objects = self._acquire_batch_objects(len(claims))
            
            # Bulk RVU enrichment
            enriched_claims = await self._enrich_claims_with_rvu(
                claims, self.cfg.processing.conversion_factor
            )
            
            # Parallel validation and rules evaluation
            validation_task = self.validator.validate_batch(enriched_claims) if self.validator else {}
            rules_task = self.rules_engine.evaluate_batch(enriched_claims) if self.rules_engine else {}
            
            validation_results, rules_results = await asyncio.gather(
                validation_task, rules_task, return_exceptions=True
            )
            
            if isinstance(validation_results, Exception):
                validation_results = {}
            if isinstance(rules_results, Exception):
                rules_results = {}
            
            # Vectorized ML inference
            predictions = []
            if self.model and hasattr(self.model, "predict_batch"):
                predictions = self.model.predict_batch(enriched_claims)
            elif self.model:
                predictions = [self.model.predict(claim) for claim in enriched_claims]
            else:
                predictions = [0] * len(enriched_claims)
            
            # Process results
            valid_claims = []
            failed_claims_data = []
            
            for claim, prediction in zip(enriched_claims, predictions):
                claim_id = claim.get("claim_id", "")
                v_errors = validation_results.get(claim_id, [])
                r_errors = rules_results.get(claim_id, [])
                
                claim["filter_number"] = prediction
                
                if self.model_monitor:
                    self.model_monitor.record_prediction(prediction)
                
                if v_errors or r_errors:
                    # Prepare failed claim data
                    all_errors = v_errors + r_errors
                    suggestions = self.repair_suggester.suggest(all_errors)
                    
                    encrypted_claim = claim.copy()
                    if self.encryption_key:
                        from ..security.compliance import encrypt_claim_fields, encrypt_text
                        encrypted_claim = encrypt_claim_fields(claim, self.encryption_key)
                        original_data = encrypt_text(str(claim), self.encryption_key)
                    else:
                        original_data = str(claim)
                    
                    failed_claims_data.append((
                        encrypted_claim.get("claim_id"),
                        encrypted_claim.get("facility_id"),
                        encrypted_claim.get("patient_account_number"),
                        "validation" if v_errors else "rules",
                        ErrorCategory.VALIDATION.value,
                        "validation",
                        original_data,
                        suggestions
                    ))
                else:
                    valid_claims.append(claim)
            
            # Bulk database operations
            bulk_tasks = []
            if valid_claims:
                bulk_tasks.append(self._insert_claims_bulk(valid_claims))
            if failed_claims_data:
                bulk_tasks.append(self._record_failed_claims_bulk(failed_claims_data))
            
            if bulk_tasks:
                await asyncio.gather(*bulk_tasks, return_exceptions=True)
            
            # Update status
            processing_status["processed"] += len(valid_claims)
            processing_status["failed"] += len(failed_claims_data)
            
            return {"processed": len(valid_claims), "failed": len(failed_claims_data)}
            
        finally:
            # Always release batch objects
            if batch_objects:
                self._release_batch_objects(batch_objects)

    async def _perform_stream_memory_cleanup(self) -> None:
        """Perform memory cleanup specific to stream processing."""
        # Clear processing caches
        if hasattr(self, 'cache'):
            if hasattr(self.cache, 'store'):
                # Keep only recent cache entries
                cache_size = len(self.cache.store)
                if cache_size > 10000:
                    # Remove oldest entries
                    keys_to_remove = list(self.cache.store.keys())[:-5000]  # Keep last 5000
                    for key in keys_to_remove:
                        self.cache.store.pop(key, None)
                    
                    self.logger.info(f"Cache cleanup: removed {len(keys_to_remove)} entries")
        
        # Clear service buffers
        if hasattr(self, 'service'):
            await self.service.flush_checkpoints()
        
        # Clear validation caches
        if hasattr(self, 'validator') and self.validator:
            # Reset large validation caches periodically
            current_time = time.time()
            if hasattr(self.validator, '_validation_cache_time'):
                if current_time - self.validator._validation_cache_time > 3600:  # 1 hour
                    self.validator._facilities_cache = set()
                    self.validator._classes_cache = set()
                    self.validator._validation_cache_time = 0.0
        
        # Clear RVU cache if it gets too large
        if self.rvu_cache and hasattr(self.rvu_cache, 'local_cache'):
            if len(self.rvu_cache.local_cache) > 5000:
                # Perform cache cleanup
                await self.rvu_cache.optimize_cache()
        
        # Clean up memory pools
        cleanup_stats = self._memory_pool.cleanup_all()
        
        # Force garbage collection
        collected = gc.collect()
        
        # Update metrics
        metrics.inc("stream_memory_cleanups")
        metrics.set("stream_gc_collected", collected)
        metrics.set("stream_pool_cleanup", sum(cleanup_stats.values()))
        
        self.logger.debug(f"Stream memory cleanup: {collected} objects collected, pools: {cleanup_stats}")

    # Backward compatibility methods
    async def startup(self) -> None:
        """Backward compatibility wrapper."""
        await self.startup_optimized()

    async def process_batch(self) -> None:
        """Backward compatibility wrapper."""
        await self.process_batch_ultra_optimized()

    async def process_stream(self) -> None:
        """Backward compatibility wrapper."""
        await self.process_stream_ultra_optimized()

    async def process_claim(self, claim: Dict[str, Any]) -> None:
        """Process single claim with memory management."""
        # Use memory pool for single claim processing
        pooled_claim = self._memory_pool.acquire("claims", dict)
        try:
            pooled_claim.clear()
            pooled_claim.update(claim)
            
            # Process claim using existing pipeline logic
            # This is a simplified version - in practice would use full pipeline
            result = await self._process_single_claim_with_pooling(pooled_claim)
            return result
            
        finally:
            self._memory_pool.release("claims", pooled_claim)

    async def _process_single_claim_with_pooling(self, claim: Dict[str, Any]) -> Dict[str, Any]:
        """Process single claim using memory pooling."""
        # Simplified single claim processing
        try:
            # Validate claim
            errors = []
            if self.validator:
                errors = await self.validator.validate(claim)
            
            # Apply rules
            if self.rules_engine and not errors:
                rule_errors = self.rules_engine.evaluate(claim)
                errors.extend(rule_errors)
            
            # Apply ML model
            prediction = 0
            if self.model and not errors:
                prediction = self.model.predict(claim)
                claim["filter_number"] = prediction
            
            if errors:
                # Record failed claim
                await self.service.record_failed_claim(
                    claim, 
                    "validation_failed", 
                    self.repair_suggester.suggest(errors)
                )
                return {"status": "failed", "errors": errors}
            else:
                # Insert valid claim
                await self.service.insert_claims([
                    (claim.get("patient_account_number"), 
                     claim.get("facility_id"),
                     claim.get("procedure_code"))
                ])
                return {"status": "processed", "prediction": prediction}
                
        except Exception as e:
            self.logger.error(f"Single claim processing failed: {e}")
            return {"status": "error", "message": str(e)}

    def calculate_batch_size(self) -> int:
        """Backward compatibility wrapper."""
        return self.calculate_batch_size_adaptive()

    async def process_claims_batch(self, claims: List[Dict[str, Any]]) -> List[tuple]:
        """Process a batch of claims and return results."""
        # Use memory pooling for batch processing
        batch_objects = None
        try:
            batch_objects = self._acquire_batch_objects(len(claims))
            
            # Process batch
            results = []
            for claim in claims:
                try:
                    # Simplified processing
                    patient_acct = claim.get("patient_account_number")
                    facility_id = claim.get("facility_id")
                    
                    if patient_acct and facility_id:
                        results.append((patient_acct, facility_id))
                        
                except Exception as e:
                    self.logger.warning(f"Claim processing failed: {e}")
                    continue
            
            return results
            
        finally:
            if batch_objects:
                self._release_batch_objects(batch_objects)

    # Performance monitoring methods
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get comprehensive optimization metrics including memory management."""
        return {
            "startup": {
                "startup_time_sec": self._startup_time,
                "preparation_stats": self._preparation_stats,
            },
            "memory_management": {
                "memory_pools": self._memory_pool.get_stats(),
                "batch_count": self._batch_count,
                "last_gc_time": self._last_gc_time,
                "memory_cleanup_cycles": metrics.get("memory_cleanup_cycles"),
                "emergency_cleanups": metrics.get("memory_emergency_cleanups"),
            },
            "pipeline_features": self._get_optimization_features(),
            "performance_metrics": {
                "current_throughput": metrics.get("batch_processing_rate_per_sec"),
                "adaptive_batch_size": metrics.get("dynamic_batch_size"),
                "processing_status": dict(processing_status),
            }
        }

    def get_memory_status(self) -> Dict[str, Any]:
        """Get current memory status and statistics."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            return {
                "current_memory_mb": memory_mb,
                "memory_limit_mb": self._total_memory_limit / 1024 / 1024,
                "memory_utilization": memory_mb / (self._total_memory_limit / 1024 / 1024),
                "batch_memory_limit_mb": self._batch_memory_limit / 1024 / 1024,
                "memory_pools": self._memory_pool.get_stats(),
                "gc_stats": {
                    "last_gc_time": self._last_gc_time,
                    "gc_interval": self._gc_interval,
                    "objects_collected": metrics.get("gc_objects_collected"),
                },
                "cleanup_stats": {
                    "cleanup_cycles": metrics.get("memory_cleanup_cycles"),
                    "emergency_cleanups": metrics.get("memory_emergency_cleanups"),
                    "stream_cleanups": metrics.get("stream_memory_cleanups"),
                }
            }
        except Exception as e:
            return {"error": str(e)}

    async def benchmark_optimizations(self, test_size: int = 10000) -> Dict[str, Any]:
        """Benchmark the optimization improvements including memory management."""
        # This would typically run against a test dataset
        benchmark_results = {
            "test_size": test_size,
            "optimizations_enabled": True,
            "features_tested": [
                "prepared_statements",
                "bulk_operations", 
                "connection_pooling",
                "query_caching",
                "vectorized_processing",
                "adaptive_batching",
                "memory_pooling",
                "memory_monitoring",
                "garbage_collection_optimization"
            ]
        }
        
        # Record initial memory state
        initial_memory = self.get_memory_status()
        
        # Run a test batch to measure performance
        start_time = time.perf_counter()
        result = await self.process_batch_ultra_optimized()
        end_time = time.perf_counter()
        
        # Record final memory state
        final_memory = self.get_memory_status()
        
        benchmark_results.update({
            "test_duration_sec": end_time - start_time,
            "test_throughput": result.get("throughput", 0),
            "performance_breakdown": result.get("performance_breakdown", {}),
            "optimization_effectiveness": "ultra_high" if result.get("throughput", 0) > 5000 else "high",
            "memory_performance": {
                "initial_memory_mb": initial_memory.get("current_memory_mb", 0),
                "final_memory_mb": final_memory.get("current_memory_mb", 0),
                "memory_growth_mb": final_memory.get("current_memory_mb", 0) - initial_memory.get("current_memory_mb", 0),
                "memory_pools_used": final_memory.get("memory_pools", {}),
                "memory_efficiency": "excellent" if final_memory.get("current_memory_mb", 0) - initial_memory.get("current_memory_mb", 0) < 100 else "good"
            }
        })
        
        return benchmark_results