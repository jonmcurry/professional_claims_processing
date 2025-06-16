import asyncio
import contextlib
import gc
import inspect
import json
import os
import time
import weakref
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Set

import psutil

from ..alerting import AlertManager, EmailNotifier
from ..analysis.error_pattern_detector import ErrorPatternDetector
from ..config.config import AppConfig
from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..models.ab_test import ABTestModel
from ..models.filter_model import FilterModel
from ..models.monitor import ModelMonitor
from ..monitoring import pool_monitor, resource_monitor
from ..monitoring.metrics import metrics, sla_monitor
from ..rules.engine import RulesEngine
from ..security.compliance import mask_claim_data
from ..services.claim_service import ClaimService
from ..utils.audit import record_audit_event
from ..utils.cache import DistributedCache, InMemoryCache, RvuCache
from ..utils.errors import ErrorCategory, categorize_exception
from ..utils.logging import RequestContextFilter, setup_logging
from ..utils.retries import retry_async
from ..utils.tracing import (correlation_id_var, start_span, start_trace,
                             trace_id_var)
from ..validation.validator import ClaimValidator
from ..web.status import batch_status, processing_status
from .repair import ClaimRepairSuggester, MLRepairAdvisor


class SystemResourceMonitor:
    """Monitor system resources for dynamic concurrency adjustment."""

    def __init__(self):
        self.cpu_percent = 0.0
        self.memory_percent = 0.0
        self.memory_available_gb = 0.0
        self.load_average = 0.0

    def update(self):
        """Update system resource metrics."""
        try:
            self.cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            self.memory_percent = memory.percent
            self.memory_available_gb = memory.available / (1024**3)

            try:
                self.load_average = os.getloadavg()[0]
            except (OSError, AttributeError):
                self.load_average = self.cpu_percent / 100.0 * psutil.cpu_count()

        except Exception:
            pass  # Keep previous values on error


class DynamicSemaphoreManager:
    """Manages dynamic semaphore limits based on system resources."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.resource_monitor = SystemResourceMonitor()

        # Initialize with enhanced limits instead of conservative defaults
        self.validation_limit = getattr(
            config.processing, "validation_concurrency", 100
        )
        self.ml_limit = getattr(config.processing, "ml_concurrency", 60)
        self.batch_limit = getattr(config.processing, "batch_concurrency", 12)
        self.database_limit = getattr(config.processing, "database_concurrency", 30)

        # Create semaphores
        self.validation_semaphore = asyncio.Semaphore(self.validation_limit)
        self.ml_semaphore = asyncio.Semaphore(self.ml_limit)
        self.batch_semaphore = asyncio.Semaphore(self.batch_limit)
        self.database_semaphore = asyncio.Semaphore(self.database_limit)

        # Adjustment parameters
        self.last_adjustment = time.time()
        self.adjustment_interval = 30  # seconds
        self.adjustment_count = 0

        # Limits
        self.min_limits = {"validation": 15, "ml": 8, "batch": 3, "database": 8}
        self.max_limits = {"validation": 300, "ml": 150, "batch": 25, "database": 80}

        # Emergency throttling
        self.emergency_active = False
        self.emergency_start_time = 0

    async def adjust_limits_dynamic(self):
        """Dynamically adjust semaphore limits based on system state."""
        current_time = time.time()
        if current_time - self.last_adjustment < self.adjustment_interval:
            return

        self.resource_monitor.update()

        # Calculate scaling factors
        cpu_factor = self._get_cpu_scaling_factor()
        memory_factor = self._get_memory_scaling_factor()
        load_factor = self._get_load_scaling_factor()

        # Combined scaling factor
        combined_factor = min(cpu_factor, memory_factor, load_factor)

        # Emergency throttling check
        if self._should_emergency_throttle():
            if not self.emergency_active:
                self.emergency_active = True
                self.emergency_start_time = current_time
                combined_factor *= 0.3  # Reduce to 30%
        elif self.emergency_active and current_time - self.emergency_start_time > 120:
            self.emergency_active = False

        # Calculate new limits
        new_validation = int(
            100 * combined_factor * 1.2
        )  # Validation is less intensive
        new_ml = int(60 * combined_factor * 0.8)  # ML is more intensive
        new_batch = int(12 * combined_factor)
        new_database = int(30 * combined_factor * 1.1)

        # Apply bounds
        new_validation = max(
            self.min_limits["validation"],
            min(self.max_limits["validation"], new_validation),
        )
        new_ml = max(self.min_limits["ml"], min(self.max_limits["ml"], new_ml))
        new_batch = max(
            self.min_limits["batch"], min(self.max_limits["batch"], new_batch)
        )
        new_database = max(
            self.min_limits["database"], min(self.max_limits["database"], new_database)
        )

        # Update limits if significant change
        if self._should_update_limits(new_validation, new_ml, new_batch, new_database):
            await self._update_semaphore_limits(
                new_validation, new_ml, new_batch, new_database
            )
            self.last_adjustment = current_time
            self.adjustment_count += 1

    def _get_cpu_scaling_factor(self) -> float:
        """Get scaling factor based on CPU usage."""
        cpu = self.resource_monitor.cpu_percent
        if cpu < 30:
            return 1.5
        elif cpu < 60:
            return 1.2
        elif cpu < 80:
            return 1.0
        elif cpu < 90:
            return 0.7
        else:
            return 0.4

    def _get_memory_scaling_factor(self) -> float:
        """Get scaling factor based on memory usage."""
        memory = self.resource_monitor.memory_percent
        available = self.resource_monitor.memory_available_gb

        if memory < 40:
            return 1.4
        elif memory < 70:
            return 1.1
        elif memory < 85:
            return 0.9
        else:
            return 0.6 if available > 1.0 else 0.3

    def _get_load_scaling_factor(self) -> float:
        """Get scaling factor based on load average."""
        load_ratio = self.resource_monitor.load_average / psutil.cpu_count()
        if load_ratio < 0.5:
            return 1.3
        elif load_ratio < 1.0:
            return 1.0
        elif load_ratio < 1.5:
            return 0.8
        else:
            return 0.4

    def _should_emergency_throttle(self) -> bool:
        """Check if emergency throttling should be activated."""
        return (
            self.resource_monitor.cpu_percent > 95
            or self.resource_monitor.memory_percent > 95
            or self.resource_monitor.memory_available_gb < 0.5
        )

    def _should_update_limits(self, new_val, new_ml, new_batch, new_db) -> bool:
        """Check if limits should be updated."""
        val_change = abs(new_val - self.validation_limit) / max(
            self.validation_limit, 1
        )
        ml_change = abs(new_ml - self.ml_limit) / max(self.ml_limit, 1)
        batch_change = abs(new_batch - self.batch_limit) / max(self.batch_limit, 1)
        db_change = abs(new_db - self.database_limit) / max(self.database_limit, 1)

        return (
            val_change > 0.15
            or ml_change > 0.15
            or batch_change > 0.15
            or db_change > 0.15
        )

    async def _update_semaphore_limits(self, new_val, new_ml, new_batch, new_db):
        """Update semaphore limits."""
        # Store old limits for logging
        old_limits = (
            self.validation_limit,
            self.ml_limit,
            self.batch_limit,
            self.database_limit,
        )

        # Update limits
        self.validation_limit = new_val
        self.ml_limit = new_ml
        self.batch_limit = new_batch
        self.database_limit = new_db

        # Create new semaphores (asyncio.Semaphore doesn't support dynamic limits)
        self.validation_semaphore = asyncio.Semaphore(new_val)
        self.ml_semaphore = asyncio.Semaphore(new_ml)
        self.batch_semaphore = asyncio.Semaphore(new_batch)
        self.database_semaphore = asyncio.Semaphore(new_db)

        # Update metrics
        metrics.set("dynamic_validation_limit", new_val)
        metrics.set("dynamic_ml_limit", new_ml)
        metrics.set("dynamic_batch_limit", new_batch)
        metrics.set("dynamic_database_limit", new_db)
        metrics.set("system_cpu_percent", self.resource_monitor.cpu_percent)
        metrics.set("system_memory_percent", self.resource_monitor.memory_percent)
        metrics.set("emergency_throttling_active", 1 if self.emergency_active else 0)

        # Log the change
        print(
            f"Dynamic concurrency adjustment #{self.adjustment_count}: "
            f"Validation: {old_limits[0]} → {new_val}, "
            f"ML: {old_limits[1]} → {new_ml}, "
            f"Batch: {old_limits[2]} → {new_batch}, "
            f"Database: {old_limits[3]} → {new_db} "
            f"(CPU: {self.resource_monitor.cpu_percent:.1f}%, "
            f"Memory: {self.resource_monitor.memory_percent:.1f}%)"
        )


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
                if hasattr(obj, "clear"):
                    obj.clear()
                elif hasattr(obj, "__dict__"):
                    # Reset object state
                    for attr in list(obj.__dict__.keys()):
                        if not attr.startswith("_"):
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

    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get pool statistics."""
        return self.stats.copy()


class ClaimsPipeline:
    """Ultra-optimized claims processing pipeline with dynamic concurrency management."""

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
        self.rules_engine: RulesEngine | None = None
        self.validator: ClaimValidator | None = None

        # Enhanced caching with optimization
        self.cache = InMemoryCache()
        self.rvu_cache: RvuCache | None = None
        self.distributed_cache: DistributedCache | None = None

        # Processing utilities
        self.repair_advisor = MLRepairAdvisor()
        self.repair_suggester = ClaimRepairSuggester(self.repair_advisor)
        self.error_detector = ErrorPatternDetector(
            self.sql, cfg.monitoring.failure_pattern_threshold
        )

        # Use service
        self.service = ClaimService(
            self.pg,
            self.sql,
            self.encryption_key,
            checkpoint_buffer_size=5000,  # Larger buffer for better performance
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

        # Dynamic concurrency management
        self.semaphore_manager = DynamicSemaphoreManager(cfg)
        self._concurrency_monitor_task: Optional[asyncio.Task] = None
        # Background monitoring tasks
        self._memory_monitor_task: Optional[asyncio.Task] = None

        # Service health monitoring
        self.mode = "normal"
        self.health_state = {"postgres": True, "sqlserver": True, "redis": True}
        self.local_queue_path = Path("claim_backup_queue.jsonl")
        from ..maintenance import RecoveryManager

        self.recovery_manager = RecoveryManager(self, cfg)

    async def _run_error_detection_check(self) -> None:
        if self.error_detector:
            self.error_detector.db = self.sql
            await self.error_detector.maybe_check()

    async def startup_optimized(self) -> None:
        """Ultra-optimized startup with dynamic concurrency management."""
        startup_start = time.perf_counter()

        # Phase 1: Database Connection and Pre-warming
        connection_start = time.perf_counter()
        await asyncio.gather(self.pg.connect(), self.sql.connect())

        # Enhanced warmup with proper health checks
        async def pg_health_check():
            """PostgreSQL health check wrapper."""
            try:
                if hasattr(self.pg, 'health_check'):
                    return await self.pg.health_check()
                else:
                    # Fallback health check if method doesn't exist
                    result = await self.pg.fetch("SELECT 1 as health_check")
                    return len(result) > 0 and result[0].get('health_check') == 1
            except Exception as e:
                self.logger.error(f"PostgreSQL health check failed: {e}")
                return False

        async def sql_health_check():
            """SQL Server health check wrapper."""
            try:
                return await self.sql.health_check()
            except Exception as e:
                self.logger.error(f"SQL Server health check failed: {e}")
                return False

        # Aggressive connection pool pre-warming with enhanced health checks
        warmup_tasks = [
            self.pg.fetch("SELECT 1 as warmup_test"),
            self.sql.execute("SELECT 1"),
            # Enhanced health checks with error handling
            pg_health_check(),
            sql_health_check(),
        ]
        
        warmup_results = await asyncio.gather(*warmup_tasks, return_exceptions=True)
        
        # Process health check results
        pg_warmup_ok = not isinstance(warmup_results[0], Exception)
        sql_warmup_ok = not isinstance(warmup_results[1], Exception)
        pg_health_ok = warmup_results[2] if not isinstance(warmup_results[2], Exception) else False
        sql_health_ok = warmup_results[3] if not isinstance(warmup_results[3], Exception) else False
        
        # Log health check results
        self.logger.info(
            f"Database warmup complete - PG warmup: {pg_warmup_ok}, "
            f"SQL warmup: {sql_warmup_ok}, PG health: {pg_health_ok}, SQL health: {sql_health_ok}"
        )
        
        # Handle unhealthy connections
        if not pg_health_ok:
            self.logger.warning("PostgreSQL health check failed, attempting reconnection...")
            try:
                await self.pg.connect()
                # Verify reconnection
                await self.pg.fetch("SELECT 1")
                self.logger.info("PostgreSQL reconnection successful")
            except Exception as e:
                self.logger.error(f"PostgreSQL reconnection failed: {e}")
                # Continue anyway, but log the issue
        
        if not sql_health_ok:
            self.logger.warning("SQL Server health check failed, attempting reconnection...")
            try:
                await self.sql.connect()
                # Verify reconnection
                await self.sql.execute("SELECT 1")
                self.logger.info("SQL Server reconnection successful")
            except Exception as e:
                self.logger.error(f"SQL Server reconnection failed: {e}")
                # Continue anyway, but log the issue

        connection_time = time.perf_counter() - connection_start
        self._preparation_stats["connection_time"] = connection_time
        self._preparation_stats["pg_health_ok"] = pg_health_ok
        self._preparation_stats["sql_health_ok"] = sql_health_ok

        # Phase 2: Prepared Statement Setup
        preparation_start = time.perf_counter()

        # Prepare critical statements in parallel
        preparation_tasks = [
            # SQL Server preparations
            self.sql.prepare_named(
                "claims_insert_optimized",
                "INSERT INTO claims (patient_account_number, facility_id, procedure_code, created_at, updated_at) VALUES (?, ?, ?, GETDATE(), GETDATE())",
            ),
            self.sql.prepare_named(
                "failed_claims_insert_batch",
                """INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
            ),
            # PostgreSQL preparations
            self.pg.prepare_named(
                "claims_fetch_optimized",
                """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                        li.units AS li_units, li.charge_amount AS li_charge_amount, 
                        li.service_from_date AS li_service_from_date, 
                        li.service_to_date AS li_service_to_date 
                FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                ORDER BY c.priority DESC LIMIT $1 OFFSET $2""",
            ),
            self.pg.prepare_named(
                "rvu_bulk_fetch",
                """SELECT procedure_code, description, total_rvu, work_rvu, 
                        practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = ANY($1) AND status = 'active'""",
            ),
            self.pg.prepare_named(
                "checkpoint_insert_optimized",
                "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
            ),
        ]

        preparation_results = await asyncio.gather(*preparation_tasks, return_exceptions=True)
        
        # Log preparation results
        preparation_success = sum(1 for r in preparation_results if not isinstance(r, Exception))
        preparation_total = len(preparation_results)
        self.logger.info(f"Prepared statements: {preparation_success}/{preparation_total} successful")
        
        # Log any preparation failures
        for i, result in enumerate(preparation_results):
            if isinstance(result, Exception):
                task_names = [
                    "claims_insert_optimized", "failed_claims_insert_batch", 
                    "claims_fetch_optimized", "rvu_bulk_fetch", "checkpoint_insert_optimized"
                ]
                self.logger.error(f"Failed to prepare {task_names[i]}: {result}")

        preparation_time = time.perf_counter() - preparation_start
        self._preparation_stats["preparation_time"] = preparation_time
        self._preparation_stats["preparation_success_rate"] = preparation_success / preparation_total

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
            try:
                self.model = FilterModel(self.cfg.model.path, self.cfg.model.version)
                self.logger.info(f"✓ ML model loaded: {self.cfg.model.path}")
            except FileNotFoundError:
                self.logger.warning(f"ML model not found: {self.cfg.model.path}")
                self.logger.info("System will use rules-based processing until ML model is available")
                self.model = None

        if self.model and self.features.enable_model_monitor:
            self.model_monitor = ModelMonitor(self.cfg.model.version)
        else:
            self.model_monitor = None

        # Initialize rules engine
        self.rules_engine = RulesEngine([])

        # Initialize distributed caching
        if self.features.enable_cache and self.cfg.cache.redis_url:
            self.distributed_cache = DistributedCache(self.cfg.cache.redis_url)

        # Initialize RVU cache with optimizations
        self.rvu_cache = RvuCache(
            self.pg,
            distributed=(
                self.distributed_cache if self.features.enable_cache else None
            ),
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

        # Start resource monitoring and alerting
        notifier = EmailNotifier()
        alert_manager = AlertManager(self.cfg, notifier)
        resource_monitor.start(
            interval=self.cfg.monitoring.resource_check_interval,
            log_interval=self.cfg.monitoring.resource_log_interval,
            alert_manager=alert_manager,
        )
        pool_monitor.start(self.pg, self.sql, interval=2.0)

        # Start memory monitoring
        self._memory_monitor_task = asyncio.create_task(
            self._memory_monitor_loop()
        )

        # Start dynamic concurrency monitoring
        self._concurrency_monitor_task = asyncio.create_task(
            self._concurrency_monitor_loop()
        )

        # Start dependency recovery monitoring
        self.recovery_manager.start()

        monitoring_time = time.perf_counter() - monitoring_start
        self._preparation_stats["monitoring_time"] = monitoring_time

        # Record startup metrics
        self._startup_time = time.perf_counter() - startup_start
        metrics.set("pipeline_startup_time", self._startup_time)
        metrics.set("pipeline_startup_optimized", 1.0)
        metrics.set("dynamic_concurrency_enabled", 1.0)
        metrics.set("pg_health_status", 1.0 if pg_health_ok else 0.0)
        metrics.set("sql_health_status", 1.0 if sql_health_ok else 0.0)

        self.logger.info(
            "Optimized pipeline startup complete with dynamic concurrency",
            extra={
                "startup_time": round(self._startup_time, 4),
                "preparation_stats": self._preparation_stats,
                "optimization_features": self._get_optimization_features(),
                "initial_concurrency_limits": {
                    "validation": self.semaphore_manager.validation_limit,
                    "ml": self.semaphore_manager.ml_limit,
                    "batch": self.semaphore_manager.batch_limit,
                    "database": self.semaphore_manager.database_limit,
                },
                "database_health": {
                    "postgresql": pg_health_ok,
                    "sql_server": sql_health_ok,
                },
            },
        )

    async def _concurrency_monitor_loop(self):
        """Background task to monitor and adjust concurrency limits."""
        while True:
            await asyncio.sleep(15)  # Check every 15 seconds
            await self._run_safely(
                self.semaphore_manager.adjust_limits_dynamic(),
                "Concurrency monitor error",
                stage="concurrency_monitor",
            )

    def _initialize_memory_pools(self) -> None:
        """Initialize memory pools for frequently used objects."""
        # Pool for claim dictionaries
        self._memory_pool.get_pool("claims", lambda: {})

        # Pool for validation result lists
        self._memory_pool.get_pool("validation_results", lambda: [])

        # Pool for processing results
        self._memory_pool.get_pool(
            "processing_results", lambda: {"processed": [], "failed": []}
        )

        # Pool for checkpoint data
        self._memory_pool.get_pool("checkpoints", lambda: [])

        # Pool for RVU data containers
        self._memory_pool.get_pool("rvu_containers", lambda: {})

    async def _memory_monitor_loop(self) -> None:
        """Background memory monitoring and cleanup."""
        while True:
            await asyncio.sleep(30)  # Check every 30 seconds
            await self._run_safely(
                self._perform_memory_cleanup(),
                "Memory monitor error",
                stage="memory_monitor",
            )

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
        if (
            current_time - self._last_gc_time > self._gc_interval
            or memory_mb > self._total_memory_limit / (1024 * 1024)
        ):
            collected = gc.collect()
            self._last_gc_time = current_time

            # Update metrics
            metrics.set("memory_usage_mb", memory_mb)
            metrics.set("gc_objects_collected", collected)
            metrics.inc("memory_cleanup_cycles")

            self.logger.debug(
                f"Memory cleanup: {collected} objects collected, {memory_mb:.1f}MB used"
            )

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
            "dynamic_concurrency": True,
            "emergency_throttling": True,
        }

    def _acquire_batch_objects(self, batch_size: int) -> Dict[str, Any]:
        """Acquire batch processing objects from memory pool."""
        return {
            "claims": [
                self._memory_pool.acquire("claims", dict)
                for _ in range(min(batch_size, 1000))
            ],
            "validation_results": self._memory_pool.acquire("validation_results", list),
            "processing_results": self._memory_pool.acquire(
                "processing_results", lambda: {"processed": [], "failed": []}
            ),
            "checkpoints": self._memory_pool.acquire("checkpoints", list),
            "rvu_containers": [
                self._memory_pool.acquire("rvu_containers", dict)
                for _ in range(min(batch_size, 1000))
            ],
        }

    def _release_batch_objects(self, batch_objects: Dict[str, Any]) -> None:
        """Return batch processing objects to memory pool."""
        # Release claim objects
        for claim_obj in batch_objects.get("claims", []):
            self._memory_pool.release("claims", claim_obj)

        # Release other objects
        if "validation_results" in batch_objects:
            self._memory_pool.release(
                "validation_results", batch_objects["validation_results"]
            )

        if "processing_results" in batch_objects:
            self._memory_pool.release(
                "processing_results", batch_objects["processing_results"]
            )

        if "checkpoints" in batch_objects:
            self._memory_pool.release("checkpoints", batch_objects["checkpoints"])

        # Release RVU containers
        for rvu_obj in batch_objects.get("rvu_containers", []):
            self._memory_pool.release("rvu_containers", rvu_obj)

    async def _run_safely(
        self,
        coro: Awaitable[Any],
        message: str,
        default: Any = None,
        *,
        claim_id: str | None = None,
        stage: str | None = None,
    ) -> Any:
        """Run a coroutine and log any exceptions uniformly."""
        try:
            return await coro
        except Exception as e:
            category = categorize_exception(e)
            self.logger.exception(
                f"{message}: {e}",
                extra={
                    "claim_id": claim_id,
                    "processing_stage": stage,
                    "correlation_id": trace_id_var.get(""),
                },
            )
            metrics.inc(f"errors_{category.value}")
            return default

    async def _check_services_health(self) -> bool:
        """Check dependent service health and update mode."""
        pg_ok = (
            await self.pg.health_check() if hasattr(self.pg, "health_check") else True
        )
        sql_ok = (
            await self.sql.health_check() if hasattr(self.sql, "health_check") else True
        )
        redis_ok = (
            await self.distributed_cache.health_check()
            if self.distributed_cache
            else True
        )

        if not hasattr(self, "health_state"):
            self.health_state = {
                "postgres": pg_ok,
                "sqlserver": sql_ok,
                "redis": redis_ok,
            }
        else:
            self.health_state.update(
                {"postgres": pg_ok, "sqlserver": sql_ok, "redis": redis_ok}
            )

        metrics.set("postgres_available", 1.0 if pg_ok else 0.0)
        metrics.set("sqlserver_available", 1.0 if sql_ok else 0.0)
        metrics.set("redis_available", 1.0 if redis_ok else 0.0)

        all_ok = pg_ok and sql_ok and redis_ok
        if not all_ok and self.mode != "backup":
            self.mode = "backup"
            metrics.set("pipeline_mode_backup", 1.0)
            self.logger.error("Dependencies unavailable - switching to backup mode")
        elif all_ok and self.mode == "backup":
            self.mode = "normal"
            metrics.set("pipeline_mode_backup", 0.0)
            self.logger.info("Dependencies healthy - resuming normal mode")
        return all_ok

    async def _queue_claims_locally(self, claims: List[Dict[str, Any]]) -> None:
        """Append claims to a local disk queue."""
        if not claims:
            return
        with self.local_queue_path.open("a", encoding="utf-8") as fh:
            for claim in claims:
                fh.write(json.dumps(claim) + "\n")
        metrics.inc("claims_queued_backup", len(claims))

    async def process_batch_ultra_optimized(self) -> Dict[str, Any]:
        """Ultra-optimized batch processing with dynamic concurrency management."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)

        await self._check_services_health()
        if self.mode == "backup":
            self.logger.warning("Batch skipped due to backup mode")
            return {}

        trace_id = start_trace()
        batch_start = time.perf_counter()
        self._batch_count += 1

        # Memory check
        if self._batch_count % self._memory_check_interval == 0:
            await self._check_memory_usage()

        # Initialize batch tracking
        batch_id = f"opt_batch_{int(batch_start * 1000)}"
        correlation_id_var.set(batch_id)
        batch_status["batch_id"] = batch_id
        batch_status["start_time"] = batch_start
        batch_status["end_time"] = None

        # Use batch semaphore for overall batch processing concurrency
        async with self.semaphore_manager.batch_semaphore:
            # Phase 1: Dead Letter Queue Processing (async)
            dead_letter_task = asyncio.create_task(
                self.service.reprocess_dead_letter_optimized(batch_size=2000)
            )

            # Phase 2: Dynamic Batch Size Calculation
            base_batch_size = self.calculate_batch_size_adaptive()
            # Scale up for ultra-optimization but consider memory
            optimized_batch_size = await self._calculate_memory_aware_batch_size(
                base_batch_size
            )

            metrics.set("dynamic_batch_size", optimized_batch_size)

            # Acquire batch objects from pool
            batch_objects = None
            try:
                batch_objects = self._acquire_batch_objects(optimized_batch_size)
                failed_claims_data = []  # Initialize early to track all failures
                valid_claims = []  # Initialize early

                # Phase 3: Optimized Claims Fetching with database semaphore
                fetch_start = time.perf_counter()
                async with self.semaphore_manager.database_semaphore:
                    claims = await self.service.fetch_claims(
                        optimized_batch_size, priority=True
                    )
                fetch_time = time.perf_counter() - fetch_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Fetch stage complete",
                    extra={
                        "event": "fetch_complete",
                        "performance_breakdown": {
                            "fetch_ms": round(fetch_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

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
                    self._record_checkpoints_bulk([(cid, "start") for cid in claim_ids])
                )

                # Bulk RVU enrichment
                enriched_claims = await self._enrich_claims_with_rvu(
                    claims, self.cfg.processing.conversion_factor
                )

                preprocess_time = time.perf_counter() - preprocess_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Preprocess stage complete",
                    extra={
                        "event": "preprocess_complete",
                        "performance_breakdown": {
                            "preprocess_ms": round(preprocess_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

                # Phase 5: Vectorized Validation and Rules with dynamic concurrency
                validation_start = time.perf_counter()

                # Use validation semaphore for validation operations
                validation_task = None
                if self.validator:
                    async with self.semaphore_manager.validation_semaphore:
                        validation_task = self.validator.validate_batch(enriched_claims)

                # Rules engine typically doesn't need heavy concurrency control
                rules_task = None
                if self.rules_engine:
                    rules_task = self.rules_engine.evaluate_batch_async(enriched_claims)

                # Wait for validation and rules
                validation_results = {}
                rules_results = {}

                if validation_task:
                    try:
                        validation_results = await validation_task
                    except Exception as e:
                        self.logger.exception(
                            f"Validation error: {e}",
                            extra={
                                "processing_stage": "validation",
                                "correlation_id": trace_id_var.get(""),
                            },
                        )
                        validation_results = {}

                if rules_task:
                    try:
                        rules_results = await rules_task
                    except Exception as e:
                        self.logger.exception(
                            f"Rules evaluation error: {e}",
                            extra={
                                "processing_stage": "rules",
                                "correlation_id": trace_id_var.get(""),
                            },
                        )
                        rules_results = {}

                validation_time = time.perf_counter() - validation_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Validation stage complete",
                    extra={
                        "event": "validation_complete",
                        "performance_breakdown": {
                            "validation_ms": round(validation_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

                # Phase 6: Vectorized ML Inference with dynamic concurrency
                ml_start = time.perf_counter()

                predictions: List[int] = []
                if self.model:
                    # Use ML semaphore for model predictions
                    async with self.semaphore_manager.ml_semaphore:
                        if hasattr(self.model, "predict_batch"):
                            predictions = self.model.predict_batch(enriched_claims)
                        else:
                            # Individual predictions with controlled concurrency
                            prediction_tasks = [
                                asyncio.create_task(self._predict_single_async(claim))
                                for claim in enriched_claims
                            ]
                            predictions = await asyncio.gather(*prediction_tasks)
                else:
                    predictions = [0] * len(enriched_claims)

                ml_time = time.perf_counter() - ml_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "ML stage complete",
                    extra={
                        "event": "ml_complete",
                        "performance_breakdown": {
                            "ml_inference_ms": round(ml_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

                # Phase 7: Results Processing and Segregation
                segregation_start = time.perf_counter()

                for i, (claim, prediction) in enumerate(
                    zip(enriched_claims, predictions)
                ):
                    claim_id = claim.get("claim_id", "")
                    correlation_id_var.set(claim.get("correlation_id", ""))
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
                        suggestions = self.repair_suggester.suggest(all_errors, claim)

                        encrypted_claim = claim.copy()
                        if self.encryption_key:
                            from ..security.compliance import (
                                encrypt_claim_fields, encrypt_text)

                            encrypted_claim = encrypt_claim_fields(
                                claim, self.encryption_key
                            )
                            original_data = encrypt_text(
                                str(claim), self.encryption_key
                            )
                        else:
                            original_data = str(claim)

                        failed_claims_data.append(
                            (
                                encrypted_claim.get("claim_id"),
                                encrypted_claim.get("facility_id"),
                                encrypted_claim.get("patient_account_number"),
                                "validation" if v_errors else "rules",
                                ErrorCategory.VALIDATION.value,
                                "validation",
                                original_data,
                                suggestions,
                            )
                        )
                    else:
                        valid_claims.append(claim)

                segregation_time = time.perf_counter() - segregation_start

                # Phase 8: Bulk Database Operations with dynamic concurrency
                db_start = time.perf_counter()

                # Use database semaphore for database operations
                async with self.semaphore_manager.database_semaphore:
                    # Parallel bulk operations
                    bulk_tasks = []

                    # Bulk insert valid claims
                    if valid_claims:
                        bulk_tasks.append(self._insert_claims_bulk(valid_claims))

                    # Bulk insert failed claims
                    if failed_claims_data:
                        bulk_tasks.append(
                            self._record_failed_claims_bulk(failed_claims_data)
                        )

                    # Execute bulk operations
                    if bulk_tasks:
                        bulk_results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
                        
                        # Check for exceptions and handle failed operations
                        record_failed_claims_failed = False
                        for i, result in enumerate(bulk_results):
                            if isinstance(result, Exception):
                                task_name = "insert_claims" if i == 0 else "record_failed_claims"
                                self.logger.error(
                                    f"Bulk {task_name} operation failed: {result}",
                                    extra={
                                        "processing_stage": "database_bulk",
                                        "correlation_id": trace_id_var.get(""),
                                        "error_type": type(result).__name__
                                    }
                                )
                                
                                # Track if recording failed claims failed
                                if i == 1:  # record_failed_claims task
                                    record_failed_claims_failed = True
                        
                        # If recording failed claims failed, try to update their status anyway
                        if record_failed_claims_failed and failed_claims_data:
                            try:
                                failed_claim_ids = [f[0] for f in failed_claims_data if f[0]]
                                if failed_claim_ids:
                                    self.logger.info(f"Fallback: updating status for {len(failed_claim_ids)} failed claims")
                                    await self._update_claims_status_bulk(failed_claim_ids, "failed", "failed")
                            except Exception as fallback_error:
                                self.logger.error(f"Fallback status update failed: {fallback_error}")

                db_time = time.perf_counter() - db_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Database stage complete",
                    extra={
                        "event": "database_complete",
                        "performance_breakdown": {
                            "database_ms": round(db_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

                # Phase 8b: Update Claims Status  
                # Update final status for successful and failed claims
                if valid_claims:
                    valid_claim_ids = [c.get("claim_id", "") for c in valid_claims if c.get("claim_id")]
                    await self._update_claims_status_bulk(valid_claim_ids, "completed", "completed")
                
                if failed_claims_data:
                    failed_claim_ids = [f[0] for f in failed_claims_data if f[0]]  # claim_id is first element
                    await self._update_claims_status_bulk(failed_claim_ids, "failed", "failed")

                # Phase 9: Bulk Checkpointing and Audit
                checkpoint_start = time.perf_counter()

                # Prepare checkpoint data
                valid_claim_ids = [c.get("claim_id", "") for c in valid_claims]
                failed_claim_ids = [
                    f[0] for f in failed_claims_data
                ]  # claim_id is first element

                checkpoint_tasks = []
                if valid_claim_ids:
                    checkpoint_tasks.append(
                        self._record_checkpoints_bulk(
                            [(cid, "completed") for cid in valid_claim_ids]
                        )
                    )
                if failed_claim_ids:
                    checkpoint_tasks.append(
                        self._record_checkpoints_bulk(
                            [(cid, "failed") for cid in failed_claim_ids]
                        )
                    )

                if checkpoint_tasks:
                    checkpoint_results = await asyncio.gather(*checkpoint_tasks, return_exceptions=True)
                    
                    # Check for exceptions in checkpoint operations
                    for i, result in enumerate(checkpoint_results):
                        if isinstance(result, Exception):
                            self.logger.warning(
                                f"Checkpoint operation {i} failed: {result}",
                                extra={
                                    "processing_stage": "checkpoint",
                                    "correlation_id": trace_id_var.get(""),
                                    "error_type": type(result).__name__
                                }
                            )

                # Ensure start checkpoint task completes
                await checkpoint_task

                checkpoint_time = time.perf_counter() - checkpoint_start
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Checkpoint stage complete",
                    extra={
                        "event": "checkpoint_complete",
                        "performance_breakdown": {
                            "checkpoint_ms": round(checkpoint_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

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
                throughput = (
                    len(enriched_claims) / total_duration if total_duration > 0 else 0
                )

                # Revenue and hourly metrics
                revenue = sum(
                    float(c.get("reimbursement_amount", 0.0) or 0.0)
                    for c in valid_claims
                )
                metrics.record_hourly("claims_processed", len(valid_claims))
                metrics.record_hourly("revenue_impact", revenue)
                metrics.inc("revenue_impact", revenue)
                sla_monitor.record_batch(total_duration, len(enriched_claims))

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
                self.semaphore_manager.resource_monitor.update()
                self.logger.info(
                    "Metrics stage complete",
                    extra={
                        "event": "metrics_complete",
                        "performance_breakdown": {
                            "metrics_ms": round(metrics_time * 1000, 2)
                        },
                        "system_resources": {
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                        },
                    },
                )

                # Wait for dead letter processing to complete
                try:
                    dlq_processed = await dead_letter_task
                    self.logger.info(
                        "Dead letter processing complete",
                        extra={
                            "processed": dlq_processed,
                            "processing_stage": "dead_letter",
                            "correlation_id": trace_id_var.get(""),
                        },
                    )
                except Exception as e:
                    self.logger.exception(
                        f"Dead letter processing failed: {e}",
                        extra={
                            "processing_stage": "dead_letter",
                            "correlation_id": trace_id_var.get(""),
                        },
                    )

                # Final status update
                batch_status["end_time"] = time.perf_counter()

                # Comprehensive logging
                self.logger.info(
                    "Ultra-optimized batch complete with dynamic concurrency",
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
                        "concurrency_limits": {
                            "validation": self.semaphore_manager.validation_limit,
                            "ml": self.semaphore_manager.ml_limit,
                            "batch": self.semaphore_manager.batch_limit,
                            "database": self.semaphore_manager.database_limit,
                        },
                        "system_resources": {
                            "cpu_percent": self.semaphore_manager.resource_monitor.cpu_percent,
                            "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                            "emergency_throttling": self.semaphore_manager.emergency_active,
                        },
                        "optimization_efficiency": {
                            "memory_pooled": True,
                            "vectorized_processing": True,
                            "bulk_database_ops": True,
                            "parallel_execution": True,
                            "prepared_statements": True,
                            "connection_reuse": True,
                            "dynamic_concurrency": True,
                        },
                        "trace_id": trace_id,
                    },
                )

                await self._run_error_detection_check()

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
                    },
                    "concurrency_stats": {
                        "validation_limit": self.semaphore_manager.validation_limit,
                        "ml_limit": self.semaphore_manager.ml_limit,
                        "batch_limit": self.semaphore_manager.batch_limit,
                        "database_limit": self.semaphore_manager.database_limit,
                        "adjustments_made": self.semaphore_manager.adjustment_count,
                    },
                }

            except Exception as process_error:
                # Handle catastrophic processing failures
                self.logger.error(
                    f"Critical processing error in batch: {process_error}",
                    extra={
                        "processing_stage": "batch_processing",
                        "correlation_id": trace_id_var.get(""),
                        "error_type": type(process_error).__name__,
                        "batch_size": optimized_batch_size
                    }
                )
                
                # If we have claims that were fetched but failed processing, mark them as failed
                try:
                    if 'claims' in locals() and claims and len(claims) > 0:
                        # All claims in this batch failed due to processing error
                        failed_claim_ids = [c.get("claim_id", "") for c in claims if c.get("claim_id")]
                        if failed_claim_ids:
                            self.logger.info(f"Marking {len(failed_claim_ids)} claims as failed due to processing error")
                            await self._update_claims_status_bulk(failed_claim_ids, "failed", "processing_error")
                except Exception as status_update_error:
                    self.logger.error(f"Failed to update status for failed claims: {status_update_error}")
                
                # Return failure metrics
                return {
                    "processed": 0,
                    "failed": len(claims) if 'claims' in locals() and claims else 0,
                    "duration": time.perf_counter() - total_start,
                    "throughput": 0,
                    "error": str(process_error)
                }

            finally:
                # Always release objects back to pool
                if batch_objects:
                    self._release_batch_objects(batch_objects)

                # Periodic cleanup
                if self._batch_count % 50 == 0:  # Every 50 batches
                    asyncio.create_task(self._perform_memory_cleanup())

    async def process_stream_ultra_optimized(self) -> None:
        """Ultra-optimized stream processing with dynamic concurrency management."""
        if not self.request_filter:
            self.request_filter = RequestContextFilter()
            self.logger.addFilter(self.request_filter)

        start_trace()
        stream_start = time.perf_counter()
        correlation_id_var.set(f"stream_{int(stream_start * 1000)}")

        # Memory management for long-running processes
        last_cleanup = stream_start
        cleanup_interval = 300  # 5 minutes

        # Large batch sizes for streaming optimization but memory-aware
        base_batch_size = self.calculate_batch_size_adaptive()
        batch_size = await self._calculate_memory_aware_batch_size(base_batch_size * 2)

        total_processed = 0
        total_failed = 0

        # Use batch semaphore for overall stream concurrency
        async with self.semaphore_manager.batch_semaphore:
            try:
                # Process stream in batches
                offset = 0
                active_tasks = []
                max_concurrent_batches = self.semaphore_manager.batch_limit

                while True:
                    current_time = time.time()

                    # Periodic memory cleanup for long-running processes
                    if current_time - last_cleanup > cleanup_interval:
                        await self._perform_stream_memory_cleanup()
                        last_cleanup = current_time

                    # Fetch next batch with database semaphore
                    fetch_start = time.perf_counter()
                    async with self.semaphore_manager.database_semaphore:
                        batch = await self.service.fetch_claims(
                            batch_size, offset=offset, priority=True
                        )
                    fetch_time = time.perf_counter() - fetch_start
                    self.semaphore_manager.resource_monitor.update()
                    self.logger.info(
                        "Fetch stage complete",
                        extra={
                            "event": "fetch_complete",
                            "performance_breakdown": {
                                "fetch_ms": round(fetch_time * 1000, 2)
                            },
                            "system_resources": {
                                "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                                "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                            },
                        },
                    )

                    if not batch:
                        break

                    # Create processing task
                    task = asyncio.create_task(
                        self._process_stream_batch_optimized(batch)
                    )
                    active_tasks.append(task)

                    # Limit concurrent tasks based on dynamic limits
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
                            f"rate: {rate:.2f} claims/sec, "
                            f"concurrency limits: V:{self.semaphore_manager.validation_limit} "
                            f"ML:{self.semaphore_manager.ml_limit} "
                            f"DB:{self.semaphore_manager.database_limit}"
                        )

                # Wait for remaining tasks
                for task in active_tasks:
                    result = await task
                    total_processed += result.get("processed", 0)
                    total_failed += result.get("failed", 0)

            except Exception as e:
                self.logger.exception(
                    f"Stream processing error: {e}",
                    extra={
                        "processing_stage": "stream_processing",
                        "correlation_id": trace_id_var.get(""),
                    },
                )
            finally:
                # Final cleanup
                await self._perform_stream_memory_cleanup()

        # Final metrics
        total_duration = time.perf_counter() - stream_start
        overall_rate = (
            (total_processed + total_failed) / total_duration
            if total_duration > 0
            else 0
        )

        metrics.set("stream_processing_rate_per_sec", overall_rate)
        metrics.set("stream_total_duration_sec", total_duration)

        self.logger.info(
            "Ultra-optimized stream processing complete with dynamic concurrency",
            extra={
                "total_processed": total_processed,
                "total_failed": total_failed,
                "duration_sec": round(total_duration, 2),
                "overall_rate_per_sec": round(overall_rate, 2),
                "optimization_level": "ultra",
                "memory_managed": True,
                "dynamic_concurrency": True,
                "final_concurrency_limits": {
                    "validation": self.semaphore_manager.validation_limit,
                    "ml": self.semaphore_manager.ml_limit,
                    "batch": self.semaphore_manager.batch_limit,
                    "database": self.semaphore_manager.database_limit,
                },
                "concurrency_adjustments": self.semaphore_manager.adjustment_count,
            },
        )

    # [Continue with all the existing methods from the original file...]

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

        except Exception as e:
            self.logger.exception(
                f"Memory aware batch size calculation failed: {e}",
                extra={
                    "processing_stage": "memory_batch_size",
                    "correlation_id": trace_id_var.get(""),
                },
            )
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
            if (
                memory_mb > self._total_memory_limit / (1024 * 1024) * 0.9
            ):  # 90% threshold
                self.logger.warning(f"High memory usage detected: {memory_mb:.1f}MB")

                # Emergency cleanup
                cleanup_stats = self._memory_pool.cleanup_all()
                collected = gc.collect()

                self.logger.info(
                    f"Emergency cleanup: pools cleared {cleanup_stats}, GC collected {collected}"
                )
                metrics.inc("memory_emergency_cleanups")

        except Exception as e:
            self.logger.exception(
                f"Memory check failed: {e}",
                extra={
                    "processing_stage": "memory_check",
                    "correlation_id": trace_id_var.get(""),
                },
            )

    async def _enrich_claims_with_rvu(
        self, claims: List[Dict[str, Any]], conversion_factor: float
    ) -> List[Dict[str, Any]]:
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
                    enriched_claim["reimbursement_amount"] = (
                        rvu_total * units * conversion_factor
                    )
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
                        line_item["reimbursement_amount"] = (
                            rvu_total * units * conversion_factor
                        )
                    except (ValueError, TypeError):
                        line_item["reimbursement_amount"] = 0.0

            enriched_claims.append(enriched_claim)

        metrics.inc("claims_enriched_with_rvu", len(enriched_claims))
        return enriched_claims

    async def _insert_claims_bulk(self, claims_data: List[Dict[str, Any]]) -> int:
        """Bulk insert claims with proper schema handling for both databases."""
        if not claims_data:
            return 0

        import uuid
        from datetime import datetime

        # Prepare data for bulk insert with proper column mapping
        sql_insert_data = []
        pg_insert_data = []
        
        for claim in claims_data:
            claim_id = claim.get("claim_id") or str(uuid.uuid4())
            patient_acct = claim.get("patient_account_number")
            facility_id = claim.get("facility_id")
            service_to_date = claim.get("service_to_date") or datetime.now().date()
            
            # Apply encryption if configured
            if self.encryption_key and patient_acct:
                from ..security.compliance import encrypt_text
                patient_acct = encrypt_text(str(patient_acct), self.encryption_key)

            if patient_acct and facility_id and claim_id:
                # SQL Server insert data (must include claim_id as PRIMARY KEY)
                sql_insert_data.append((
                    claim_id,
                    facility_id, 
                    patient_acct,
                    claim.get("processing_status", "pending"),
                    claim.get("processing_stage", "received"),
                    datetime.now(),
                    datetime.now()
                ))
                
                # PostgreSQL insert data (must include service_to_date for composite key)
                pg_insert_data.append((
                    claim_id,
                    facility_id,
                    patient_acct, 
                    service_to_date,
                    claim.get("processing_status", "pending"),
                    claim.get("processing_stage", "received"),
                    datetime.now(),
                    datetime.now()
                ))

        total_inserted = 0

        # Try SQL Server first (primary database)
        if sql_insert_data:
            try:
                sql_columns = [
                    "claim_id", "facility_id", "patient_account_number", 
                    "processing_status", "processing_stage", "created_at", "updated_at"
                ]
                sql_result = await self.sql.bulk_insert_tvp("claims", sql_columns, sql_insert_data)
                total_inserted += sql_result
                self.logger.info(f"Successfully inserted {sql_result} claims into SQL Server")
            except Exception as e:
                self.logger.warning(f"SQL Server insert failed: {e}")
                # Continue to try PostgreSQL fallback

        # Try PostgreSQL (staging database) 
        if pg_insert_data:
            try:
                pg_columns = [
                    "claim_id", "facility_id", "patient_account_number", "service_to_date",
                    "processing_status", "processing_stage", "created_at", "updated_at"
                ]
                pg_result = await self.pg.copy_records("claims", pg_columns, pg_insert_data)
                total_inserted += pg_result
                self.logger.info(f"Successfully inserted {pg_result} claims into PostgreSQL")
            except Exception as fallback_e:
                self.logger.exception(
                    f"PostgreSQL fallback insert failed: {fallback_e}",
                    extra={
                        "processing_stage": "insert_claims_bulk",
                        "correlation_id": trace_id_var.get(""),
                    },
                )

        return total_inserted

    async def _update_claims_status_bulk(self, claim_ids: List[str], processing_status: str, processing_stage: str) -> None:
        """Update processing status and stage for multiple claims with deadlock retry logic."""
        if not claim_ids:
            return

        # Sort claim_ids to ensure consistent lock ordering and reduce deadlock probability
        sorted_claim_ids = sorted(claim_ids)
        
        max_retries = 3
        base_delay = 0.1  # 100ms base delay
        
        for attempt in range(max_retries + 1):
            try:
                # Update PostgreSQL
                if sorted_claim_ids:
                    pg_query = """
                        UPDATE claims 
                        SET processing_status = $1, processing_stage = $2, updated_at = CURRENT_TIMESTAMP 
                        WHERE claim_id = ANY($3)
                    """
                    await self.pg.execute(pg_query, processing_status, processing_stage, sorted_claim_ids)

                # Update SQL Server (if claims exist there)
                if sorted_claim_ids and hasattr(self.sql, 'execute_many'):
                    try:
                        sql_query = """
                            UPDATE claims 
                            SET processing_status = ?, processing_stage = ?, updated_at = GETDATE() 
                            WHERE claim_id = ?
                        """
                        sql_data = [(processing_status, processing_stage, claim_id) for claim_id in sorted_claim_ids]
                        await self.sql.execute_many(sql_query, sql_data)
                    except Exception as sql_error:
                        # Log SQL Server update failure but don't fail the entire operation
                        # since PostgreSQL is the primary source for processing status
                        self.logger.warning(f"SQL Server status update failed (non-critical): {sql_error}")
                
                # If we reach here, the update succeeded
                return

            except Exception as e:
                error_msg = str(e).lower()
                
                # Check if this is a deadlock error
                is_deadlock = any(keyword in error_msg for keyword in [
                    'deadlock detected', 'deadlock', 'lock timeout', 
                    'concurrent update', 'serialization failure'
                ])
                
                if is_deadlock and attempt < max_retries:
                    # Calculate exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + (time.time() % 0.1)  # Add jitter
                    self.logger.warning(
                        f"Deadlock detected on attempt {attempt + 1}, retrying in {delay:.3f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    # Final attempt failed or non-deadlock error
                    if is_deadlock:
                        self.logger.error(f"Failed to update claims status after {max_retries + 1} attempts due to deadlock: {e}")
                    else:
                        self.logger.warning(f"Failed to update claims status: {e}")
                    return

    def _truncate_failed_claims_data(self, failed_claims_data: List[tuple]) -> List[tuple]:
        """Truncate failed claims data to fit database column constraints."""
        truncated_data = []
        for row in failed_claims_data:
            # Truncate each field to match SQL Server schema limits
            truncated_row = (
                row[0][:50] if row[0] else None,     # claim_id - VARCHAR(50)
                row[1][:20] if row[1] else None,     # facility_id - VARCHAR(20)
                row[2][:50] if row[2] else None,     # patient_account_number - VARCHAR(50)
                row[3][:1000] if row[3] else None,   # failure_reason - NVARCHAR(1000)
                row[4][:50] if row[4] else None,     # failure_category - VARCHAR(50)
                row[5][:50] if row[5] else None,     # processing_stage - VARCHAR(50)
                row[6],                              # original_data - NVARCHAR(MAX) (no limit)
                row[7],                              # repair_suggestions - NVARCHAR(MAX) (no limit)
            )
            truncated_data.append(truncated_row)
        return truncated_data

    async def _record_failed_claims_bulk(self, failed_claims_data: List[tuple]) -> None:
        """Record failed claims in bulk with memory management."""
        if not failed_claims_data:
            return

        # Truncate data to fit database constraints
        truncated_data = self._truncate_failed_claims_data(failed_claims_data)

        try:
            if "concurrency" in inspect.signature(self.sql.execute_many).parameters:
                await self.sql.execute_many(
                    """INSERT INTO failed_claims (
                        claim_id, facility_id, patient_account_number,
                        failure_reason, failure_category, processing_stage,
                        failed_at, original_data, repair_suggestions
                    ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                    truncated_data,
                    concurrency=4,
                )
            else:
                await self.sql.execute_many(
                    """INSERT INTO failed_claims (
                        claim_id, facility_id, patient_account_number,
                        failure_reason, failure_category, processing_stage,
                        failed_at, original_data, repair_suggestions
                    ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                    truncated_data,
                )
            metrics.inc("bulk_failed_claims_recorded", len(failed_claims_data))
        except Exception as e:
            self.logger.exception(
                f"Bulk failed claims recording failed: {e}",
                extra={
                    "processing_stage": "record_failed_claims",
                    "correlation_id": trace_id_var.get(""),
                },
            )

    async def _record_checkpoints_bulk(self, checkpoints: List[tuple]) -> None:
        """Record checkpoints in bulk with memory management."""
        if not checkpoints:
            return

        try:
            if "concurrency" in inspect.signature(self.pg.execute_many).parameters:
                await self.pg.execute_many(
                    "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
                    checkpoints,
                    concurrency=4,
                )
            else:
                await self.pg.execute_many(
                    "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
                    checkpoints,
                )
            metrics.inc("bulk_checkpoints_recorded", len(checkpoints))
        except Exception as e:
            self.logger.exception(
                f"Bulk checkpoint recording failed: {e}",
                extra={
                    "processing_stage": "record_checkpoints",
                    "correlation_id": trace_id_var.get(""),
                },
            )

    def calculate_batch_size_adaptive(self) -> int:
        """Advanced adaptive batch sizing based on system metrics and dynamic concurrency."""
        base_size = self.cfg.processing.batch_size

        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0

        # Get current performance metrics
        current_throughput = metrics.get("batch_processing_rate_per_sec")
        memory_usage = metrics.get("memory_usage_mb")
        cpu_usage = metrics.get("cpu_usage_percent")

        # Get system resource factors from semaphore manager
        self.semaphore_manager.resource_monitor.update()
        cpu_factor = self.semaphore_manager._get_cpu_scaling_factor()
        memory_factor = self.semaphore_manager._get_memory_scaling_factor()
        load_factor = self.semaphore_manager._get_load_scaling_factor()

        # Throughput-based scaling
        throughput_factor = 1.0
        if current_throughput > 0:
            if current_throughput > 8000:  # High throughput
                throughput_factor = 1.2
            elif current_throughput < 2000:  # Low throughput
                throughput_factor = 0.8

        # Concurrency-based scaling - adjust batch size based on available concurrency
        concurrency_factor = min(
            self.semaphore_manager.validation_limit / 100,  # Normalize
            self.semaphore_manager.ml_limit / 60,
            self.semaphore_manager.database_limit / 30,
        )

        # Calculate final batch size
        combined_factor = (
            cpu_factor
            * memory_factor
            * load_factor
            * throughput_factor
            * concurrency_factor
        )
        adaptive_size = int(base_size * combined_factor)

        # Apply bounds
        min_size = max(100, base_size // 10)
        max_size = base_size * 5  # Reduced for memory safety
        final_size = max(min_size, min(max_size, adaptive_size))

        # Update metrics
        metrics.set("adaptive_cpu_factor", cpu_factor)
        metrics.set("adaptive_memory_factor", memory_factor)
        metrics.set("adaptive_load_factor", load_factor)
        metrics.set("adaptive_throughput_factor", throughput_factor)
        metrics.set("adaptive_concurrency_factor", concurrency_factor)
        metrics.set("adaptive_combined_factor", combined_factor)

        return final_size

    async def _process_stream_batch_optimized(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Process a single batch in the optimized stream with dynamic concurrency management."""
        if not claims:
            return {"processed": 0, "failed": 0}

        batch_start = time.perf_counter()

        # Use memory pooling for stream processing
        batch_objects = None
        try:
            batch_objects = self._acquire_batch_objects(len(claims))
            failed_claims_data = []  # Initialize early to track all failures
            valid_claims = []  # Initialize early

            # Bulk RVU enrichment
            enrich_start = time.perf_counter()
            enriched_claims = await self._enrich_claims_with_rvu(
                claims, self.cfg.processing.conversion_factor
            )
            enrich_time = time.perf_counter() - enrich_start
            self.semaphore_manager.resource_monitor.update()
            self.logger.info(
                "Enrichment stage complete",
                extra={
                    "event": "enrichment_complete",
                    "performance_breakdown": {
                        "enrich_ms": round(enrich_time * 1000, 2)
                    },
                    "system_resources": {
                        "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                        "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                    },
                },
            )

            # Validation with dynamic concurrency
            validation_start = time.perf_counter()
            validation_results = {}
            if self.validator:
                async with self.semaphore_manager.validation_semaphore:
                    try:
                        validation_results = await self.validator.validate_batch(
                            enriched_claims
                        )
                    except Exception as e:
                        self.logger.exception(
                            f"Validation error: {e}",
                            extra={
                                "processing_stage": "validation_stream",
                                "correlation_id": trace_id_var.get(""),
                            },
                        )
                        self.logger.error(f"Validation error: {e}")
            validation_time = time.perf_counter() - validation_start
            self.semaphore_manager.resource_monitor.update()
            self.logger.info(
                "Validation stage complete",
                extra={
                    "event": "validation_complete",
                    "performance_breakdown": {
                        "validation_ms": round(validation_time * 1000, 2)
                    },
                    "system_resources": {
                        "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                        "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                    },
                },
            )

            # Update claims status to validation stage
            if enriched_claims:
                claim_ids = [claim.get("claim_id") for claim in enriched_claims if claim.get("claim_id")]
                await self._update_claims_status_bulk(claim_ids, "processing", "validation")

            # Rules evaluation
            rules_results = {}
            if self.rules_engine:
                try:
                    rules_results = await self.rules_engine.evaluate_batch_async(
                        enriched_claims
                    )
                except Exception as e:
                    self.logger.exception(
                        f"Rules evaluation error: {e}",
                        extra={
                            "processing_stage": "rules_stream",
                            "correlation_id": trace_id_var.get(""),
                        },
                    )

            # Update claims status to rules stage
            if enriched_claims:
                claim_ids = [claim.get("claim_id") for claim in enriched_claims if claim.get("claim_id")]
                await self._update_claims_status_bulk(claim_ids, "processing", "rules")

            # ML inference with dynamic concurrency
            ml_start = time.perf_counter()
            predictions = []
            if self.model:
                async with self.semaphore_manager.ml_semaphore:
                    if hasattr(self.model, "predict_batch"):
                        predictions = self.model.predict_batch(enriched_claims)
                    else:
                        predictions = [
                            self.model.predict(claim) for claim in enriched_claims
                        ]
            else:
                predictions = [0] * len(enriched_claims)
            ml_time = time.perf_counter() - ml_start
            self.semaphore_manager.resource_monitor.update()
            self.logger.info(
                "ML stage complete",
                extra={
                    "event": "ml_complete",
                    "performance_breakdown": {
                        "ml_inference_ms": round(ml_time * 1000, 2)
                    },
                    "system_resources": {
                        "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                        "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                    },
                },
            )

            # Update claims status to ML stage
            if enriched_claims:
                claim_ids = [claim.get("claim_id") for claim in enriched_claims if claim.get("claim_id")]
                await self._update_claims_status_bulk(claim_ids, "processing", "ml")

            # Process results

            for claim, prediction in zip(enriched_claims, predictions):
                claim_id = claim.get("claim_id", "")
                correlation_id_var.set(claim.get("correlation_id", ""))
                v_errors = validation_results.get(claim_id, [])
                r_errors = rules_results.get(claim_id, [])

                claim["filter_number"] = prediction

                if self.model_monitor:
                    self.model_monitor.record_prediction(prediction)

                if v_errors or r_errors:
                    # Prepare failed claim data
                    all_errors = v_errors + r_errors
                    suggestions = self.repair_suggester.suggest(all_errors, claim)

                    encrypted_claim = claim.copy()
                    if self.encryption_key:
                        from ..security.compliance import (
                            encrypt_claim_fields, encrypt_text)

                        encrypted_claim = encrypt_claim_fields(
                            claim, self.encryption_key
                        )
                        original_data = encrypt_text(str(claim), self.encryption_key)
                    else:
                        original_data = str(claim)

                    failed_claims_data.append(
                        (
                            encrypted_claim.get("claim_id"),
                            encrypted_claim.get("facility_id"),
                            encrypted_claim.get("patient_account_number"),
                            "validation" if v_errors else "rules",
                            ErrorCategory.VALIDATION.value,
                            "validation",
                            original_data,
                            suggestions,
                        )
                    )
                else:
                    valid_claims.append(claim)

            # Bulk database operations with dynamic concurrency
            db_start = time.perf_counter()
            async with self.semaphore_manager.database_semaphore:
                bulk_tasks = []
                if valid_claims:
                    bulk_tasks.append(self._insert_claims_bulk(valid_claims))
                if failed_claims_data:
                    bulk_tasks.append(
                        self._record_failed_claims_bulk(failed_claims_data)
                    )

                if bulk_tasks:
                    bulk_results = await asyncio.gather(*bulk_tasks, return_exceptions=True)
                    
                    # Check for exceptions in bulk operations
                    record_failed_claims_failed = False
                    for i, result in enumerate(bulk_results):
                        if isinstance(result, Exception):
                            task_name = "insert_claims" if i == 0 else "record_failed_claims"
                            self.logger.error(
                                f"Stream bulk {task_name} operation failed: {result}",
                                extra={
                                    "processing_stage": "stream_database_bulk",
                                    "correlation_id": trace_id_var.get(""),
                                    "error_type": type(result).__name__
                                }
                            )
                            
                            # Track if recording failed claims failed
                            if i == 1:  # record_failed_claims task
                                record_failed_claims_failed = True
                    
                    # If recording failed claims failed, try to update their status anyway  
                    if record_failed_claims_failed and failed_claims_data:
                        try:
                            failed_claim_ids = [f[0] for f in failed_claims_data if f[0]]
                            if failed_claim_ids:
                                self.logger.info(f"Stream fallback: updating status for {len(failed_claim_ids)} failed claims")
                                await self._update_claims_status_bulk(failed_claim_ids, "failed", "failed")
                        except Exception as fallback_error:
                            self.logger.error(f"Stream fallback status update failed: {fallback_error}")
            db_time = time.perf_counter() - db_start
            self.semaphore_manager.resource_monitor.update()
            self.logger.info(
                "Database stage complete",
                extra={
                    "event": "database_complete",
                    "performance_breakdown": {"database_ms": round(db_time * 1000, 2)},
                    "system_resources": {
                        "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                        "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                    },
                },
            )

            # Update final status for successful and failed claims
            if valid_claims:
                valid_claim_ids = [claim.get("claim_id") for claim in valid_claims if claim.get("claim_id")]
                await self._update_claims_status_bulk(valid_claim_ids, "completed", "completed")
            
            if failed_claims_data:
                failed_claim_ids = [row[0] for row in failed_claims_data if row[0]]  # claim_id is first element
                await self._update_claims_status_bulk(failed_claim_ids, "failed", "failed")

            # Update status
            processing_status["processed"] += len(valid_claims)
            processing_status["failed"] += len(failed_claims_data)

            # Additional metrics
            revenue = sum(
                float(c.get("reimbursement_amount", 0.0) or 0.0) for c in valid_claims
            )
            metrics.inc("claims_processed", len(valid_claims))
            metrics.inc("claims_failed", len(failed_claims_data))
            metrics.record_hourly("claims_processed", len(valid_claims))
            metrics.record_hourly("revenue_impact", revenue)
            metrics.inc("revenue_impact", revenue)
            total_time = time.perf_counter() - batch_start
            sla_monitor.record_batch(total_time, len(enriched_claims))

            await self._run_error_detection_check()

            return {"processed": len(valid_claims), "failed": len(failed_claims_data)}

        except Exception as process_error:
            # Handle catastrophic processing failures in stream processing
            self.logger.error(
                f"Critical processing error in stream batch: {process_error}",
                extra={
                    "processing_stage": "stream_batch_processing",
                    "correlation_id": trace_id_var.get(""),
                    "error_type": type(process_error).__name__,
                    "batch_size": len(claims)
                }
            )
            
            # If we have claims that were fetched but failed processing, mark them as failed
            try:
                if claims and len(claims) > 0:
                    # All claims in this batch failed due to processing error
                    failed_claim_ids = [c.get("claim_id", "") for c in claims if c.get("claim_id")]
                    if failed_claim_ids:
                        self.logger.info(f"Marking {len(failed_claim_ids)} stream claims as failed due to processing error")
                        await self._update_claims_status_bulk(failed_claim_ids, "failed", "processing_error")
            except Exception as status_update_error:
                self.logger.error(f"Failed to update status for failed stream claims: {status_update_error}")
            
            # Return failure metrics
            return {
                "processed": 0,
                "failed": len(claims) if claims else 0
            }

        finally:
            # Always release batch objects
            if batch_objects:
                self._release_batch_objects(batch_objects)

    async def _perform_stream_memory_cleanup(self) -> None:
        """Perform memory cleanup specific to stream processing."""
        # Clear processing caches
        if hasattr(self, "cache"):
            if hasattr(self.cache, "store"):
                # Keep only recent cache entries
                cache_size = len(self.cache.store)
                if cache_size > 10000:
                    # Remove oldest entries
                    keys_to_remove = list(self.cache.store.keys())[
                        :-5000
                    ]  # Keep last 5000
                    for key in keys_to_remove:
                        self.cache.store.pop(key, None)

                    self.logger.info(
                        f"Cache cleanup: removed {len(keys_to_remove)} entries"
                    )

        # Clear service buffers
        if hasattr(self, "service"):
            await self.service.flush_checkpoints()

        # Clear validation caches
        if hasattr(self, "validator") and self.validator:
            # Reset large validation caches periodically
            current_time = time.time()
            if hasattr(self.validator, "_validation_cache_time"):
                if (
                    current_time - self.validator._validation_cache_time > 3600
                ):  # 1 hour
                    self.validator._facilities_cache = set()
                    self.validator._classes_cache = set()
                    self.validator._validation_cache_time = 0.0

        # Clear RVU cache if it gets too large
        if self.rvu_cache and hasattr(self.rvu_cache, "local_cache"):
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

        self.logger.debug(
            f"Stream memory cleanup: {collected} objects collected, pools: {cleanup_stats}"
        )

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

    async def shutdown(self) -> None:
        """Shutdown the pipeline and cleanup resources."""
        # Stop the concurrency monitor
        if self._concurrency_monitor_task:
            self._concurrency_monitor_task.cancel()
            try:
                await self._concurrency_monitor_task
            except asyncio.CancelledError:
                pass

        # Stop the memory monitor
        if self._memory_monitor_task:
            self._memory_monitor_task.cancel()
            try:
                await self._memory_monitor_task
            except asyncio.CancelledError:
                pass

        await self.recovery_manager.stop()

        # Perform final cleanup
        await self._perform_memory_cleanup()

        # Close database connections
        if self.pg:
            await self.pg.close()
        if self.sql:
            await self.sql.close()

        self.logger.info("Pipeline shutdown complete")

    async def process_claim(self, claim: Dict[str, Any]) -> None:
        """Process single claim with memory management and dynamic concurrency."""
        # Use memory pool for single claim processing
        await self._check_services_health()
        if self.mode == "backup":
            await self._queue_claims_locally([claim])
            self.logger.warning(
                "Queued claim locally due to backup mode",
                extra={"claim_id": claim.get("claim_id")},
            )
            return {"status": "queued"}

        correlation_id_var.set(claim.get("correlation_id", ""))
        pooled_claim = self._memory_pool.acquire("claims", dict)
        try:
            pooled_claim.clear()
            pooled_claim.update(claim)

            # Process claim using existing pipeline logic with concurrency control
            result = await self._process_single_claim_with_pooling(pooled_claim)
            return result

        finally:
            self._memory_pool.release("claims", pooled_claim)

    async def _process_single_claim_with_pooling(
        self, claim: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process single claim using memory pooling and dynamic concurrency."""
        correlation_id_var.set(claim.get("correlation_id", ""))
        try:
            # Validate claim with dynamic concurrency
            errors = []
            if self.validator:
                async with self.semaphore_manager.validation_semaphore:
                    errors = await self.validator.validate(claim)

            # Apply rules
            if self.rules_engine and not errors:
                rule_errors = self.rules_engine.evaluate(claim)
                errors.extend(rule_errors)

            # Apply ML model with dynamic concurrency
            prediction = 0
            if self.model and not errors:
                async with self.semaphore_manager.ml_semaphore:
                    prediction = self.model.predict(claim)
                    claim["filter_number"] = prediction

            if errors:
                # Record failed claim with database semaphore
                async with self.semaphore_manager.database_semaphore:
                    await self.service.record_failed_claim(
                        claim,
                        "validation_failed",
                        self.repair_suggester.suggest(errors, claim),
                    )
                return {"status": "failed", "errors": errors}
            else:
                # Insert valid claim with database semaphore
                async with self.semaphore_manager.database_semaphore:
                    await self.service.insert_claims(
                        [
                            (
                                claim.get("patient_account_number"),
                                claim.get("facility_id"),
                                claim.get("procedure_code"),
                            )
                        ]
                    )
                return {"status": "processed", "prediction": prediction}

        except Exception as e:
            self.logger.exception(
                f"Single claim processing failed: {e}",
                extra={
                    "claim_id": claim.get("claim_id"),
                    "processing_stage": "single_claim",
                    "correlation_id": trace_id_var.get(""),
                },
            )
            return {"status": "error", "message": str(e)}

    def calculate_batch_size(self) -> int:
        """Backward compatibility wrapper."""
        return self.calculate_batch_size_adaptive()

    async def process_claims_batch(self, claims: List[Dict[str, Any]]) -> List[tuple]:
        """Process a batch of claims and return results with dynamic concurrency."""
        await self._check_services_health()
        if self.mode == "backup":
            await self._queue_claims_locally(claims)
            self.logger.warning("Queued batch locally due to backup mode")
            return []

        # Use memory pooling for batch processing
        batch_objects = None
        try:
            batch_objects = self._acquire_batch_objects(len(claims))

            # Use batch semaphore for overall concurrency control
            async with self.semaphore_manager.batch_semaphore:
                # Process batch with database semaphore
                async with self.semaphore_manager.database_semaphore:
                    results = []
                    for claim in claims:
                        try:
                            # Simplified processing
                            correlation_id_var.set(claim.get("correlation_id", ""))
                            patient_acct = claim.get("patient_account_number")
                            facility_id = claim.get("facility_id")

                            if patient_acct and facility_id:
                                results.append((patient_acct, facility_id))

                        except Exception as e:
                            self.logger.exception(
                                f"Claim processing failed: {e}",
                                extra={
                                    "claim_id": claim.get("claim_id"),
                                    "processing_stage": "batch_single_claim",
                                    "correlation_id": trace_id_var.get(""),
                                },
                            )
                            continue

                    return results

        finally:
            if batch_objects:
                self._release_batch_objects(batch_objects)

    # Performance monitoring methods
    def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get comprehensive optimization metrics including dynamic concurrency management."""
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
            "dynamic_concurrency": {
                "current_limits": {
                    "validation": self.semaphore_manager.validation_limit,
                    "ml": self.semaphore_manager.ml_limit,
                    "batch": self.semaphore_manager.batch_limit,
                    "database": self.semaphore_manager.database_limit,
                },
                "adjustments_made": self.semaphore_manager.adjustment_count,
                "emergency_throttling_active": self.semaphore_manager.emergency_active,
                "system_resources": {
                    "cpu_percent": self.semaphore_manager.resource_monitor.cpu_percent,
                    "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                    "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                    "load_average": self.semaphore_manager.resource_monitor.load_average,
                },
            },
            "pipeline_features": self._get_optimization_features(),
            "performance_metrics": {
                "current_throughput": metrics.get("batch_processing_rate_per_sec"),
                "adaptive_batch_size": metrics.get("dynamic_batch_size"),
                "processing_status": dict(processing_status),
            },
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
                "memory_utilization": memory_mb
                / (self._total_memory_limit / 1024 / 1024),
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
                },
            }
        except Exception as e:
            return {"error": str(e)}

    def get_concurrency_status(self) -> Dict[str, Any]:
        """Get current dynamic concurrency status and statistics."""
        return {
            "current_limits": {
                "validation": self.semaphore_manager.validation_limit,
                "ml": self.semaphore_manager.ml_limit,
                "batch": self.semaphore_manager.batch_limit,
                "database": self.semaphore_manager.database_limit,
            },
            "limit_bounds": {
                "min_limits": self.semaphore_manager.min_limits,
                "max_limits": self.semaphore_manager.max_limits,
            },
            "system_resources": {
                "cpu_percent": self.semaphore_manager.resource_monitor.cpu_percent,
                "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                "memory_available_gb": self.semaphore_manager.resource_monitor.memory_available_gb,
                "load_average": self.semaphore_manager.resource_monitor.load_average,
            },
            "adjustment_stats": {
                "total_adjustments": self.semaphore_manager.adjustment_count,
                "last_adjustment": self.semaphore_manager.last_adjustment,
                "adjustment_interval": self.semaphore_manager.adjustment_interval,
            },
            "emergency_throttling": {
                "active": self.semaphore_manager.emergency_active,
                "start_time": self.semaphore_manager.emergency_start_time,
            },
            "scaling_factors": {
                "cpu_factor": metrics.get("adaptive_cpu_factor", 1.0),
                "memory_factor": metrics.get("adaptive_memory_factor", 1.0),
                "load_factor": metrics.get("adaptive_load_factor", 1.0),
                "concurrency_factor": metrics.get("adaptive_concurrency_factor", 1.0),
            },
        }

    async def benchmark_optimizations(self, test_size: int = 10000) -> Dict[str, Any]:
        """Benchmark the optimization improvements including dynamic concurrency management."""
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
                "garbage_collection_optimization",
                "dynamic_concurrency_management",
                "emergency_throttling",
                "resource_aware_scaling",
            ],
        }

        # Record initial states
        initial_memory = self.get_memory_status()
        initial_concurrency = self.get_concurrency_status()

        # Run a test batch to measure performance
        start_time = time.perf_counter()
        result = await self.process_batch_ultra_optimized()
        end_time = time.perf_counter()

        # Record final states
        final_memory = self.get_memory_status()
        final_concurrency = self.get_concurrency_status()

        benchmark_results.update(
            {
                "test_duration_sec": end_time - start_time,
                "test_throughput": result.get("throughput", 0),
                "performance_breakdown": result.get("performance_breakdown", {}),
                "optimization_effectiveness": (
                    "ultra_high" if result.get("throughput", 0) > 5000 else "high"
                ),
                "memory_performance": {
                    "initial_memory_mb": initial_memory.get("current_memory_mb", 0),
                    "final_memory_mb": final_memory.get("current_memory_mb", 0),
                    "memory_growth_mb": final_memory.get("current_memory_mb", 0)
                    - initial_memory.get("current_memory_mb", 0),
                    "memory_pools_used": final_memory.get("memory_pools", {}),
                    "memory_efficiency": (
                        "excellent"
                        if final_memory.get("current_memory_mb", 0)
                        - initial_memory.get("current_memory_mb", 0)
                        < 100
                        else "good"
                    ),
                },
                "concurrency_performance": {
                    "initial_limits": initial_concurrency.get("current_limits", {}),
                    "final_limits": final_concurrency.get("current_limits", {}),
                    "adjustments_during_test": final_concurrency.get(
                        "adjustment_stats", {}
                    ).get("total_adjustments", 0)
                    - initial_concurrency.get("adjustment_stats", {}).get(
                        "total_adjustments", 0
                    ),
                    "emergency_throttling_triggered": final_concurrency.get(
                        "emergency_throttling", {}
                    ).get("active", False),
                    "resource_utilization": final_concurrency.get(
                        "system_resources", {}
                    ),
                    "concurrency_efficiency": (
                        "excellent"
                        if result.get("concurrency_stats", {}).get(
                            "adjustments_made", 0
                        )
                        > 0
                        else "good"
                    ),
                },
            }
        )

        return benchmark_results

    async def force_concurrency_adjustment(self) -> Dict[str, Any]:
        """Force an immediate concurrency adjustment for testing/debugging."""
        old_limits = {
            "validation": self.semaphore_manager.validation_limit,
            "ml": self.semaphore_manager.ml_limit,
            "batch": self.semaphore_manager.batch_limit,
            "database": self.semaphore_manager.database_limit,
        }

        # Force adjustment
        await self.semaphore_manager.adjust_limits_dynamic()

        new_limits = {
            "validation": self.semaphore_manager.validation_limit,
            "ml": self.semaphore_manager.ml_limit,
            "batch": self.semaphore_manager.batch_limit,
            "database": self.semaphore_manager.database_limit,
        }

        return {
            "adjustment_forced": True,
            "old_limits": old_limits,
            "new_limits": new_limits,
            "system_resources": {
                "cpu_percent": self.semaphore_manager.resource_monitor.cpu_percent,
                "memory_percent": self.semaphore_manager.resource_monitor.memory_percent,
                "load_average": self.semaphore_manager.resource_monitor.load_average,
            },
            "emergency_throttling": self.semaphore_manager.emergency_active,
        }
