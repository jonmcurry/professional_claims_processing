import asyncio
import gc
import logging
import time
import weakref
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import asyncpg
    import psutil
except Exception:  # pragma: no cover - allow missing dependency in tests
    asyncpg = None
    psutil = None

from ..analysis.query_tracker import record as record_query
from ..config.config import PostgresConfig
from ..monitoring.metrics import metrics
from ..monitoring.stats import latencies
from ..utils.cache import InMemoryCache
from ..utils.circuit_breaker import CircuitBreaker
from ..utils.errors import (CircuitBreakerOpenError, DatabaseConnectionError,
                            QueryError)
from ..utils.memory_pool import memory_pool
from ..utils.tracing import get_traceparent
from .base import BaseDatabase
from .connection_utils import connect_with_retry, report_pool_metrics

logger = logging.getLogger("claims_processor")


class MemoryMonitor:
    """System memory monitoring and alerting."""

    def __init__(
        self, warning_threshold_mb: int = 1500, critical_threshold_mb: int = 2000
    ):
        self.warning_threshold = warning_threshold_mb
        self.critical_threshold = critical_threshold_mb
        self.last_warning = 0
        self.last_critical = 0
        self.warning_interval = 60  # 1 minute between warnings
        self.critical_interval = 30  # 30 seconds between critical alerts

    def check_memory(self) -> Dict[str, Any]:
        """Check current memory usage and return status."""
        try:
            if not psutil:
                return {
                    "status": "unknown",
                    "alerts": [],
                    "error": "psutil not available",
                }

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            system_memory = psutil.virtual_memory()
            available_mb = system_memory.available / 1024 / 1024

            current_time = time.time()
            alerts = []

            # Check critical threshold
            if memory_mb > self.critical_threshold:
                if current_time - self.last_critical > self.critical_interval:
                    alerts.append(
                        {
                            "level": "critical",
                            "message": f"Critical memory usage: {memory_mb:.1f}MB",
                            "memory_mb": memory_mb,
                        }
                    )
                    self.last_critical = current_time

            # Check warning threshold
            elif memory_mb > self.warning_threshold:
                if current_time - self.last_warning > self.warning_interval:
                    alerts.append(
                        {
                            "level": "warning",
                            "message": f"High memory usage: {memory_mb:.1f}MB",
                            "memory_mb": memory_mb,
                        }
                    )
                    self.last_warning = current_time

            return {
                "memory_mb": memory_mb,
                "available_mb": available_mb,
                "status": "critical"
                if memory_mb > self.critical_threshold
                else "warning"
                if memory_mb > self.warning_threshold
                else "ok",
                "alerts": alerts,
            }

        except Exception as e:
            return {"error": str(e), "status": "unknown", "alerts": []}

    def suggest_cleanup_actions(self, memory_mb: float) -> List[str]:
        """Suggest cleanup actions based on memory usage."""
        suggestions = []

        if memory_mb > self.critical_threshold:
            suggestions.extend(
                [
                    "Force garbage collection immediately",
                    "Clear all caches and buffers",
                    "Reduce batch sizes by 50%",
                    "Close idle database connections",
                    "Restart processing pipeline if possible",
                ]
            )
        elif memory_mb > self.warning_threshold:
            suggestions.extend(
                [
                    "Clear old cache entries",
                    "Flush processing buffers",
                    "Reduce batch sizes by 25%",
                    "Schedule garbage collection",
                ]
            )

        return suggestions


# Global memory monitor
memory_monitor = MemoryMonitor()


# Circuit Breaker Recovery Helper Function
async def handle_circuit_breaker_error(
    postgres_db: "PostgresDatabase", 
    logger,
    max_wait_time: float = 15.0
) -> bool:
    """
    Handle circuit breaker open error by waiting for recovery or manual reset.
    
    Returns:
        bool: True if circuit breaker recovered, False if timeout
    """
    start_time = time.time()
    
    logger.warning("PostgreSQL circuit breaker is open, attempting recovery...")
    
    while time.time() - start_time < max_wait_time:
        # Check if circuit breaker allows operations now
        if await postgres_db.circuit_breaker.allow():
            logger.info("Circuit breaker recovered naturally")
            return True
        
        # Try manual health check to potentially reset circuit breaker
        if await postgres_db.health_check():
            logger.info("Circuit breaker recovered via health check")
            return True
        
        # Wait before next attempt
        await asyncio.sleep(1.0)
    
    # Last resort: manual reset (use with caution in production)
    logger.warning("Circuit breaker did not recover, performing manual reset")
    await postgres_db.reset_circuit_breaker()
    
    return await postgres_db.circuit_breaker.allow()


class PostgresDatabase(BaseDatabase):
    """Enhanced PostgreSQL database with comprehensive memory management and circuit breaker fixes."""

    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None
        self.replica_pool: asyncpg.pool.Pool | None = None
        
        # CRITICAL FIX: Configure circuit breaker with more lenient settings
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=10,  # Increased from default 3 to 10
            recovery_time=10.0,    # Reduced from default 30.0 to 10 seconds
            name="postgres"        # Named for monitoring
        )

        # Enhanced query result cache with TTL and memory limits
        self.query_cache = InMemoryCache(ttl=60)
        self._result_cache_max_memory = 50 * 1024 * 1024  # 50MB max for result cache
        self._current_cache_memory = 0

        # Track prepared statements cached across connections
        self._prepared: Dict[str, str] = {}  # statement_name -> query
        self._prepared_statements: Dict[
            str, Any
        ] = {}  # statement_name -> prepared_statement
        self._lock = asyncio.Lock()

        # Connection pool health tracking
        self._pool_health_check_interval = 30.0
        self._last_health_check = 0.0

        # Memory management additions
        self._memory_pool = memory_pool
        self._connection_memory_tracking: Dict[int, float] = {}
        self._last_pool_cleanup = time.time()
        self._pool_cleanup_interval = 300  # 5 minutes
        self._memory_check_frequency = 50  # Check every 50 operations
        self._operation_count = 0

        # Memory cleanup references
        self._cleanup_refs: List[weakref.ref] = []

        # Circuit breaker monitoring
        self._circuit_was_open = False

    def _with_traceparent(self, query: str, traceparent: str | None) -> str:
        """Prepend a traceparent comment to the query when provided."""
        tp = traceparent or get_traceparent()
        if tp:
            return f"/* traceparent={tp} */ {query}"
        return query

    async def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker manually - useful for recovery."""
        await self.circuit_breaker.record_success()
        logger.info("PostgreSQL circuit breaker manually reset")

    async def get_circuit_breaker_status(self) -> dict:
        """Get current circuit breaker status for debugging."""
        return {
            "is_open": self.circuit_breaker.is_open,
            "failure_count": self.circuit_breaker.failure_count,
            "failure_threshold": self.circuit_breaker.failure_threshold,
            "recovery_time": self.circuit_breaker.recovery_time,
            "open_until": self.circuit_breaker.open_until,
        }

    async def _try_circuit_breaker_recovery(self) -> bool:
        """Attempt to recover from circuit breaker open state."""
        try:
            # Try a simple health check
            if self.pool:
                async with self.pool.acquire(timeout=2.0) as conn:
                    result = await conn.fetchval("SELECT 1")
                    if result == 1:
                        await self.circuit_breaker.record_success()
                        return True
            return False
        except Exception:
            return False

    async def _init_connection(self, conn: "asyncpg.Connection") -> None:
        """Initialize a new connection with prepared statements and optimizations."""
        
        # Only set session-level parameters that are safe to change
        try:
            await conn.execute("SET work_mem = '64MB'")  # Reduced for safety
            await conn.execute("SET maintenance_work_mem = '128MB'")  # Reduced for safety
            await conn.execute("SET effective_cache_size = '1GB'")  # Reduced for safety
        except Exception as e:
            # Log warning but don't fail connection initialization
            print(f"Warning: Could not set PostgreSQL session parameters: {e}")

        # Prepare cached statements on new connection
        for stmt_name, query in self._prepared.items():
            try:
                prepared_stmt = await conn.prepare(query)
                # Cache the prepared statement object if needed
                if stmt_name not in self._prepared_statements:
                    self._prepared_statements[stmt_name] = prepared_stmt
            except Exception as e:
                # Log but don't fail connection initialization
                print(f"Warning: Failed to prepare statement {stmt_name}: {e}")
                continue

    async def connect(self, prepare_queries: bool = True) -> None:
        """Enhanced connection with pre-warming and health monitoring.

        Parameters
        ----------
        prepare_queries: bool, optional
            When ``True`` (default) the connection pools will be warmed and
            common prepared statements will be created.  During initial setup
            the required tables may not exist yet, so this can be disabled to
            avoid noisy preparation errors.
        """

        async def _open() -> None:
            # Create main pool with optimizations
            self.pool = await asyncpg.create_pool(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                min_size=self.cfg.min_pool_size,
                max_size=self.cfg.max_pool_size,
                init=self._init_connection,
                command_timeout=30,
                server_settings={
                    "application_name": "claims_processor",
                    "tcp_keepalives_idle": "600",
                    "tcp_keepalives_interval": "30",
                    "tcp_keepalives_count": "3",
                    # Memory settings - have to change at the server level
                    # "shared_buffers": "256MB",
                    # "effective_cache_size": "2GB",
                    # "work_mem": "256MB",
                },
            )

            # Create replica pool if configured
            if self.cfg.replica_host:
                self.replica_pool = await asyncpg.create_pool(
                    host=self.cfg.replica_host,
                    port=self.cfg.replica_port or self.cfg.port,
                    user=self.cfg.user,
                    password=self.cfg.password,
                    database=self.cfg.database,
                    min_size=self.cfg.min_pool_size,
                    max_size=self.cfg.max_pool_size,
                    init=self._init_connection,
                    command_timeout=30,
                    server_settings={
                        "application_name": "claims_processor_replica",
                        "default_transaction_isolation": "read_committed",
                    },
                )

            # Aggressive connection pool pre-warming
            await self._warm_connection_pools()

            if prepare_queries:
                # Pre-prepare common queries
                await self._prepare_common_queries()

        await connect_with_retry(
            self.circuit_breaker,
            CircuitBreakerOpenError("Postgres circuit open"),
            _open,
            retries=self.cfg.retries,
            delay=self.cfg.retry_delay,
            max_delay=self.cfg.retry_max_delay,
            jitter=self.cfg.retry_jitter,
        )

    async def fetch_optimized_with_memory_management(
        self,
        query: str,
        *params: Any,
        use_replica: bool = True,
        traceparent: str | None = None,
    ) -> Iterable[dict]:
        """Optimized fetch with enhanced circuit breaker handling and memory management."""
        self._operation_count += 1

        # Periodic memory check
        if self._operation_count % self._memory_check_frequency == 0:
            await self._check_database_memory()

        # Enhanced circuit breaker check with logging
        if not await self.circuit_breaker.allow():
            # Mark that circuit breaker was open for recovery logging
            self._circuit_was_open = True
            
            # Try a health check to potentially reset the circuit breaker
            if await self._try_circuit_breaker_recovery():
                logger.info("Circuit breaker recovery successful, retrying operation")
            else:
                cb_status = await self.get_circuit_breaker_status()
                logger.warning(
                    f"PostgreSQL circuit breaker open - Status: {cb_status}"
                )
                raise CircuitBreakerOpenError("PostgreSQL circuit breaker open")

        # Choose pool based on use_replica flag and availability
        pool = self.replica_pool if use_replica and self.replica_pool else self.pool
        
        if not pool:
            await self.circuit_breaker.record_failure()
            raise ConnectionError("Database pool not available")

        start = time.perf_counter()
        
        # Enhanced connection acquisition with retry logic
        max_retries = 3  # Increased from 2 to 3
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # Use timeout and proper connection state management
                async with pool.acquire(timeout=15.0) as conn:  # Increased timeout
                    try:
                        # Ensure connection is ready
                        await conn.execute("SELECT 1")

                        # Execute the actual query with timeout
                        query_with_trace = self._with_traceparent(query, traceparent)
                        rows = await asyncio.wait_for(
                            conn.fetch(query_with_trace, *params),
                            timeout=60.0  # Increased timeout for complex queries
                        )

                        # Convert to dict format
                        result = [dict(row) for row in rows]

                        # Record success metrics
                        duration = (time.perf_counter() - start) * 1000
                        metrics.inc("postgres_query_ms", duration)
                        metrics.inc("postgres_query_count")

                        if duration > self.cfg.threshold_ms:
                            logger.warning(
                                "slow_query",
                                extra={"query": query[:100], "duration_ms": duration},
                            )

                        await self.circuit_breaker.record_success()

                        # Log successful recovery if circuit breaker was previously open
                        if hasattr(self, '_circuit_was_open') and self._circuit_was_open:
                            logger.info("PostgreSQL circuit breaker recovered successfully")
                            self._circuit_was_open = False

                        return result

                    except asyncio.TimeoutError:
                        last_exception = Exception("Query execution timeout")
                        logger.warning(f"Query timeout on attempt {attempt + 1}: {query[:100]}...")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Progressive backoff
                        continue

                    except Exception as e:
                        last_exception = e
                        logger.warning(f"Query execution error on attempt {attempt + 1}: {e}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(0.5 * (attempt + 1))  # Progressive backoff
                        continue
                        
            except asyncio.TimeoutError:
                last_exception = Exception("Connection acquisition timeout")
                logger.warning(f"Connection timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.0 * (attempt + 1))  # Progressive backoff
                continue
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.0 * (attempt + 1))  # Progressive backoff
                continue

        # All retries failed
        await self.circuit_breaker.record_failure()
        error_msg = f"All connection attempts failed. Last error: {last_exception}"
        logger.error(error_msg)
        
        # Include circuit breaker status in error for debugging
        cb_status = await self.get_circuit_breaker_status()
        raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}")

    async def health_check(self) -> bool:
        """Enhanced health check with circuit breaker reset on success."""
        try:
            # Check if pool exists and is healthy
            if not self.pool:
                await self.connect()
            
            # Test multiple connections from the pool
            healthy_connections = 0
            total_connections = min(3, self.pool._queue.qsize() if self.pool else 0)
            
            if total_connections == 0:
                # If no connections in queue, test by getting a new one
                try:
                    async with self.pool.acquire(timeout=5.0) as conn:  # Increased timeout
                        result = await conn.fetchval("SELECT 1")
                        if result == 1:
                            healthy_connections = 1
                            total_connections = 1
                except Exception:
                    pass
            else:
                # Test existing connections
                for i in range(min(3, total_connections)):
                    try:
                        async with self.pool.acquire(timeout=2.0) as conn:
                            result = await conn.fetchval("SELECT 1")
                            if result == 1:
                                healthy_connections += 1
                    except Exception:
                        pass
            
            # Health check passes if majority of tested connections work
            is_healthy = (
                healthy_connections >= (total_connections // 2 + 1)
                if total_connections > 0
                else False
            )
            
            if is_healthy:
                await self.circuit_breaker.record_success()
                # Log successful recovery if circuit breaker was previously open
                if hasattr(self, '_circuit_was_open') and self._circuit_was_open:
                    logger.info("PostgreSQL circuit breaker recovered successfully")
                    self._circuit_was_open = False
            else:
                await self.circuit_breaker.record_failure()
            
            # Update health metrics if metrics are available
            try:
                from ..monitoring.metrics import metrics
                metrics.set("postgres_healthy_connections", float(healthy_connections))
                metrics.set(
                    "postgres_health_check_ratio",
                    float(healthy_connections) / float(total_connections)
                    if total_connections > 0
                    else 0.0,
                )
                metrics.set("postgres_circuit_breaker_open", 1.0 if self.circuit_breaker.is_open else 0.0)
            except (ImportError, AttributeError):
                pass  # Metrics not available or not configured
            
            return is_healthy
            
        except Exception as e:
            await self.circuit_breaker.record_failure()
            # Use logger if available, otherwise print
            if hasattr(self, 'logger'):
                self.logger.error(f"PostgreSQL health check failed: {e}")
            else:
                print(f"PostgreSQL health check failed: {e}")
            return False

    # Keep all the existing memory management methods
    async def _get_from_cache_with_memory_check(self, cache_key: str) -> Optional[Any]:
        """Get from cache with memory management check."""
        # Check if cache memory usage is too high
        if self._current_cache_memory > self._result_cache_max_memory:
            await self._cleanup_cache_by_memory()

        return self.query_cache.get(cache_key)

    async def _emergency_memory_cleanup(self) -> None:
        """Emergency memory cleanup operations."""
        print("PostgreSQL: Emergency memory cleanup initiated")

        # Clear query cache
        if hasattr(self.query_cache, "store"):
            cache_size = len(self.query_cache.store)
            self.query_cache.store.clear()
            self._current_cache_memory = 0
            print(f"Emergency: cleared {cache_size} cached queries")

        # Clear prepared statement cache for unused statements
        current_time = time.time()
        unused_statements = []

        for stmt_name in list(self._prepared.keys()):
            # Remove statements not used recently (this is simplified)
            if stmt_name.startswith("temp_") or "test_" in stmt_name:
                unused_statements.append(stmt_name)

        for stmt_name in unused_statements:
            del self._prepared[stmt_name]
            self._prepared_statements.pop(stmt_name, None)

        # Clear memory pool
        if hasattr(self._memory_pool, "cleanup_all"):
            pool_cleanup_stats = self._memory_pool.cleanup_all()
            print(f"Emergency: cleared memory pools: {pool_cleanup_stats}")

        # Clear connection memory tracking
        self._connection_memory_tracking.clear()

        # Force garbage collection
        import gc
        collected = gc.collect()

        metrics.inc("database_emergency_cleanups")
        metrics.set("emergency_gc_collected", collected)
        print(f"Emergency cleanup completed, collected {collected} objects")

    def _add_memory_protection_to_query(self, query: str, limit: int = 50000) -> str:
        """Add memory protection limits to queries."""
        query_upper = query.upper()

        # Add LIMIT if not present
        if "LIMIT" not in query_upper and "TOP" not in query_upper:
            if "ORDER BY" in query_upper:
                # Insert LIMIT before ORDER BY
                order_pos = query_upper.rfind("ORDER BY")
                return query[:order_pos] + f" LIMIT {limit} " + query[order_pos:]
            else:
                # Append LIMIT at the end
                return query + f" LIMIT {limit}"

        return query

    async def _periodic_memory_check(self) -> None:
        """Periodic memory health check and cleanup."""
        memory_status = memory_monitor.check_memory()

        for alert in memory_status.get("alerts", []):
            print(f"PostgreSQL Memory Alert: {alert['message']}")
            metrics.inc(f"postgres_memory_alert_{alert['level']}")

        # Take action based on memory status
        if memory_status["status"] == "critical":
            await self._emergency_memory_cleanup()
        elif memory_status["status"] == "warning":
            await self._routine_memory_cleanup()

        # Update memory metrics
        if "memory_mb" in memory_status:
            metrics.set("postgres_process_memory_mb", memory_status["memory_mb"])

    async def _routine_memory_cleanup(self) -> None:
        """Routine memory cleanup operations."""
        # Clean old cache entries
        await self._cleanup_cache_by_memory()

        # Clean prepared statements cache
        self._cleanup_unused_prepared_statements()

        # Clean memory pool periodically
        if hasattr(self._memory_pool, "cleanup_unused"):
            self._memory_pool.cleanup_unused()

        # Schedule garbage collection
        collected = gc.collect()
        metrics.set("postgres_routine_gc_collected", collected)
        print("Performed routine PostgreSQL memory cleanup")

    async def _cache_with_memory_management(self, cache_key: str, data: Any) -> None:
        """Cache data with memory management."""
        # Estimate memory usage of the data
        estimated_size = self._estimate_data_size(data)

        # Only cache if within memory limits
        if self._current_cache_memory + estimated_size <= self._result_cache_max_memory:
            self.query_cache.set(cache_key, data)
            self._current_cache_memory += estimated_size
        else:
            # Clean cache and try again
            await self._cleanup_cache_by_memory()
            if (
                estimated_size <= self._result_cache_max_memory // 2
            ):  # Only cache if less than half limit
                self.query_cache.set(cache_key, data)
                self._current_cache_memory += estimated_size

    def _estimate_data_size(self, data: Any) -> int:
        """Estimate memory size of data (simplified)."""
        try:
            if isinstance(data, (list, tuple)):
                return len(data) * 1000  # Rough estimate: 1KB per item
            elif isinstance(data, dict):
                return len(str(data))
            else:
                return len(str(data))
        except Exception:
            return 1000  # Default estimate

    async def _cleanup_cache_by_memory(self) -> None:
        """Clean up cache based on memory usage."""
        if hasattr(self.query_cache, "store"):
            # Remove half the cache entries (LRU would be better)
            cache_items = list(self.query_cache.store.items())
            items_to_remove = len(cache_items) // 2

            for i in range(items_to_remove):
                if cache_items and i < len(cache_items):
                    key = cache_items[i][0]
                    del self.query_cache.store[key]

        # Reset memory tracking
        self._current_cache_memory = 0

    def _cleanup_unused_prepared_statements(self) -> None:
        """Clean up unused prepared statements."""
        # Simple cleanup - remove temporary statements
        temp_statements = [
            name
            for name in self._prepared.keys()
            if name.startswith(("temp_", "test_", "debug_"))
        ]

        for stmt_name in temp_statements:
            del self._prepared[stmt_name]
            self._prepared_statements.pop(stmt_name, None)

    async def _check_database_memory(self) -> None:
        """Check database memory usage and perform maintenance."""
        await self._periodic_memory_check()

    async def execute_many_with_memory_management(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 4,
        batch_size: int = 1000,
        traceparent: str | None = None,
    ) -> int:
        """Execute many with memory management and adaptive batching."""
        params_list = list(params_seq)
        if not params_list:
            return 0

        # Calculate memory-aware batch size
        memory_aware_batch_size = await self._calculate_memory_aware_batch_size(
            len(params_list)
        )
        actual_batch_size = min(batch_size, memory_aware_batch_size)

        query = self._with_traceparent(query, traceparent)

        total_processed = 0

        # Process with memory monitoring
        for i in range(0, len(params_list), actual_batch_size):
            batch = params_list[i : i + actual_batch_size]

            # Monitor memory before processing
            initial_memory = self._get_process_memory()

            try:
                batch_result = await self.execute_many_optimized(
                    query, batch, concurrency=concurrency, batch_size=actual_batch_size
                )
                total_processed += batch_result

            except Exception as e:
                if "memory" in str(e).lower():
                    # Reduce batch size and retry
                    smaller_batch_size = max(100, actual_batch_size // 2)
                    print(f"Memory error, reducing batch size to {smaller_batch_size}")

                    # Process in smaller batches
                    for j in range(0, len(batch), smaller_batch_size):
                        small_batch = batch[j : j + smaller_batch_size]
                        try:
                            small_result = await self.execute_many_optimized(
                                query,
                                small_batch,
                                concurrency=1,
                                batch_size=smaller_batch_size,
                            )
                            total_processed += small_result
                        except Exception as retry_e:
                            print(f"Failed even with smaller batch: {retry_e}")
                            raise
                else:
                    raise

            # Check memory growth
            final_memory = self._get_process_memory()
            memory_growth = final_memory - initial_memory

            if memory_growth > 100:  # More than 100MB growth
                print(f"High memory growth detected: {memory_growth:.1f}MB")
                await self._emergency_memory_cleanup()

        return total_processed

    async def execute_many_optimized(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 4,
        batch_size: int = 1000,
    ) -> int:
        """Optimized execute_many implementation."""
        params_list = list(params_seq)
        if not params_list:
            return 0

        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")

        await self._ensure_pool()
        assert self.pool

        total_affected = 0
        
        try:
            async with self.pool.acquire() as conn:
                # Use executemany for better performance
                result = await conn.executemany(query, params_list)
                total_affected = len(params_list)  # Estimate
                await self.circuit_breaker.record_success()
                
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e

        return total_affected

    async def _calculate_memory_aware_batch_size(self, total_records: int) -> int:
        """Calculate batch size based on available memory."""
        try:
            if not psutil:
                return min(1000, total_records)

            # Get current memory usage
            process_memory = self._get_process_memory()
            available_memory = psutil.virtual_memory().available / 1024 / 1024  # MB

            # Calculate safe batch size
            if process_memory > 1500:  # Over 1.5GB
                return min(500, total_records)
            elif process_memory > 1000:  # Over 1GB
                return min(1000, total_records)
            elif available_memory < 500:  # Less than 500MB available
                return min(250, total_records)
            else:
                return min(2000, total_records)

        except Exception:
            return min(1000, total_records)  # Safe default

    def _get_process_memory(self) -> float:
        """Get current process memory usage in MB."""
        try:
            if not psutil:
                return 0.0
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0

    # Enhanced versions of existing methods with memory management
    async def fetch(
        self,
        query: str,
        *params: Any,
        use_replica: bool = True,
        traceparent: str | None = None,
    ) -> Iterable[dict]:
        """Enhanced fetch with memory management."""
        return await self.fetch_optimized_with_memory_management(
            query, *params, use_replica=use_replica, traceparent=traceparent
        )

    async def execute(
        self, query: str, *params: Any, traceparent: str | None = None
    ) -> int:
        """Execute a single statement and return affected row count."""
        self._operation_count += 1

        if self._operation_count % self._memory_check_frequency == 0:
            await self._periodic_memory_check()

        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")

        await self._ensure_pool()
        assert self.pool

        query = self._with_traceparent(query, traceparent)

        start = time.perf_counter()
        async with self.pool.acquire() as conn:
            try:
                result = await conn.execute(query, *params)
                duration = (time.perf_counter() - start) * 1000
                metrics.inc("postgres_query_ms", duration)
                metrics.inc("postgres_query_count")
                latencies.record("postgres_query", duration)
                if duration > self.cfg.threshold_ms:
                    logger.warning(
                        "slow_query",
                        extra={"query": query, "duration_ms": duration},
                    )
                await self.circuit_breaker.record_success()

                try:
                    return int(result.split()[-1])
                except Exception:
                    return 0

            except Exception as e:
                await self.circuit_breaker.record_failure()
                raise QueryError(str(e)) from e

    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        traceparent: str | None = None,
    ) -> int:
        """Execute many with memory management."""
        return await self.execute_many_with_memory_management(
            query,
            params_seq,
            concurrency=concurrency,
            batch_size=1000,
            traceparent=traceparent,
        )

    async def copy_records(
        self, table: str, columns: Iterable[str], records: Iterable[Iterable[Any]]
    ) -> int:
        """Insert multiple records using execute_many."""
        rows = list(records)
        if not rows:
            return 0

        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        inserted = await self.execute_many(query, rows)
        return inserted

    async def close_with_cleanup(self) -> None:
        """Enhanced close with memory cleanup."""
        # Clear all caches
        if hasattr(self.query_cache, "store"):
            self.query_cache.store.clear()

        # Clear prepared statements
        self._prepared.clear()
        self._prepared_statements.clear()

        # Clear connection tracking
        self._connection_memory_tracking.clear()

        # Clear memory pool
        if hasattr(self._memory_pool, "cleanup_all"):
            self._memory_pool.cleanup_all()

        # Close connections
        if self.pool:
            await self.pool.close()
        if self.replica_pool:
            await self.replica_pool.close()

        # Force garbage collection
        gc.collect()

        metrics.inc("database_closes_with_cleanup")

    async def close(self) -> None:
        """Enhanced cleanup with connection health tracking."""
        await self.close_with_cleanup()

    # Include all other existing methods from the original implementation
    # (keeping the existing functionality while adding memory management)

    async def _warm_connection_pools(self) -> None:
        """Pre-warm connection pools by creating and testing all connections."""

        async def warm_pool(pool: asyncpg.pool.Pool, pool_name: str) -> None:
            """Warm a specific pool by creating all connections."""
            connections = []
            try:
                # Acquire all possible connections to force creation
                for i in range(pool._maxsize):  # type: ignore[attr-defined]
                    try:
                        conn = await pool.acquire(timeout=2.0)
                        connections.append(conn)
                        # Test connection with simple query
                        await conn.execute("SELECT 1")
                    except asyncio.TimeoutError:
                        break  # Pool likely at capacity
                    except Exception as e:
                        print(
                            f"Warning: Failed to warm connection {i} in {pool_name}: {e}"
                        )
                        break

                # Release all connections back to pool
                for conn in connections:
                    await pool.release(conn)

                print(f"Pre-warmed {len(connections)} connections in {pool_name}")
                metrics.set(f"postgres_pool_{pool_name}_warmed", len(connections))

            except Exception as e:
                print(f"Warning: Pool warming failed for {pool_name}: {e}")
                # Release any acquired connections
                for conn in connections:
                    try:
                        await pool.release(conn)
                    except Exception:
                        pass

        # Warm main pool
        if self.pool:
            await warm_pool(self.pool, "main")

        # Warm replica pool
        if self.replica_pool:
            await warm_pool(self.replica_pool, "replica")

    async def _prepare_common_queries(self) -> None:
        """Pre-prepare commonly used queries for better performance."""
        common_queries = {
            # Claims processing queries
            "fetch_claims_batch": """
                SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                       li.units AS li_units, li.charge_amount AS li_charge_amount, 
                       li.service_from_date AS li_service_from_date, 
                       li.service_to_date AS li_service_to_date 
                FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                ORDER BY c.priority DESC LIMIT $1 OFFSET $2
            """,
            "fetch_claims_priority": """
                SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                       li.units AS li_units, li.charge_amount AS li_charge_amount, 
                       li.service_from_date AS li_service_from_date, 
                       li.service_to_date AS li_service_to_date 
                FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                WHERE c.priority > $3
                ORDER BY c.priority DESC LIMIT $1 OFFSET $2
            """,
            # RVU queries - optimized for bulk operations
            "get_rvu_single": """
                SELECT procedure_code, description, total_rvu, work_rvu, 
                       practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = $1 AND status = 'active'
            """,
            "get_rvu_bulk_any": """
                SELECT procedure_code, description, total_rvu, work_rvu, 
                       practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = ANY($1) AND status = 'active'
            """,
        }

        # Prepare all common queries
        for stmt_name, query in common_queries.items():
            await self.prepare_named(stmt_name, query)

    async def prepare_named(self, statement_name: str, query: str) -> None:
        """Prepare a named statement for reuse."""
        async with self._lock:
            if statement_name in self._prepared:
                return  # Already prepared

            self._prepared[statement_name] = query

            # Prepare on existing connections if pool is available
            if self.pool:
                try:
                    async with self.pool.acquire() as conn:
                        prepared_stmt = await conn.prepare(query)
                        self._prepared_statements[statement_name] = prepared_stmt
                except Exception as e:
                    print(f"Warning: Failed to prepare statement {statement_name}: {e}")
                    # Remove from prepared dict if preparation failed
                    del self._prepared[statement_name]
                    return

    async def _ensure_pool(self) -> None:
        """Enhanced pool management with health checking."""
        if not self.pool:
            await self.connect()

        # Periodic health check
        current_time = time.time()
        if current_time - self._last_health_check > self._pool_health_check_interval:
            await self._health_check_pools()
            self._last_health_check = current_time

        if self.cfg.replica_host and not self.replica_pool:
            await self.connect()

    async def _health_check_pools(self) -> None:
        """Health check for connection pools."""
        try:
            healthy = bool(self.pool)
            if healthy and self.replica_pool:
                healthy = True
            if healthy:
                await self.circuit_breaker.record_success()
            else:
                await self.circuit_breaker.record_failure()
        except Exception:
            await self.circuit_breaker.record_failure()

    def report_pool_status(self) -> None:
        """Report connection pool metrics."""
        main_size = self.pool._queue.qsize() if self.pool else 0  # type: ignore[attr-defined]
        report_pool_metrics(
            "postgres",
            size=main_size,
            min_size=self.cfg.min_pool_size,
            max_size=self.cfg.max_pool_size,
            prepared_statements=len(self._prepared),
            cache_memory=self._current_cache_memory,
        )
    
    async def fetch_prepared(
        self, 
        statement_name: str, 
        *params: Any, 
        use_replica: bool = True,
        traceparent: str | None = None
    ) -> Iterable[dict]:
        """Fetch using a prepared statement or fallback to regular query."""
        self._operation_count += 1

        # Periodic memory check
        if self._operation_count % self._memory_check_frequency == 0:
            await self._check_database_memory()

        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("PostgreSQL circuit breaker open")

        # Check if we have this prepared statement
        if statement_name in self._prepared:
            query = self._prepared[statement_name]
            return await self.fetch_optimized_with_memory_management(
                query, *params, use_replica=use_replica, traceparent=traceparent
            )
        
        # Check for predefined common queries
        predefined_queries = {
            "fetch_claims_batch": """
                SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                    li.units AS li_units, li.charge_amount AS li_charge_amount, 
                    li.service_from_date AS li_service_from_date, 
                    li.service_to_date AS li_service_to_date 
                FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                ORDER BY c.priority DESC LIMIT $1 OFFSET $2
            """,
            "fetch_claims_priority": """
                SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, 
                    li.units AS li_units, li.charge_amount AS li_charge_amount, 
                    li.service_from_date AS li_service_from_date, 
                    li.service_to_date AS li_service_to_date 
                FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id 
                WHERE c.priority > $3
                ORDER BY c.priority DESC LIMIT $1 OFFSET $2
            """,
            "get_rvu_single": """
                SELECT procedure_code, description, total_rvu, work_rvu, 
                    practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = $1 AND status = 'active'
            """,
            "get_rvu_bulk_any": """
                SELECT procedure_code, description, total_rvu, work_rvu, 
                    practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = ANY($1) AND status = 'active'
            """,
            "rvu_bulk_fetch": """
                SELECT procedure_code, description, total_rvu, work_rvu, 
                    practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = ANY($1) AND status = 'active'
            """
        }
        
        if statement_name in predefined_queries:
            query = predefined_queries[statement_name]
            # Prepare it for future use
            await self.prepare_named(statement_name, query)
            return await self.fetch_optimized_with_memory_management(
                query, *params, use_replica=use_replica, traceparent=traceparent
            )
        
        # If no predefined query found, raise an error instead of failing silently
        raise ValueError(f"Prepared statement '{statement_name}' not found and no predefined query available")

    # Also add this connection management improvement to prevent "operation in progress" errors
    async def _acquire_connection_safely(self, use_replica: bool = False) -> "asyncpg.Connection":
        """Safely acquire a connection with proper error handling."""
        pool = self.replica_pool if use_replica and self.replica_pool else self.pool
        
        if not pool:
            raise ConnectionError("Database pool not initialized")
        
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Use timeout to prevent hanging
                conn = await pool.acquire(timeout=5.0)
                try:
                    # Ensure connection is in a clean state
                    await conn.execute("SELECT 1")
                    return conn
                except Exception as e:
                    # Connection might be in a bad state, continue to retry
                    print(f"Connection validation failed on attempt {attempt + 1}: {e}")
                    continue
                
            except asyncio.TimeoutError:
                print(f"Connection acquisition timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                    continue
                raise ConnectionError("Unable to acquire database connection: timeout")
                
            except Exception as e:
                print(f"Connection acquisition error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                    continue
                raise ConnectionError(f"Unable to acquire database connection: {e}")
        
        raise ConnectionError("Failed to acquire connection after all retries")