import gc
import time
import asyncio
import os
import logging
from typing import Any, Dict, Iterable, List, Optional
import weakref

import pyodbc

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency
    psutil = None

from ..utils.circuit_breaker import CircuitBreaker
from ..utils.errors import CircuitBreakerOpenError, DatabaseConnectionError, QueryError
from .connection_utils import connect_with_retry, report_pool_metrics
from ..utils.cache import InMemoryCache

from .base import BaseDatabase
from ..config.config import SQLServerConfig
from ..monitoring.metrics import metrics
from ..analysis.query_tracker import record as record_query
from ..monitoring.stats import latencies
from ..utils.tracing import get_traceparent
from ..memory.memory_pool import sql_memory_pool

logger = logging.getLogger("claims_processor")

class MemoryMonitor:
    """SQL Server specific memory monitoring."""
    
    def __init__(self, warning_threshold_mb: int = 1000, critical_threshold_mb: int = 1500):
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
                return {"status": "unknown", "alerts": [], "error": "psutil not available"}
                
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
                    alerts.append({
                        "level": "critical",
                        "message": f"Critical SQL Server memory usage: {memory_mb:.1f}MB",
                        "memory_mb": memory_mb
                    })
                    self.last_critical = current_time
            
            # Check warning threshold
            elif memory_mb > self.warning_threshold:
                if current_time - self.last_warning > self.warning_interval:
                    alerts.append({
                        "level": "warning",
                        "message": f"High SQL Server memory usage: {memory_mb:.1f}MB",
                        "memory_mb": memory_mb
                    })
                    self.last_warning = current_time
            
            return {
                "memory_mb": memory_mb,
                "available_mb": available_mb,
                "status": "critical" if memory_mb > self.critical_threshold else 
                         "warning" if memory_mb > self.warning_threshold else "ok",
                "alerts": alerts
            }
        
        except Exception as e:
            return {
                "error": str(e),
                "status": "unknown",
                "alerts": []
            }


# Global SQL Server memory monitor
sql_memory_monitor = MemoryMonitor()


class SQLServerDatabase(BaseDatabase):
    """Enhanced SQL Server database with comprehensive memory management."""

    def __init__(self, cfg: SQLServerConfig):
        self.cfg = cfg
        self.pool: list[pyodbc.Connection] = []
        self._lock = asyncio.Lock()
        
        # Enhanced prepared statement management
        self._prepared: Dict[str, str] = {}  # statement_name -> query
        self._prepared_on_connections: set[int] = set()  # connection IDs with prepared statements
        self.circuit_breaker = CircuitBreaker()
        
        # Enhanced query result cache with TTL and size limits
        self.query_cache = InMemoryCache(ttl=60)
        self._cache_max_memory = 30 * 1024 * 1024  # 30MB max for result cache
        self._current_cache_memory = 0
        
        # Connection pool configuration
        self.min_pool = cfg.min_pool_size
        self.max_pool = cfg.max_pool_size
        self.current_pool_size = 0
        
        # Performance tracking
        self._connection_create_count = 0
        self._last_health_check = 0.0
        self._health_check_interval = 30.0
        
        # Memory management additions
        self._memory_pool = sql_memory_pool
        self._connection_memory_limit = 100 * 1024 * 1024  # 100MB per connection
        self._total_memory_limit = 500 * 1024 * 1024  # 500MB total
        self._memory_check_frequency = 50  # Check every 50 operations
        self._operation_count = 0
        self._connection_memory_tracking: Dict[int, float] = {}
        
        # Memory cleanup references
        self._cleanup_refs: List[weakref.ref] = []

    def _with_traceparent(self, query: str, traceparent: str | None) -> str:
        tp = traceparent or get_traceparent()
        if tp:
            return f"/* traceparent={tp} */ {query}"
        return query

    async def connect(self, size: int | None = None) -> None:
        """Enhanced connection with aggressive pre-warming and optimization."""

        async def _open() -> None:
            pool_size = size or self.cfg.pool_size
            self.pool.clear()  # Clear any existing connections

            # Create connections with optimizations
            for _ in range(pool_size):
                conn = self._create_connection_optimized()
                self.pool.append(conn)
                self.current_pool_size += 1

            # Pre-warm connections and prepare common statements
            await self._warm_connection_pool()
            await self._prepare_common_statements()

            metrics.set("sqlserver_pool_initial_size", float(len(self.pool)))

        await connect_with_retry(
            self.circuit_breaker,
            CircuitBreakerOpenError("SQLServer circuit open"),
            _open,
        )

    def _create_connection_optimized(self) -> pyodbc.Connection:
        """Create an optimized connection with performance and memory settings."""
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.cfg.host},{self.cfg.port};"
            f"DATABASE={self.cfg.database};"
            f"UID={self.cfg.user};"
            f"PWD={self.cfg.password};"
            f"APP=ClaimsProcessor;"
            f"Connection Timeout=30;"
            f"Command Timeout=60;"
            f"MultipleActiveResultSets=true;"
            f"TrustServerCertificate=yes;"
            # Memory optimization settings
            f"Packet Size=32767;"  # Maximum packet size for better throughput
            f"LoginTimeout=15;"
        )
        
        conn = pyodbc.connect(connection_string, autocommit=False)
        self._connection_create_count += 1
        
        # Apply connection-level optimizations
        cursor = conn.cursor()
        try:
            # Set session-level optimizations
            cursor.execute("SET NOCOUNT ON")  # Reduce network traffic
            cursor.execute("SET ARITHABORT ON")  # Consistent query plans
            cursor.execute("SET ANSI_NULLS ON")  # Standard NULL handling
            cursor.execute("SET QUOTED_IDENTIFIER ON")  # Standard identifier quoting
            cursor.execute("SET IMPLICIT_TRANSACTIONS OFF")  # Explicit transaction control
            
            # Memory-specific optimizations
            cursor.execute("SET LOCK_TIMEOUT 30000")  # 30 second lock timeout
            cursor.execute("SET DEADLOCK_PRIORITY LOW")  # Lower deadlock priority
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")  # Standard isolation
            
            # Enable fast_executemany for bulk operations
            cursor.fast_executemany = True
        except Exception as e:
            print(f"Warning: Failed to set connection optimizations: {e}")
        finally:
            cursor.close()
        
        return conn

    async def _warm_connection_pool(self) -> None:
        """Pre-warm connections by executing simple queries with memory monitoring."""
        warmed_count = 0
        failed_count = 0
        
        for i, conn in enumerate(self.pool):
            try:
                # Monitor memory before warming
                initial_memory = self._get_process_memory()
                
                cursor = conn.cursor()
                # Execute a simple query to warm the connection
                cursor.execute("SELECT 1 as test_query")
                cursor.fetchone()
                cursor.close()
                
                # Track connection memory usage
                final_memory = self._get_process_memory()
                self._connection_memory_tracking[id(conn)] = final_memory - initial_memory
                
                warmed_count += 1
            except Exception as e:
                print(f"Warning: Failed to warm connection {i}: {e}")
                failed_count += 1
                # Try to recreate failed connection
                try:
                    conn.close()
                    self.pool[i] = self._create_connection_optimized()
                    cursor = self.pool[i].cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    warmed_count += 1
                    failed_count -= 1
                except Exception:
                    pass  # Keep the failed connection for now
        
        metrics.set("sqlserver_connections_warmed", float(warmed_count))
        metrics.set("sqlserver_connections_failed_warmup", float(failed_count))
        print(f"SQL Server pool warmed: {warmed_count} successful, {failed_count} failed")

    async def _prepare_common_statements(self) -> None:
        """Prepare commonly used statements across all connections."""
        common_statements = {
            # Facility validation queries
            "check_facility_exists": """
                SELECT 1 FROM facilities 
                WHERE facility_id = ? AND facility_id IS NOT NULL
            """,
            
            "get_facilities_batch": """
                SELECT DISTINCT facility_id 
                FROM facilities 
                WHERE facility_id IS NOT NULL
            """,
            
            # Financial class validation
            "check_financial_class": """
                SELECT 1 FROM facility_financial_classes 
                WHERE financial_class_id = ? AND active = 1
            """,
            
            "get_financial_classes_batch": """
                SELECT DISTINCT financial_class_id 
                FROM facility_financial_classes 
                WHERE financial_class_id IS NOT NULL AND active = 1
            """,
            
            # Claims insertion
            "insert_claim": """
                INSERT INTO claims (
                    patient_account_number, facility_id, procedure_code,
                    created_at, updated_at
                ) VALUES (?, ?, ?, GETDATE(), GETDATE())
            """,
            
            # Failed claims handling
            "insert_failed_claim": """
                INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)
            """,
            
            "get_failed_claims_recent": """
                SELECT TOP 100 claim_id, facility_id, patient_account_number,
                       failure_reason, failure_category, failed_at, resolution_status
                FROM failed_claims 
                ORDER BY failed_at DESC
            """,
        }
        
        # Prepare statements on all connections
        for stmt_name, query in common_statements.items():
            await self.prepare_named(stmt_name, query)

    async def prepare_named(self, statement_name: str, query: str) -> None:
        """Prepare a named statement on all connections."""
        async with self._lock:
            if statement_name in self._prepared:
                return  # Already prepared
            
            self._prepared[statement_name] = query
            prepared_count = 0
            
            # Prepare on all existing connections
            for i, conn in enumerate(self.pool):
                try:
                    cursor = conn.cursor()
                    # SQL Server doesn't have explicit PREPARE, but we cache the query
                    # The query will be automatically prepared on first execution
                    cursor.close()
                    prepared_count += 1
                except Exception as e:
                    print(f"Warning: Failed to prepare statement {statement_name} on connection {i}: {e}")
            
            metrics.set(f"sqlserver_prepared_{statement_name}", float(prepared_count))

    async def execute_many_with_memory_management(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        batch_size: int = 1000,
        traceparent: str | None = None,
    ) -> int:
        """Execute many with adaptive memory management."""
        params_list = list(params_seq)
        if not params_list:
            return 0
        
        self._operation_count += 1
        
        # Periodic memory check
        if self._operation_count % self._memory_check_frequency == 0:
            await self._check_database_memory()
        
        # Calculate memory-safe batch size
        safe_batch_size = await self._calculate_safe_batch_size(len(params_list), batch_size)

        total_processed = 0

        query = self._with_traceparent(query, traceparent)
        
        # Process with memory monitoring
        for i in range(0, len(params_list), safe_batch_size):
            batch = params_list[i:i + safe_batch_size]
            
            # Use memory pool for processing
            processing_containers = []
            try:
                # Acquire containers for batch processing
                for _ in range(min(100, len(batch))):  # Limit container allocation
                    container = self._memory_pool.acquire("sql_containers", dict)
                    processing_containers.append(container)
                
                # Process batch
                batch_result = await self._execute_batch_with_containers(
                    query, batch, processing_containers, concurrency
                )
                total_processed += batch_result
                
            finally:
                # Return containers to pool
                for container in processing_containers:
                    self._memory_pool.release("sql_containers", container)
        
        return total_processed

    async def _execute_batch_with_containers(
        self, query: str, batch: List, containers: List[Dict], concurrency: int
    ) -> int:
        """Execute batch using memory-pooled containers."""
        if concurrency > 1 and len(batch) > 100:
            # Parallel processing with memory management
            chunk_size = len(batch) // concurrency + 1
            chunks = [batch[j:j + chunk_size] for j in range(0, len(batch), chunk_size)]

            async def run_chunk_with_memory(chunk: list) -> int:
                return await self._process_chunk_with_memory_monitoring(query, chunk)

            results = await asyncio.gather(*[run_chunk_with_memory(c) for c in chunks])
            return sum(results)
        else:
            # Sequential processing with memory monitoring
            return await self._process_chunk_with_memory_monitoring(query, batch)

    async def _process_chunk_with_memory_monitoring(self, query: str, chunk: List) -> int:
        """Process chunk with comprehensive memory monitoring."""
        query = self._with_traceparent(query, traceparent)
        conn = await self._acquire()
        initial_memory = self._get_process_memory()
        start = time.perf_counter()
        
        try:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(query, chunk)
            conn.commit()
            row_count = cursor.rowcount if cursor.rowcount != -1 else len(chunk)
            cursor.close()

            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            latencies.record("sqlserver_query", duration)
            if duration > self.cfg.threshold_ms:
                logger.warning(
                    "slow_query",
                    extra={"query": query, "duration_ms": duration},
                )
            
            # Check memory growth
            final_memory = self._get_process_memory()
            memory_growth = final_memory - initial_memory
            
            if memory_growth > 50:  # More than 50MB growth
                print(f"High memory growth in SQL batch: {memory_growth:.1f}MB")
                # Trigger cleanup
                await self._routine_memory_cleanup()
                metrics.inc("sql_high_memory_growth")
            
            return row_count
            
        except Exception as e:
            if "memory" in str(e).lower() or "out of" in str(e).lower():
                print(f"SQL Server memory error: {e}")
                metrics.inc("sql_memory_errors")
                
                # Try with smaller batch
                if len(chunk) > 10:
                    smaller_chunks = [chunk[i:i+10] for i in range(0, len(chunk), 10)]
                    total_processed = 0
                    
                    for small_chunk in smaller_chunks:
                        try:
                            cursor = conn.cursor()
                            cursor.fast_executemany = True
                            cursor.executemany(query, small_chunk)
                            conn.commit()
                            chunk_count = cursor.rowcount if cursor.rowcount != -1 else len(small_chunk)
                            total_processed += chunk_count
                            cursor.close()
                        except Exception:
                            continue
                    
                    return total_processed
            raise
        finally:
            await self._release(conn)

    async def _calculate_safe_batch_size(self, total_records: int, requested_batch_size: int) -> int:
        """Calculate safe batch size based on memory constraints."""
        try:
            process_memory = self._get_process_memory()
            available_memory = psutil.virtual_memory().available / 1024 / 1024 if psutil else 1000  # MB
            
            # Adjust batch size based on memory pressure
            if process_memory > 1000:  # Over 1GB
                return min(500, requested_batch_size, total_records)
            elif process_memory > 500:  # Over 500MB
                return min(1000, requested_batch_size, total_records)
            elif available_memory < 200:  # Less than 200MB available
                return min(100, requested_batch_size, total_records)
            else:
                return min(requested_batch_size, total_records)
                
        except Exception:
            return min(1000, requested_batch_size, total_records)

    async def _check_database_memory(self) -> None:
        """Check database connection memory usage and take corrective action."""
        try:
            memory_status = sql_memory_monitor.check_memory()
            
            for alert in memory_status.get("alerts", []):
                print(f"SQL Server Memory Alert: {alert['message']}")
                metrics.inc(f"sql_memory_alert_{alert['level']}")
            
            # Take action based on memory status
            if memory_status["status"] == "critical":
                await self._emergency_sql_cleanup()
            elif memory_status["status"] == "warning":
                await self._routine_memory_cleanup()
            
            # Update memory metrics
            if "memory_mb" in memory_status:
                metrics.set("sql_process_memory_mb", memory_status["memory_mb"])
        
        except Exception as e:
            print(f"SQL memory check failed: {e}")

    async def _routine_memory_cleanup(self) -> None:
        """Routine memory cleanup operations."""
        # Clean old cache entries
        await self._cleanup_cache_by_memory()
        
        # Clean prepared statements cache
        self._cleanup_unused_prepared_statements()
        
        # Schedule garbage collection
        collected = gc.collect()
        metrics.set("sql_routine_gc_collected", collected)

    async def _emergency_sql_cleanup(self) -> None:
        """Emergency cleanup for SQL Server memory issues."""
        print("SQL Server: Emergency memory cleanup initiated")
        
        # Clear query cache
        if hasattr(self.query_cache, 'store'):
            cache_size = len(self.query_cache.store)
            self.query_cache.store.clear()
            self._current_cache_memory = 0
            print(f"SQL Emergency: cleared {cache_size} cached queries")
        
        # Close idle connections
        async with self._lock:
            if len(self.pool) > self.min_pool:
                excess_connections = len(self.pool) - self.min_pool
                for _ in range(excess_connections):
                    if self.pool:
                        conn = self.pool.pop()
                        try:
                            conn.close()
                        except Exception:
                            pass
        
        # Clear prepared statements cache
        self._prepared.clear()
        
        # Clear memory pool
        pool_cleanup_stats = self._memory_pool.cleanup_all()
        print(f"SQL Emergency: cleared memory pools: {pool_cleanup_stats}")
        
        # Clear connection memory tracking
        self._connection_memory_tracking.clear()
        
        # Force garbage collection
        collected = gc.collect()
        
        metrics.inc("sql_emergency_cleanups")
        metrics.set("sql_emergency_gc_collected", collected)

    async def _cleanup_cache_by_memory(self) -> None:
        """Clean up cache based on memory usage."""
        if hasattr(self.query_cache, 'store'):
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
        temp_statements = [name for name in self._prepared.keys() 
                          if name.startswith(('temp_', 'test_', 'debug_'))]
        
        for stmt_name in temp_statements:
            del self._prepared[stmt_name]

    def _get_process_memory(self) -> float:
        """Get current process memory usage in MB."""
        try:
            if not psutil:
                return 0.0
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0

    async def fetch_optimized_with_memory_management(
        self,
        query: str,
        *params: Any,
        is_prepared: bool = False,
        traceparent: str | None = None,
    ) -> Iterable[dict]:
        """Optimized fetch with comprehensive memory management."""
        self._operation_count += 1
        
        # Periodic memory check
        if self._operation_count % self._memory_check_frequency == 0:
            await self._check_database_memory()
        
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        query = self._with_traceparent(query, traceparent)

        # Check cache first with memory management
        cache_key = f"query:{query}:{str(params)}"
        cached = await self._get_from_cache_with_memory_check(cache_key)
        if cached is not None:
            metrics.inc("sqlserver_cache_hits")
            return cached
        
        # Use memory pool for result containers
        result_container = self._memory_pool.acquire("sql_results", list)
        
        try:
            conn = await self._acquire()
            initial_memory = self._get_process_memory()
            
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            
            # Check memory growth
            final_memory = self._get_process_memory()
            memory_growth = final_memory - initial_memory
            
            if memory_growth > 50:  # More than 50MB growth
                print(f"Warning: High memory growth in SQL fetch: {memory_growth:.1f}MB")
                await self._routine_memory_cleanup()
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            if is_prepared:
                metrics.inc("sqlserver_prepared_queries")
            latencies.record("sqlserver_query", duration)
            if duration > self.cfg.threshold_ms:
                logger.warning(
                    "slow_query",
                    extra={"query": query, "duration_ms": duration},
                )
            record_query(query, duration)
            await self.circuit_breaker.record_success()
            
            # Convert to dictionaries using memory pool
            result_container.clear()
            for row in rows:
                row_dict = self._memory_pool.acquire("sql_row_dict", dict)
                row_dict.clear()
                row_dict.update(dict(zip(columns, row)))
                result_container.append(row_dict)
            
            # Cache results with memory management
            await self._cache_with_memory_management(cache_key, result_container.copy())
            metrics.inc("sqlserver_cache_misses")
            
            return result_container
            
        except pyodbc.Error as e:
            # Handle connection issues
            try:
                conn.close()
            except Exception:
                pass
            conn = self._create_connection_optimized()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            await self.circuit_breaker.record_success()
            
            # Convert results
            result_container.clear()
            for row in rows:
                row_dict = self._memory_pool.acquire("sql_row_dict", dict)
                row_dict.clear()
                row_dict.update(dict(zip(columns, row)))
                result_container.append(row_dict)
            
            await self._cache_with_memory_management(cache_key, result_container.copy())
            return result_container
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)
            # Return container to pool
            self._memory_pool.release("sql_results", result_container)

    async def _get_from_cache_with_memory_check(self, cache_key: str) -> Optional[Any]:
        """Get from cache with memory usage check."""
        # Check if cache memory usage is too high
        if self._current_cache_memory > self._cache_max_memory:
            await self._cleanup_cache_by_memory()
        
        return self.query_cache.get(cache_key)

    async def _cache_with_memory_management(self, cache_key: str, data: Any) -> None:
        """Cache data with memory management."""
        # Estimate memory usage of the data
        estimated_size = self._estimate_data_size(data)
        
        # Only cache if within memory limits
        if self._current_cache_memory + estimated_size <= self._cache_max_memory:
            self.query_cache.set(cache_key, data)
            self._current_cache_memory += estimated_size
        else:
            # Clean cache and try again
            await self._cleanup_cache_by_memory()
            if estimated_size <= self._cache_max_memory // 2:  # Only cache if less than half limit
                self.query_cache.set(cache_key, data)
                self._current_cache_memory += estimated_size

    def _estimate_data_size(self, data: Any) -> int:
        """Estimate memory size of data (simplified)."""
        try:
            if isinstance(data, (list, tuple)):
                return len(data) * 500  # Rough estimate: 500 bytes per item
            elif isinstance(data, dict):
                return len(str(data))
            else:
                return len(str(data))
        except Exception:
            return 500  # Default estimate

    async def _acquire(self) -> pyodbc.Connection:
        """Enhanced connection acquisition with health checking."""
        async with self._lock:
            if self.pool:
                conn = self.pool.pop()
                # Quick health check on acquired connection
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                except Exception:
                    # Connection is dead, create a new one
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._create_connection_optimized()
                    
                metrics.set("sqlserver_pool_size", float(len(self.pool)))
                return conn
            else:
                # Pool is empty, create new connection
                conn = self._create_connection_optimized()
                metrics.set("sqlserver_pool_size", float(len(self.pool)))
                return conn

    async def _release(self, conn: pyodbc.Connection) -> None:
        """Enhanced connection release with pool size management."""
        async with self._lock:
            # Only return connection to pool if pool is not at capacity
            if len(self.pool) < self.max_pool:
                self.pool.append(conn)
            else:
                # Pool is full, close the connection
                try:
                    conn.close()
                except Exception:
                    pass
            
            metrics.set("sqlserver_pool_size", float(len(self.pool)))

    # Enhanced versions of existing methods with memory management
    async def fetch(
        self, query: str, *params: Any, traceparent: str | None = None
    ) -> Iterable[dict]:
        """Enhanced fetch with memory management."""
        return await self.fetch_optimized_with_memory_management(
            query, *params, is_prepared=False, traceparent=traceparent
        )

    async def execute_optimized(
        self,
        query: str,
        *params: Any,
        is_prepared: bool = False,
        traceparent: str | None = None,
    ) -> int:
        """Optimized execute with performance tracking and memory management."""
        self._operation_count += 1
        
        # Periodic memory check
        if self._operation_count % self._memory_check_frequency == 0:
            await self._check_database_memory()
        
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            row_count = cursor.rowcount if cursor.rowcount != -1 else 1
            cursor.close()
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            if is_prepared:
                metrics.inc("sqlserver_prepared_queries")
            latencies.record("sqlserver_query", duration)
            if duration > self.cfg.threshold_ms:
                logger.warning(
                    "slow_query",
                    extra={"query": query, "duration_ms": duration},
                )
            await self.circuit_breaker.record_success()
            return row_count
            
        except pyodbc.Error as e:
            try:
                conn.close()
            except Exception:
                pass
            conn = self._create_connection_optimized()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            row_count = cursor.rowcount if cursor.rowcount != -1 else 1
            cursor.close()
            await self.circuit_breaker.record_success()
            return row_count
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute(
        self, query: str, *params: Any, traceparent: str | None = None
    ) -> int:
        """Enhanced execute with optimization detection."""
        return await self.execute_optimized(
            query, *params, is_prepared=False, traceparent=traceparent
        )

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

    async def execute_many_optimized(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        batch_size: int = 1000,
        traceparent: str | None = None,
    ) -> int:
        """Optimized execute_many with batching and parallel processing."""
        return await self.execute_many_with_memory_management(
            query,
            params_seq,
            concurrency=concurrency,
            batch_size=batch_size,
            traceparent=traceparent,
        )

    async def bulk_insert_tvp_optimized(
        self,
        table: str,
        columns: Iterable[str],
        rows: Iterable[Iterable[Any]],
        *,
        chunk_size: int = 5000,
    ) -> int:
        """Enhanced TVP bulk insert with chunking and memory optimization."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        rows_list = list(rows)
        if not rows_list:
            return 0
        
        total_inserted = 0
        columns_list = list(columns)
        
        # Process in chunks for memory efficiency
        for i in range(0, len(rows_list), chunk_size):
            chunk = rows_list[i:i + chunk_size]
            
            # Monitor memory before processing
            initial_memory = self._get_process_memory()
            
            conn = await self._acquire()
            placeholders = ", ".join(["?"] * len(columns_list))
            tvp_insert = f"INSERT INTO {table} ({', '.join(columns_list)}) SELECT * FROM ?"
            fallback_insert = f"INSERT INTO {table} ({', '.join(columns_list)}) VALUES ({placeholders})"
            
            try:
                start = time.perf_counter()
                cursor = conn.cursor()
                
                try:
                    # Try TVP first (faster for large datasets)
                    if hasattr(cursor, "setinputsizes") and hasattr(pyodbc, "SQL_STRUCTURED"):
                        tvp_name = f"{table}_type"
                        cursor.setinputsizes([(pyodbc.SQL_STRUCTURED, tvp_name)])
                        cursor.fast_executemany = True
                        cursor.execute(tvp_insert, (chunk,))
                    else:
                        raise AttributeError("TVP not supported")
                except Exception:
                    # Fallback to regular bulk insert
                    cursor.fast_executemany = True
                    cursor.executemany(fallback_insert, chunk)
                
                conn.commit()
                cursor.close()
                
                duration = (time.perf_counter() - start) * 1000
                metrics.inc("sqlserver_query_ms", duration)
                metrics.inc("sqlserver_query_count")
                metrics.inc("sqlserver_tvp_operations")
                latencies.record("sqlserver_query", duration)
                if duration > self.cfg.threshold_ms:
                    logger.warning(
                        "slow_query",
                        extra={"query": query, "duration_ms": duration},
                    )
                await self.circuit_breaker.record_success()
                
                total_inserted += len(chunk)
                
                # Check memory growth
                final_memory = self._get_process_memory()
                memory_growth = final_memory - initial_memory
                
                if memory_growth > 50:  # More than 50MB growth
                    print(f"High memory growth in TVP insert: {memory_growth:.1f}MB")
                    await self._routine_memory_cleanup()
                
            except pyodbc.Error:
                try:
                    conn.close()
                except Exception:
                    pass
                # Retry with new connection
                conn = self._create_connection_optimized()
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.executemany(fallback_insert, chunk)
                conn.commit()
                cursor.close()
                await self.circuit_breaker.record_success()
                total_inserted += len(chunk)
            except Exception as e:
                await self.circuit_breaker.record_failure()
                raise QueryError(str(e)) from e
            finally:
                await self._release(conn)
        
        return total_inserted

    async def bulk_insert_tvp(
        self,
        table: str,
        columns: Iterable[str],
        rows: Iterable[Iterable[Any]],
    ) -> int:
        """Override to use optimized version."""
        return await self.bulk_insert_tvp_optimized(table, columns, rows)

    async def health_check(self) -> bool:
        """Enhanced health check with detailed diagnostics."""
        if not await self.circuit_breaker.allow():
            return False
        
        try:
            # Check multiple connections
            healthy_connections = 0
            total_connections = min(3, len(self.pool))  # Check up to 3 connections
            
            for i in range(total_connections):
                try:
                    conn = await self._acquire()
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    await self._release(conn)
                    healthy_connections += 1
                except Exception:
                    pass
            
            # Health check passes if majority of tested connections work
            is_healthy = healthy_connections >= (total_connections // 2 + 1) if total_connections > 0 else False
            
            if is_healthy:
                await self.circuit_breaker.record_success()
            else:
                await self.circuit_breaker.record_failure()
            
            # Update health metrics
            metrics.set("sqlserver_healthy_connections", float(healthy_connections))
            metrics.set("sqlserver_health_check_ratio", 
                       float(healthy_connections) / float(total_connections) if total_connections > 0 else 0.0)
            
            return is_healthy
            
        except Exception:
            await self.circuit_breaker.record_failure()
            return False

    def report_pool_status(self) -> None:
        """Enhanced pool status reporting with optimization metrics."""
        report_pool_metrics(
            "sqlserver",
            size=len(self.pool),
            min_size=self.min_pool,
            max_size=self.max_pool,
            connections_created=self._connection_create_count,
            prepared_statements=len(self._prepared),
            cache_memory=self._current_cache_memory,
        )

    async def adjust_pool_size(self) -> None:
        """Enhanced dynamic pool adjustment based on load and performance."""
        try:
            load = os.getloadavg()[0] if hasattr(os, 'getloadavg') else 0
        except Exception:
            load = 0
        
        # Get memory status
        memory_status = sql_memory_monitor.check_memory()
        memory_mb = memory_status.get("memory_mb", 0)
        
        async with self._lock:
            current_size = len(self.pool)
            target_size = current_size
            
            # Adjust based on system load and memory
            if (load > 4 or memory_mb > 1000) and current_size > self.min_pool:
                # High load or memory - reduce pool size
                target_size = max(self.min_pool, current_size - 2)
                excess = current_size - target_size
                for _ in range(excess):
                    if self.pool:
                        conn = self.pool.pop()
                        try:
                            conn.close()
                        except Exception:
                            pass
                metrics.inc("sqlserver_pool_size_decreased")
                
            elif load < 1 and memory_mb < 500 and current_size < self.max_pool:
                # Low load and memory - potentially increase pool size
                pool_utilization = (self.max_pool - current_size) / self.max_pool
                if pool_utilization < 0.5:  # Pool is more than 50% utilized
                    target_size = min(self.max_pool, current_size + 1)
                    try:
                        new_conn = self._create_connection_optimized()
                        self.pool.append(new_conn)
                        metrics.inc("sqlserver_pool_size_increased")
                    except Exception as e:
                        print(f"Warning: Failed to expand SQL Server pool: {e}")
            
            metrics.set("sqlserver_pool_adjusted_size", float(len(self.pool)))

    async def close_with_memory_cleanup(self) -> None:
        """Close with comprehensive memory cleanup."""
        # Clear all caches and prepared statements
        if hasattr(self.query_cache, 'store'):
            self.query_cache.store.clear()
        
        self._prepared.clear()
        self._prepared_on_connections.clear()
        self._connection_memory_tracking.clear()
        
        # Clear memory pool
        self._memory_pool.cleanup_all()
        
        # Close all connections with cleanup
        async with self._lock:
            while self.pool:
                conn = self.pool.pop()
                try:
                    conn.close()
                except Exception:
                    pass
        
        # Force garbage collection
        gc.collect()
        
        metrics.inc("sql_closes_with_cleanup")

    async def close(self) -> None:
        """Enhanced cleanup with connection health tracking."""
        await self.close_with_memory_cleanup()

    def get_memory_stats(self) -> Dict[str, Any]:
        """Get comprehensive memory statistics."""
        return {
            "process_memory_mb": self._get_process_memory(),
            "cache_memory_mb": self._current_cache_memory / 1024 / 1024,
            "cache_max_memory_mb": self._cache_max_memory / 1024 / 1024,
            "prepared_statements_count": len(self._prepared),
            "connection_memory_tracking": len(self._connection_memory_tracking),
            "memory_pool_stats": self._memory_pool.get_stats(),
            "memory_status": sql_memory_monitor.check_memory(),
        }

    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get comprehensive optimization statistics."""
        return {
            "connection_pool": {
                "current_size": len(self.pool),
                "min_size": self.min_pool,
                "max_size": self.max_pool,
                "connections_created": self._connection_create_count,
                "utilization": (self.max_pool - len(self.pool)) / self.max_pool if self.max_pool > 0 else 0.0,
            },
            "prepared_statements": {
                "count": len(self._prepared),
                "statements": list(self._prepared.keys()),
            },
            "caching": {
                "cache_size": len(self.query_cache.store) if hasattr(self.query_cache, 'store') else 0,
                "cache_ttl": self.query_cache.ttl,
                "cache_memory_mb": self._current_cache_memory / 1024 / 1024,
                "cache_max_memory_mb": self._cache_max_memory / 1024 / 1024,
            },
            "memory_management": {
                "process_memory_mb": self._get_process_memory(),
                "memory_checks_performed": self._operation_count // self._memory_check_frequency,
                "memory_pool_stats": self._memory_pool.get_stats(),
            },
            "optimization_features": {
                "connection_warming": True,
                "prepared_statements": True,
                "query_caching": True,
                "bulk_operations": True,
                "tvp_support": True,
                "health_monitoring": True,
                "adaptive_pooling": True,
                "bulk_validation": True,
                "memory_management": True,
                "emergency_cleanup": True,
            },
            "performance_optimizations": {
                "fast_executemany": True,
                "connection_reuse": True,
                "batch_processing": True,
                "parallel_execution": True,
                "circuit_breaker": True,
                "memory_monitoring": True,
                "adaptive_batch_sizing": True,
            }
        }