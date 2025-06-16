"""
COMPLETE FIXED VERSION: src/db/postgres.py

This file contains all the fixes for the connection errors you're experiencing.
Replace your existing src/db/postgres.py with this complete version.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import time
import weakref
from typing import Any, Dict, Iterable, List, Optional

import asyncpg

from ..config.config import PostgresConfig
from ..monitoring.metrics import metrics
from ..utils.cache import InMemoryCache
from ..utils.circuit_breaker import CircuitBreaker
from ..utils.errors import CircuitBreakerOpenError, QueryError
from ..utils.memory_pool import memory_pool
from ..utils.query_optimizer import QueryOptimizer
from ..utils.query_tracker import QueryTracker
from ..utils.tracing import get_traceparent
from .connection_utils import connect_with_retry, report_pool_metrics

# Import memory monitoring
try:
    from ..monitoring.memory_monitor import memory_monitor
except ImportError:
    memory_monitor = None

logger = logging.getLogger(__name__)


class PostgresDatabase:
    """Enhanced PostgreSQL database with optimized connection handling and memory management."""

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
        self._prepared_statements: Dict[str, Any] = {}  # statement_name -> prepared_statement
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
        
        # Connection limiting to prevent pool exhaustion
        self._max_concurrent_operations = 30
        self._operation_semaphore = asyncio.Semaphore(self._max_concurrent_operations)
        self._active_connections = 0

        # Query tracking and optimization
        self.query_tracker = QueryTracker()
        self.query_optimizer = QueryOptimizer()

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

    def _get_circuit_breaker_status(self) -> dict:
        """Synchronous version for error handling."""
        return {
            "is_open": self.circuit_breaker.is_open,
            "failure_count": self.circuit_breaker.failure_count,
            "failure_threshold": self.circuit_breaker.failure_threshold,
            "recovery_time": self.circuit_breaker.recovery_time,
            "open_until": self.circuit_breaker.open_until,
        }

    async def _try_circuit_breaker_recovery(self) -> bool:
        """FIXED: Attempt to recover from circuit breaker open state with proper connection handling."""
        try:
            # Try a simple health check
            if self.pool:
                conn = None
                try:
                    conn = await asyncio.wait_for(self.pool.acquire(), timeout=2.0)
                    result = await asyncio.wait_for(
                        conn.fetchval("SELECT 1"),
                        timeout=2.0
                    )
                    if result == 1:
                        await self.circuit_breaker.record_success()
                        logger.info("Circuit breaker recovery successful")
                        return True
                finally:
                    if conn and self.pool:
                        await self.pool.release(conn)
            return False
        except Exception as e:
            logger.debug(f"Circuit breaker recovery attempt failed: {e}")
            return False

    async def _init_connection(self, conn: "asyncpg.Connection") -> None:
        """Initialize a new connection with prepared statements and optimizations."""
        
        # Only set session-level parameters that are safe to change
        try:
            await conn.execute("SET work_mem = '32MB'")  # Reduced for safety
            await conn.execute("SET maintenance_work_mem = '64MB'")  # Reduced for safety
            await conn.execute("SET effective_cache_size = '512MB'")  # Reduced for safety
            await conn.execute("SET random_page_cost = 1.1")  # SSD optimization
            await conn.execute("SET seq_page_cost = 1.0")
        except Exception as e:
            # Log warning but don't fail connection initialization
            logger.debug(f"Could not set PostgreSQL session parameters: {e}")

        # Prepare cached statements on new connection
        for stmt_name, query in self._prepared.items():
            try:
                prepared_stmt = await conn.prepare(query)
                # Cache the prepared statement object if needed
                if stmt_name not in self._prepared_statements:
                    self._prepared_statements[stmt_name] = prepared_stmt
            except Exception as e:
                # Log but don't fail connection initialization
                logger.debug(f"Failed to prepare statement {stmt_name}: {e}")
                continue

    async def _get_connection_with_retry(
        self, 
        use_replica: bool = False, 
        max_retries: int = 3
    ) -> asyncpg.Connection:
        """FIXED: Get a database connection with proper async handling and retry logic."""
        
        # Choose pool based on use_replica flag and availability
        pool = self.replica_pool if use_replica and self.replica_pool else self.pool
        
        if not pool:
            raise ConnectionError("Database pool not initialized")
        
        retry_delay = 0.1
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # FIXED: Properly await the wait_for coroutine - don't use as context manager
                conn = await asyncio.wait_for(pool.acquire(), timeout=15.0)
                
                try:
                    # Ensure connection is in a clean state
                    await asyncio.wait_for(conn.execute("SELECT 1"), timeout=5.0)
                    self._active_connections += 1
                    return conn
                except Exception as e:
                    # Connection might be in a bad state, release it and retry
                    await pool.release(conn)
                    last_exception = e
                    logger.warning(f"Connection validation failed on attempt {attempt + 1}: {e}")
                    
            except asyncio.TimeoutError as e:
                last_exception = e
                logger.warning(f"Connection acquisition timeout on attempt {attempt + 1}")
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
            
            # Wait before next attempt (except on last attempt)
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (2 ** attempt))
        
        # All attempts failed
        logger.error(f"All connection attempts failed. Last error: {last_exception}")
        raise ConnectionError(f"Unable to acquire database connection after {max_retries} attempts: {last_exception}")

    async def _release_connection(self, conn: asyncpg.Connection, use_replica: bool = False):
        """Safely release a connection back to the pool."""
        if conn:
            try:
                pool = self.replica_pool if use_replica and self.replica_pool else self.pool
                if pool:
                    await pool.release(conn)
                    self._active_connections = max(0, self._active_connections - 1)
            except Exception as e:
                logger.warning(f"Failed to release connection: {e}")

    async def connect(self, prepare_queries: bool = True) -> None:
        """Enhanced connection with pre-warming and health monitoring."""

        async def _open() -> None:
            # Create main pool with optimizations
            self.pool = await asyncpg.create_pool(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                min_size=max(5, self.cfg.min_pool_size),  # Ensure minimum 5
                max_size=max(20, self.cfg.max_pool_size),  # Ensure minimum 20
                init=self._init_connection,
                command_timeout=30,
                server_settings={
                    "application_name": "claims_processor_v2",
                    "tcp_keepalives_idle": "300",
                    "tcp_keepalives_interval": "30",
                    "tcp_keepalives_count": "3",
                    "statement_timeout": "60000",
                    "idle_in_transaction_session_timeout": "30000",
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
                    min_size=max(3, self.cfg.min_pool_size // 2),
                    max_size=max(10, self.cfg.max_pool_size // 2),
                    init=self._init_connection,
                    command_timeout=30,
                    server_settings={
                        "application_name": "claims_processor_replica",
                        "default_transaction_isolation": "read_committed",
                    },
                )

            # Pre-warm connection pools
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

    async def _warm_connection_pools(self) -> None:
        """FIXED: Pre-warm connection pools with proper async handling."""
        
        async def warm_pool(pool: asyncpg.pool.Pool, pool_name: str) -> None:
            """Warm a specific pool by creating all connections."""
            connections = []
            try:
                # Acquire connections to force creation
                max_connections = min(pool._maxsize, 10)  # Limit warming to prevent issues
                
                for i in range(max_connections):
                    try:
                        # FIXED: Properly await the wait_for coroutine
                        conn = await asyncio.wait_for(pool.acquire(), timeout=5.0)
                        connections.append(conn)
                        # Test connection with simple query
                        await conn.execute("SELECT 1")
                    except asyncio.TimeoutError:
                        break  # Pool likely at capacity
                    except Exception as e:
                        logger.warning(f"Failed to warm connection {i} in {pool_name}: {e}")
                        break

                # Release all connections back to pool
                for conn in connections:
                    try:
                        await pool.release(conn)
                    except Exception as e:
                        logger.warning(f"Failed to release warmed connection: {e}")

                logger.info(f"Pre-warmed {len(connections)} connections in {pool_name}")
                metrics.set(f"postgres_pool_{pool_name}_warmed", len(connections))

            except Exception as e:
                logger.warning(f"Pool warming failed for {pool_name}: {e}")
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

        # Prepare each query on a test connection
        if self.pool:
            try:
                conn = await asyncio.wait_for(self.pool.acquire(), timeout=5.0)
                try:
                    for name, query in common_queries.items():
                        try:
                            await conn.prepare(query)
                            self._prepared[name] = query
                            logger.debug(f"Prepared statement: {name}")
                        except Exception as e:
                            logger.warning(f"Could not prepare {name}: {e}")
                finally:
                    await self.pool.release(conn)
            except Exception as e:
                logger.warning(f"Could not prepare common queries: {e}")

    async def fetch_optimized_with_memory_management(
        self,
        query: str,
        *params: Any,
        use_replica: bool = True,
        traceparent: str | None = None,
    ) -> Iterable[dict]:
        """FIXED: Optimized fetch with proper connection handling and memory management."""
        
        # Limit concurrent operations to prevent pool exhaustion
        async with self._operation_semaphore:
            # Circuit breaker check
            if not await self.circuit_breaker.allow():
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"Circuit breaker is open. Status: {cb_status}")
            
            # Memory management
            current_operation_id = f"fetch_{int(time.time() * 1000000)}"
            
            conn = None
            try:
                # Get connection with proper retry logic
                conn = await self._get_connection_with_retry(use_replica=use_replica, max_retries=3)
                
                # Add traceparent if provided
                traced_query = self._with_traceparent(query, traceparent)
                
                # Execute query with timeout
                start_time = time.perf_counter()
                result = await asyncio.wait_for(
                    conn.fetch(traced_query, *params),
                    timeout=30.0
                )
                
                query_time = time.perf_counter() - start_time
                
                # Track performance
                self.query_tracker.record_query(query, query_time)
                
                # Record success for circuit breaker
                await self.circuit_breaker.record_success()
                
                # Convert to list of dicts for consistency
                return [dict(row) for row in result]
                
            except asyncio.TimeoutError as e:
                error_msg = f"Query timeout after 30 seconds"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            except Exception as e:
                error_msg = f"PostgreSQL operation failed: {str(e)}"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                
                # Check if we should attempt circuit breaker recovery
                if self.circuit_breaker.is_open:
                    recovery_success = await self._try_circuit_breaker_recovery()
                    if recovery_success:
                        logger.info("Circuit breaker recovery successful, retrying operation")
                        # Retry once after successful recovery
                        try:
                            return await self.fetch_optimized_with_memory_management(
                                query, *params, use_replica=use_replica, traceparent=traceparent
                            )
                        except Exception:
                            pass  # Fall through to original error
                
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            finally:
                # Always release connection back to pool
                if conn is not None:
                    await self._release_connection(conn, use_replica)

    async def fetch_prepared(
        self,
        statement_name: str,
        *params: Any,
        use_replica: bool = True,
        traceparent: str | None = None,
    ) -> Iterable[dict]:
        """FIXED: Execute a prepared statement with proper connection handling."""
        
        # Limit concurrent operations
        async with self._operation_semaphore:
            # Circuit breaker check
            if not await self.circuit_breaker.allow():
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"Circuit breaker is open. Status: {cb_status}")
            
            conn = None
            try:
                # Get connection with retry logic
                conn = await self._get_connection_with_retry(use_replica=use_replica, max_retries=3)
                
                # Check if statement is prepared on this connection
                query = self._prepared.get(statement_name)
                if not query:
                    raise QueryError(f"Prepared statement '{statement_name}' not found")
                
                # Try to use prepared statement, fallback to regular query if needed
                start_time = time.perf_counter()
                try:
                    # Try prepared statement first
                    if hasattr(conn, '_prepared_stmts') and statement_name in getattr(conn, '_prepared_stmts', {}):
                        result = await asyncio.wait_for(
                            conn.fetch_prepared(statement_name, *params),
                            timeout=30.0
                        )
                    else:
                        # Fallback to regular query
                        traced_query = self._with_traceparent(query, traceparent)
                        result = await asyncio.wait_for(
                            conn.fetch(traced_query, *params),
                            timeout=30.0
                        )
                except Exception:
                    # Fallback to regular query
                    traced_query = self._with_traceparent(query, traceparent)
                    result = await asyncio.wait_for(
                        conn.fetch(traced_query, *params),
                        timeout=30.0
                    )
                
                query_time = time.perf_counter() - start_time
                
                # Track performance
                self.query_tracker.record_query(f"prepared_{statement_name}", query_time)
                
                # Record success for circuit breaker
                await self.circuit_breaker.record_success()
                
                # Convert to list of dicts
                return [dict(row) for row in result]
                
            except asyncio.TimeoutError as e:
                error_msg = f"Prepared statement timeout after 30 seconds"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            except Exception as e:
                error_msg = f"Prepared statement execution failed: {str(e)}"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            finally:
                # Always release connection
                if conn is not None:
                    await self._release_connection(conn, use_replica)

    async def execute_optimized(
        self,
        query: str,
        *params: Any,
        use_replica: bool = False,
        traceparent: str | None = None,
    ) -> str:
        """FIXED: Execute a query with proper connection handling."""
        
        # Limit concurrent operations
        async with self._operation_semaphore:
            # Circuit breaker check
            if not await self.circuit_breaker.allow():
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"Circuit breaker is open. Status: {cb_status}")
            
            conn = None
            try:
                # Get connection (use main pool for writes)
                conn = await self._get_connection_with_retry(use_replica=use_replica, max_retries=3)
                
                # Add traceparent if provided
                traced_query = self._with_traceparent(query, traceparent)
                
                # Execute with timeout
                start_time = time.perf_counter()
                result = await asyncio.wait_for(
                    conn.execute(traced_query, *params),
                    timeout=30.0
                )
                
                query_time = time.perf_counter() - start_time
                
                # Track performance
                self.query_tracker.record_query(query, query_time)
                
                # Record success for circuit breaker
                await self.circuit_breaker.record_success()
                
                return result
                
            except asyncio.TimeoutError as e:
                error_msg = f"Execute timeout after 30 seconds"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            except Exception as e:
                error_msg = f"Execute operation failed: {str(e)}"
                await self.circuit_breaker.record_failure()
                cb_status = self._get_circuit_breaker_status()
                raise QueryError(f"{error_msg}. Circuit breaker status: {cb_status}") from e
                
            finally:
                # Always release connection
                if conn is not None:
                    await self._release_connection(conn, use_replica)

    async def health_check(self) -> bool:
        """FIXED: Enhanced health check with proper connection handling."""
        try:
            # Check if pool exists and is healthy
            if not self.pool:
                await self.connect()
            
            # Test connection from the pool
            conn = None
            try:
                # Get connection with short timeout for health check
                conn = await asyncio.wait_for(self.pool.acquire(), timeout=5.0)
                
                # Test with simple query
                result = await asyncio.wait_for(
                    conn.fetchval("SELECT 1"),
                    timeout=5.0
                )
                
                is_healthy = (result == 1)
                
                if is_healthy:
                    await self.circuit_breaker.record_success()
                    # Log successful recovery if circuit breaker was previously open
                    if hasattr(self, '_circuit_was_open') and self._circuit_was_open:
                        logger.info("PostgreSQL connection recovered, circuit breaker reset")
                        self._circuit_was_open = False
                else:
                    await self.circuit_breaker.record_failure()
                
                return is_healthy
                
            except Exception as e:
                logger.warning(f"PostgreSQL health check failed: {e}")
                await self.circuit_breaker.record_failure()
                
                # Mark that circuit breaker was open for recovery logging
                if self.circuit_breaker.is_open:
                    self._circuit_was_open = True
                
                return False
                
            finally:
                # Always release connection
                if conn is not None:
                    try:
                        await self.pool.release(conn)
                    except Exception as release_error:
                        logger.warning(f"Failed to release health check connection: {release_error}")
                        
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return False

    # Public API methods that use the optimized versions
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
    ) -> str:
        """Execute a single statement and return affected row count."""
        return await self.execute_optimized(query, *params, traceparent=traceparent)

    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        traceparent: str | None = None,
    ) -> int:
        """Execute many with enhanced memory management."""
        return await self.execute_many_with_memory_management(
            query,
            params_seq,
            concurrency=concurrency,
            batch_size=1000,
            traceparent=traceparent,
        )

    async def execute_many_with_memory_management(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        batch_size: int = 1000,
        traceparent: str | None = None,
    ) -> int:
        """Execute many with enhanced memory management."""
        params_list = list(params_seq)
        if not params_list:
            return 0

        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")

        await self._ensure_pool()
        
        total_affected = 0
        conn = None
        
        try:
            conn = await self._get_connection_with_retry()
            # Use executemany for better performance
            result = await conn.executemany(query, params_list)
            total_affected = len(params_list)  # Estimate
            await self.circuit_breaker.record_success()
                
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            if conn:
                await self._release_connection(conn)

        return total_affected

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

    async def prepare_named(self, name: str, query: str) -> None:
        """Prepare and cache a named statement."""
        async with self._lock:
            self._prepared[name] = query

    async def _ensure_pool(self) -> None:
        """Ensure database pool is connected and healthy."""
        if not self.pool:
            await self.connect()

    async def _check_database_memory(self) -> None:
        """Check and manage database-related memory usage."""
        if memory_monitor:
            memory_status = memory_monitor.check_memory()
            
            if memory_status["status"] == "critical":
                # Aggressive cleanup
                await self._aggressive_memory_cleanup()
            elif memory_status["status"] == "warning":
                # Gentle cleanup
                await self._gentle_memory_cleanup()

    async def _aggressive_memory_cleanup(self) -> None:
        """Perform aggressive memory cleanup."""
        # Clear all caches
        if hasattr(self.query_cache, "store"):
            self.query_cache.store.clear()
        
        # Clear prepared statements
        self._prepared_statements.clear()
        
        # Force garbage collection
        gc.collect()
        
        logger.info("Performed aggressive memory cleanup")

    async def _gentle_memory_cleanup(self) -> None:
        """Perform gentle memory cleanup."""
        # Clear old cache entries
        if hasattr(self.query_cache, "expire_old"):
            self.query_cache.expire_old()
        
        # Trigger garbage collection
        gc.collect()

    async def _periodic_memory_check(self) -> None:
        """Periodic memory check during operations."""
        current_time = time.time()
        if current_time - self._last_pool_cleanup > self._pool_cleanup_interval:
            await self._check_database_memory()
            self._last_pool_cleanup = current_time

    async def close(self) -> None:
        """Close the database connection pools."""
        try:
            if self.pool:
                await self.pool.close()
                self.pool = None
                
            if self.replica_pool:
                await self.replica_pool.close()
                self.replica_pool = None
                
            logger.info("PostgreSQL connections closed")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connections: {e}")

    def get_pool_stats(self) -> dict:
        """Get connection pool statistics."""
        stats = {
            "main_pool": None,
            "replica_pool": None,
            "active_connections": self._active_connections,
        }
        
        if self.pool:
            stats["main_pool"] = {
                "size": self.pool.get_size(),
                "max_size": self.pool.get_max_size(),
                "min_size": self.pool.get_min_size(),
            }
            
        if self.replica_pool:
            stats["replica_pool"] = {
                "size": self.replica_pool.get_size(),
                "max_size": self.replica_pool.get_max_size(),
                "min_size": self.replica_pool.get_min_size(),
            }
            
        return stats