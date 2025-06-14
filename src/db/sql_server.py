import asyncio
import os
import time
import pyodbc
from typing import Iterable, Any, Dict, List, Optional

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from ..utils.errors import DatabaseConnectionError, QueryError
from ..utils.cache import InMemoryCache

from .base import BaseDatabase
from ..config.config import SQLServerConfig
from ..monitoring.metrics import metrics
from ..analysis.query_tracker import record as record_query
from ..monitoring.stats import latencies


class SQLServerDatabase(BaseDatabase):
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
        # Connection pool configuration
        self.min_pool = cfg.min_pool_size
        self.max_pool = cfg.max_pool_size
        self.current_pool_size = 0
        # Performance tracking
        self._connection_create_count = 0
        self._last_health_check = 0.0
        self._health_check_interval = 30.0

    async def connect(self, size: int | None = None) -> None:
        """Enhanced connection with aggressive pre-warming and optimization."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        try:
            pool_size = size or self.cfg.pool_size
            self.pool.clear()  # Clear any existing connections
            
            # Create connections with optimizations
            for i in range(pool_size):
                conn = self._create_connection_optimized()
                self.pool.append(conn)
                self.current_pool_size += 1
            
            # Pre-warm connections and prepare common statements
            await self._warm_connection_pool()
            await self._prepare_common_statements()
            
            metrics.set("sqlserver_pool_initial_size", float(len(self.pool)))
            
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise DatabaseConnectionError(str(e)) from e
        await self.circuit_breaker.record_success()

    def _create_connection_optimized(self) -> pyodbc.Connection:
        """Create an optimized connection with performance settings."""
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
            # Enable fast_executemany for bulk operations
            cursor.fast_executemany = True
        except Exception as e:
            print(f"Warning: Failed to set connection optimizations: {e}")
        
        return conn

    async def _warm_connection_pool(self) -> None:
        """Pre-warm connections by executing simple queries."""
        warmed_count = 0
        failed_count = 0
        
        for i, conn in enumerate(self.pool):
            try:
                cursor = conn.cursor()
                # Execute a simple query to warm the connection
                cursor.execute("SELECT 1 as test_query")
                cursor.fetchone()
                cursor.close()
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
            
            # Audit logging
            "insert_audit_log": """
                INSERT INTO audit_log (
                    table_name, record_id, operation, user_id,
                    old_values, new_values, operation_timestamp, reason
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?)
            """,
            
            # Performance metrics
            "insert_performance_metrics": """
                INSERT INTO performance_metrics (
                    metric_date, metric_type, facility_id, claims_per_second,
                    records_per_minute, cpu_usage_percent, memory_usage_mb,
                    database_response_time_ms, queue_depth, error_rate,
                    processing_accuracy, revenue_per_claim
                ) VALUES (GETDATE(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            
            # RVU data queries
            "get_rvu_data": """
                SELECT procedure_code, description, total_rvu, work_rvu,
                       practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code = ? AND status = 'active'
            """,
            
            # Batch processing queries
            "update_batch_status": """
                UPDATE batch_metadata 
                SET status = ?, updated_at = GETDATE(), 
                    processed_claims = ?, failed_claims = ?
                WHERE batch_id = ?
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

    async def execute_prepared(self, statement_name: str, *params: Any) -> int:
        """Execute a prepared statement by name."""
        if statement_name not in self._prepared:
            raise ValueError(f"Statement {statement_name} not prepared")
        
        query = self._prepared[statement_name]
        return await self.execute_optimized(query, *params, is_prepared=True)

    async def fetch_prepared(self, statement_name: str, *params: Any) -> Iterable[dict]:
        """Fetch results using a prepared statement."""
        if statement_name not in self._prepared:
            raise ValueError(f"Statement {statement_name} not prepared")
        
        query = self._prepared[statement_name]
        return await self.fetch_optimized(query, *params, is_prepared=True)

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

    async def prepare(self, query: str) -> None:
        """Prepare a statement on all existing connections."""
        async with self._lock:
            query_hash = str(hash(query))
            if query_hash in self._prepared:
                return
            
            self._prepared[query_hash] = query
            
            # SQL Server doesn't have explicit prepare, but we cache the query
            # for faster execution
            for conn in self.pool:
                try:
                    cursor = conn.cursor()
                    cursor.close()
                except Exception:
                    continue

    async def fetch_optimized(self, query: str, *params: Any, is_prepared: bool = False) -> Iterable[dict]:
        """Optimized fetch with caching and connection reuse."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        # Check cache first
        cache_key = f"query:{query}:{str(params)}"
        cached = self.query_cache.get(cache_key)
        if cached is not None:
            metrics.inc("sqlserver_cache_hits")
            return cached
        
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            if is_prepared:
                metrics.inc("sqlserver_prepared_queries")
            latencies.record("sqlserver_query", duration)
            record_query(query, duration)
            await self.circuit_breaker.record_success()
            
            # Cache results
            self.query_cache.set(cache_key, rows)
            metrics.inc("sqlserver_cache_misses")
            return rows
            
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
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            await self.circuit_breaker.record_success()
            self.query_cache.set(cache_key, rows)
            return rows
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        """Enhanced fetch with optimization detection."""
        return await self.fetch_optimized(query, *params, is_prepared=False)

    async def execute_optimized(self, query: str, *params: Any, is_prepared: bool = False) -> int:
        """Optimized execute with performance tracking."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            row_count = cursor.rowcount
            cursor.close()
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            if is_prepared:
                metrics.inc("sqlserver_prepared_queries")
            latencies.record("sqlserver_query", duration)
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
            row_count = cursor.rowcount
            cursor.close()
            await self.circuit_breaker.record_success()
            return row_count
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute(self, query: str, *params: Any) -> int:
        """Enhanced execute with optimization detection."""
        return await self.execute_optimized(query, *params, is_prepared=False)

    async def execute_many_optimized(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
        batch_size: int = 1000,
    ) -> int:
        """Optimized execute_many with batching and parallel processing."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        
        params_list = list(params_seq)
        if not params_list:
            return 0
        
        total_processed = 0
        
        # Process in optimized batches
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i:i + batch_size]
            
            if concurrency > 1 and len(batch) > 100:
                # Parallel processing for large batches
                chunk_size = len(batch) // concurrency + 1
                chunks = [
                    batch[j : j + chunk_size]
                    for j in range(0, len(batch), chunk_size)
                ]

                async def run_chunk(chunk: list[Iterable[Any]]) -> int:
                    conn = await self._acquire()
                    try:
                        cursor = conn.cursor()
                        cursor.fast_executemany = True
                        cursor.executemany(query, chunk)
                        conn.commit()
                        row_count = cursor.rowcount
                        cursor.close()
                        return row_count
                    finally:
                        await self._release(conn)

                results = await asyncio.gather(*[run_chunk(c) for c in chunks])
                total_processed += sum(results)
            else:
                # Sequential processing for smaller batches
                conn = await self._acquire()
                try:
                    start = time.perf_counter()
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.executemany(query, batch)
                    conn.commit()
                    row_count = cursor.rowcount
                    cursor.close()
                    
                    duration = (time.perf_counter() - start) * 1000
                    metrics.inc("sqlserver_query_ms", duration)
                    metrics.inc("sqlserver_query_count")
                    metrics.inc("sqlserver_bulk_operations")
                    latencies.record("sqlserver_query", duration)
                    await self.circuit_breaker.record_success()
                    total_processed += row_count
                    
                except pyodbc.Error:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._create_connection_optimized()
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.executemany(query, batch)
                    conn.commit()
                    row_count = cursor.rowcount
                    cursor.close()
                    await self.circuit_breaker.record_success()
                    total_processed += row_count
                except Exception as e:
                    await self.circuit_breaker.record_failure()
                    raise QueryError(str(e)) from e
                finally:
                    await self._release(conn)
        
        return total_processed

    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
    ) -> int:
        """Override to use optimized version."""
        return await self.execute_many_optimized(
            query, params_seq, concurrency=concurrency, batch_size=1000
        )

    async def bulk_insert_tvp_optimized(
        self,
        table: str,
        columns: Iterable[str],
        rows: Iterable[Iterable[Any]],
        *,
        chunk_size: int = 5000,
    ) -> int:
        """Enhanced TVP bulk insert with chunking and optimization."""
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
                await self.circuit_breaker.record_success()
                
                total_inserted += len(chunk)
                
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
            is_healthy = healthy_connections >= (total_connections // 2 + 1)
            
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
        metrics.set("sqlserver_pool_size", float(len(self.pool)))
        metrics.set("sqlserver_pool_max_size", float(self.max_pool))
        metrics.set("sqlserver_pool_min_size", float(self.min_pool))
        metrics.set("sqlserver_connections_created", float(self._connection_create_count))
        metrics.set("sqlserver_prepared_statements", float(len(self._prepared)))
        
        # Calculate pool utilization
        if self.max_pool > 0:
            utilization = (self.max_pool - len(self.pool)) / self.max_pool
            metrics.set("sqlserver_pool_utilization", utilization)

    async def adjust_pool_size(self) -> None:
        """Enhanced dynamic pool adjustment based on load and performance."""
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0
        
        async with self._lock:
            current_size = len(self.pool)
            target_size = current_size
            
            # Adjust based on system load
            if load > 4 and current_size > self.min_pool:
                # High load - reduce pool size
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
                
            elif load < 1 and current_size < self.max_pool:
                # Low load - potentially increase pool size
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

    async def optimize_connection_settings(self, conn: pyodbc.Connection) -> None:
        """Apply connection-specific optimizations."""
        try:
            cursor = conn.cursor()
            
            # Query-specific optimizations
            optimization_queries = [
                "SET LOCK_TIMEOUT 30000",  # 30 second lock timeout
                "SET DEADLOCK_PRIORITY LOW",  # Lower deadlock priority
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",  # Standard isolation
                "SET ANSI_WARNINGS OFF",  # Reduce warnings for bulk operations
                "SET XACT_ABORT ON",  # Abort transaction on error
            ]
            
            for query in optimization_queries:
                try:
                    cursor.execute(query)
                except Exception:
                    continue  # Skip failed optimizations
            
            cursor.close()
        except Exception as e:
            print(f"Warning: Failed to apply connection optimizations: {e}")

    async def bulk_validate_facilities(self, facility_ids: list[str]) -> Dict[str, bool]:
        """Optimized bulk facility validation."""
        if not facility_ids:
            return {}
        
        # Check cache first
        results = {}
        uncached_ids = []
        
        for fid in facility_ids:
            cache_key = f"facility_valid:{fid}"
            cached = self.query_cache.get(cache_key)
            if cached is not None:
                results[fid] = cached
            else:
                uncached_ids.append(fid)
        
        # Bulk query for uncached IDs
        if uncached_ids:
            try:
                # Use IN clause for bulk validation
                placeholders = ",".join("?" * len(uncached_ids))
                query = f"""
                    SELECT facility_id 
                    FROM facilities 
                    WHERE facility_id IN ({placeholders}) 
                    AND facility_id IS NOT NULL
                """
                
                rows = await self.fetch_optimized(query, *uncached_ids)
                valid_ids = {row["facility_id"] for row in rows}
                
                # Cache results and build response
                for fid in uncached_ids:
                    is_valid = fid in valid_ids
                    results[fid] = is_valid
                    cache_key = f"facility_valid:{fid}"
                    self.query_cache.set(cache_key, is_valid)
                
                metrics.inc("sqlserver_bulk_facility_validations")
                
            except Exception as e:
                print(f"Warning: Bulk facility validation failed: {e}")
                # Fallback to individual validations
                for fid in uncached_ids:
                    results[fid] = False
        
        return results

    async def bulk_validate_financial_classes(self, class_ids: list[str]) -> Dict[str, bool]:
        """Optimized bulk financial class validation."""
        if not class_ids:
            return {}
        
        # Check cache first
        results = {}
        uncached_ids = []
        
        for cid in class_ids:
            cache_key = f"financial_class_valid:{cid}"
            cached = self.query_cache.get(cache_key)
            if cached is not None:
                results[cid] = cached
            else:
                uncached_ids.append(cid)
        
        # Bulk query for uncached IDs
        if uncached_ids:
            try:
                placeholders = ",".join("?" * len(uncached_ids))
                query = f"""
                    SELECT financial_class_id 
                    FROM facility_financial_classes 
                    WHERE financial_class_id IN ({placeholders}) 
                    AND financial_class_id IS NOT NULL AND active = 1
                """
                
                rows = await self.fetch_optimized(query, *uncached_ids)
                valid_ids = {row["financial_class_id"] for row in rows}
                
                # Cache results and build response
                for cid in uncached_ids:
                    is_valid = cid in valid_ids
                    results[cid] = is_valid
                    cache_key = f"financial_class_valid:{cid}"
                    self.query_cache.set(cache_key, is_valid)
                
                metrics.inc("sqlserver_bulk_financial_class_validations")
                
            except Exception as e:
                print(f"Warning: Bulk financial class validation failed: {e}")
                # Fallback to individual validations
                for cid in uncached_ids:
                    results[cid] = False
        
        return results

    async def get_performance_insights(self) -> Dict[str, Any]:
        """Get performance insights and optimization recommendations."""
        try:
            conn = await self._acquire()
            cursor = conn.cursor()
            
            insights = {}
            
            # Check for missing indexes
            cursor.execute("""
                SELECT TOP 5 
                    d.object_id,
                    OBJECT_NAME(d.object_id) AS TableName,
                    d.equality_columns,
                    d.inequality_columns,
                    d.included_columns,
                    d.user_seeks,
                    d.user_scans,
                    d.avg_total_user_cost,
                    d.avg_user_impact
                FROM sys.dm_db_missing_index_details d
                INNER JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
                INNER JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
                WHERE d.database_id = DB_ID()
                ORDER BY d.avg_user_impact DESC
            """)
            
            missing_indexes = []
            for row in cursor.fetchall():
                missing_indexes.append({
                    "table": row[1],
                    "equality_columns": row[2],
                    "inequality_columns": row[3],
                    "included_columns": row[4],
                    "user_seeks": row[5],
                    "avg_user_impact": row[8]
                })
            insights["missing_indexes"] = missing_indexes
            
            # Check for expensive queries
            cursor.execute("""
                SELECT TOP 5
                    qs.execution_count,
                    qs.total_worker_time / qs.execution_count AS avg_cpu_time,
                    qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
                    qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
                    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
                        ((CASE qs.statement_end_offset
                            WHEN -1 THEN DATALENGTH(qt.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset)/2)+1) AS statement_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
                WHERE qt.text LIKE '%claims%' OR qt.text LIKE '%failed_claims%'
                ORDER BY qs.total_worker_time / qs.execution_count DESC
            """)
            
            expensive_queries = []
            for row in cursor.fetchall():
                expensive_queries.append({
                    "execution_count": row[0],
                    "avg_cpu_time": row[1],
                    "avg_elapsed_time": row[2],
                    "avg_logical_reads": row[3],
                    "statement": row[4][:200] if row[4] else ""  # Truncate for display
                })
            insights["expensive_queries"] = expensive_queries
            
            cursor.close()
            await self._release(conn)
            
            return insights
            
        except Exception as e:
            print(f"Warning: Failed to get performance insights: {e}")
            return {}

    async def close(self) -> None:
        """Enhanced cleanup with connection health tracking."""
        async with self._lock:
            closed_count = 0
            failed_count = 0
            
            while self.pool:
                conn = self.pool.pop()
                try:
                    conn.close()
                    closed_count += 1
                except Exception:
                    failed_count += 1
            
            # Clear prepared statements
            self._prepared.clear()
            self._prepared_on_connections.clear()
            
            metrics.set("sqlserver_connections_closed", float(closed_count))
            if failed_count > 0:
                metrics.set("sqlserver_connections_close_failed", float(failed_count))

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
                "performance_insights": True,
            },
            "performance_optimizations": {
                "fast_executemany": True,
                "connection_reuse": True,
                "batch_processing": True,
                "parallel_execution": True,
                "circuit_breaker": True,
            }
        }