try:
    import asyncpg
except Exception:  # pragma: no cover - allow missing dependency in tests
    asyncpg = None
import asyncio
import csv
import io
import os
import time
from typing import Any, Iterable, List, Dict, Optional

from ..analysis.query_tracker import record as record_query
from ..config.config import PostgresConfig
from ..monitoring.metrics import metrics
from ..monitoring.stats import latencies
from ..utils.cache import InMemoryCache
from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from ..utils.errors import (CircuitBreakerOpenError, DatabaseConnectionError,
                            QueryError)
from .base import BaseDatabase


class PostgresDatabase(BaseDatabase):
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg
        self.pool: asyncpg.pool.Pool | None = None
        self.replica_pool: asyncpg.pool.Pool | None = None
        self.circuit_breaker = CircuitBreaker()
        # Enhanced query result cache with TTL
        self.query_cache = InMemoryCache(ttl=60)
        # Track prepared statements cached across connections
        self._prepared: Dict[str, str] = {}  # statement_name -> query
        self._prepared_statements: Dict[str, Any] = {}  # statement_name -> prepared_statement
        self._lock = asyncio.Lock()
        # Connection pool health tracking
        self._pool_health_check_interval = 30.0
        self._last_health_check = 0.0

    async def _init_connection(self, conn: "asyncpg.Connection") -> None:
        """Initialize a new connection with prepared statements and optimizations."""
        # Set connection-level optimizations
        await conn.execute("SET synchronous_commit = OFF")  # For staging/non-critical data
        await conn.execute("SET wal_writer_delay = '50ms'")
        await conn.execute("SET checkpoint_completion_target = 0.9")
        
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

    async def connect(self) -> None:
        """Enhanced connection with pre-warming and health monitoring."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        try:
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
                    'application_name': 'claims_processor',
                    'tcp_keepalives_idle': '600',
                    'tcp_keepalives_interval': '30',
                    'tcp_keepalives_count': '3',
                }
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
                        'application_name': 'claims_processor_replica',
                        'default_transaction_isolation': 'read_committed',
                    }
                )

            # Aggressive connection pool pre-warming
            await self._warm_connection_pools()
            
            # Pre-prepare common queries
            await self._prepare_common_queries()
            
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise DatabaseConnectionError(str(e)) from e
        await self.circuit_breaker.record_success()

    async def _warm_connection_pools(self) -> None:
        """Pre-warm connection pools by creating and testing all connections."""
        async def warm_pool(pool: asyncpg.pool.Pool, pool_name: str) -> None:
            """Warm a specific pool by creating all connections."""
            connections = []
            try:
                # Acquire all possible connections to force creation
                for i in range(pool._maxsize):  # type: ignore[attr-defined]
                    try:
                        conn = await asyncio.wait_for(pool.acquire(), timeout=2.0)
                        connections.append(conn)
                        # Test connection with simple query
                        await conn.execute("SELECT 1")
                    except asyncio.TimeoutError:
                        break  # Pool likely at capacity
                    except Exception as e:
                        print(f"Warning: Failed to warm connection {i} in {pool_name}: {e}")
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
            
            "get_top_rvu_codes": """
                SELECT procedure_code FROM rvu_data 
                WHERE status = 'active'
                ORDER BY total_rvu DESC NULLS LAST 
                LIMIT $1
            """,
            
            # Checkpoint operations
            "insert_checkpoint": """
                INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) 
                VALUES ($1, $2, NOW())
                ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()
            """,
            
            # Dead letter queue operations
            "insert_dead_letter": """
                INSERT INTO dead_letter_queue (claim_id, reason, data, inserted_at) 
                VALUES ($1, $2, $3, NOW())
            """,
            
            "fetch_dead_letter_batch": """
                SELECT dlq_id, claim_id, data, reason
                FROM dead_letter_queue 
                ORDER BY inserted_at ASC 
                LIMIT $1
            """,
            
            "delete_dead_letter": """
                DELETE FROM dead_letter_queue WHERE dlq_id = ANY($1)
            """,
            
            # Metrics and monitoring
            "insert_processing_metrics": """
                INSERT INTO processing_metrics (
                    batch_id, stage_name, worker_id, records_processed, 
                    records_failed, processing_time_seconds, throughput_per_second,
                    memory_usage_mb, cpu_usage_percent, additional_metrics
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            
            # Business rules queries
            "fetch_active_rules": """
                SELECT rule_name, rule_type, rule_logic, severity_level,
                       applies_to_facilities, applies_to_financial_classes
                FROM business_rules 
                WHERE is_active = true
                ORDER BY severity_level DESC, rule_name
            """
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
            
            if self.replica_pool:
                try:
                    async with self.replica_pool.acquire() as conn:
                        await conn.prepare(query)
                except Exception as e:
                    print(f"Warning: Failed to prepare statement {statement_name} on replica: {e}")

    async def execute_prepared(self, statement_name: str, *params: Any, use_replica: bool = False) -> Any:
        """Execute a prepared statement by name."""
        if statement_name not in self._prepared:
            raise ValueError(f"Statement {statement_name} not prepared")
        
        pool = self.replica_pool if (use_replica and self.replica_pool) else self.pool
        if not pool:
            await self._ensure_pool()
            pool = self.pool
        
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        try:
            start = time.perf_counter()
            async with pool.acquire() as conn:
                # Try to use the prepared statement
                query = self._prepared[statement_name]
                result = await conn.execute(query, *params)
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            metrics.inc("postgres_prepared_queries")
            latencies.record("postgres_query", duration)
            record_query(f"PREPARED:{statement_name}", duration)
            await self.circuit_breaker.record_success()
            
            # Parse result for row count
            if isinstance(result, str) and " " in result:
                try:
                    return int(result.split(" ")[-1])
                except ValueError:
                    return 1
            return 1
            
        except Exception as e:
            await self.circuit_breaker.record_failure()
            # Try to reconnect and retry once
            try:
                await self.connect()
                async with pool.acquire() as conn:
                    query = self._prepared[statement_name]
                    result = await conn.execute(query, *params)
                await self.circuit_breaker.record_success()
                if isinstance(result, str) and " " in result:
                    try:
                        return int(result.split(" ")[-1])
                    except ValueError:
                        return 1
                return 1
            except Exception as retry_e:
                await self.circuit_breaker.record_failure()
                raise QueryError(f"Prepared statement execution failed: {retry_e}") from retry_e

    async def fetch_prepared(self, statement_name: str, *params: Any, use_replica: bool = True) -> Iterable[dict]:
        """Fetch results using a prepared statement."""
        if statement_name not in self._prepared:
            raise ValueError(f"Statement {statement_name} not prepared")
        
        # Use replica for read operations when available
        pool = self.replica_pool if (use_replica and self.replica_pool) else self.pool
        if not pool:
            await self._ensure_pool()
            pool = self.pool
        
        # Check cache first
        cache_key = f"PREP:{statement_name}:{str(params)}"
        cached = self.query_cache.get(cache_key)
        if cached is not None:
            metrics.inc("postgres_cache_hits")
            return cached
        
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        try:
            start = time.perf_counter()
            async with pool.acquire() as conn:
                query = self._prepared[statement_name]
                rows = await conn.fetch(query, *params)
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            metrics.inc("postgres_prepared_queries")
            latencies.record("postgres_query", duration)
            record_query(f"PREPARED:{statement_name}", duration)
            await self.circuit_breaker.record_success()
            
            result = [dict(row) for row in rows]
            self.query_cache.set(cache_key, result)
            metrics.inc("postgres_cache_misses")
            return result
            
        except Exception as e:
            await self.circuit_breaker.record_failure()
            # Try to reconnect and retry once
            try:
                await self.connect()
                async with pool.acquire() as conn:
                    query = self._prepared[statement_name]
                    rows = await conn.fetch(query, *params)
                result = [dict(row) for row in rows]
                self.query_cache.set(cache_key, result)
                await self.circuit_breaker.record_success()
                return result
            except Exception as retry_e:
                await self.circuit_breaker.record_failure()
                raise QueryError(f"Prepared fetch failed: {retry_e}") from retry_e

    async def get_rvu_bulk_optimized(self, procedure_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Optimized bulk RVU lookup using prepared statements and ANY() operator."""
        if not procedure_codes:
            return {}
        
        # Check cache first for all codes
        uncached_codes = []
        results = {}
        
        for code in procedure_codes:
            cache_key = f"rvu:{code}"
            cached = self.query_cache.get(cache_key)
            if cached:
                results[code] = cached
                metrics.inc("rvu_cache_hits")
            else:
                uncached_codes.append(code)
                metrics.inc("rvu_cache_misses")
        
        # Bulk fetch uncached codes using prepared statement
        if uncached_codes:
            try:
                rows = await self.fetch_prepared("get_rvu_bulk_any", uncached_codes, use_replica=True)
                
                for row in rows:
                    code = row.get("procedure_code")
                    if code:
                        rvu_data = dict(row)
                        results[code] = rvu_data
                        # Cache individual results
                        cache_key = f"rvu:{code}"
                        self.query_cache.set(cache_key, rvu_data)
                
                # Track bulk fetch performance
                metrics.inc("postgres_rvu_bulk_fetches")
                metrics.set("postgres_last_bulk_rvu_size", len(uncached_codes))
                
            except Exception as e:
                print(f"Warning: Bulk RVU fetch failed: {e}")
                # Fallback to individual lookups for uncached codes
                for code in uncached_codes:
                    try:
                        rows = await self.fetch_prepared("get_rvu_single", code, use_replica=True)
                        if rows:
                            rvu_data = dict(rows[0])
                            results[code] = rvu_data
                            cache_key = f"rvu:{code}"
                            self.query_cache.set(cache_key, rvu_data)
                    except Exception:
                        continue  # Skip failed individual lookups
        
        return results

    # Enhanced versions of existing methods with prepared statement support
    async def fetch(self, query: str, *params: Any, use_replica: bool = True) -> Iterable[dict]:
        """Enhanced fetch with automatic replica routing and caching."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        cache_key = f"query:{query}:{str(params)}"
        cached = self.query_cache.get(cache_key)
        if cached is not None:
            metrics.inc("postgres_cache_hits")
            return cached
        
        await self._ensure_pool()
        assert self.pool
        
        # Use replica for read operations when available
        pool = self.replica_pool if (use_replica and self.replica_pool) else self.pool
        assert pool
        
        try:
            start = time.perf_counter()
            async with pool.acquire() as conn:
                # Check if this is a query we should have prepared
                if self._should_prepare_query(query):
                    rows = await conn.fetch(query, *params)
                else:
                    rows = await conn.fetch(query, *params)
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            latencies.record("postgres_query", duration)
            record_query(query, duration)
            await self.circuit_breaker.record_success()
            
            result = [dict(row) for row in rows]
            self.query_cache.set(cache_key, result)
            metrics.inc("postgres_cache_misses")
            return result
            
        except asyncpg.PostgresError:
            await self.circuit_breaker.record_failure()
            await self.connect()
            pool = self.replica_pool if (use_replica and self.replica_pool) else self.pool
            assert pool
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            await self.circuit_breaker.record_success()
            result = [dict(row) for row in rows]
            self.query_cache.set(cache_key, result)
            return result
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e

    def _should_prepare_query(self, query: str) -> bool:
        """Determine if a query should be prepared based on patterns."""
        prep_patterns = [
            "SELECT * FROM rvu_data WHERE procedure_code",
            "INSERT INTO processing_checkpoints",
            "INSERT INTO dead_letter_queue",
            "DELETE FROM dead_letter_queue",
            "SELECT * FROM claims",
            "UPDATE claims SET",
            "INSERT INTO processing_metrics"
        ]
        
        query_clean = " ".join(query.split()).upper()
        return any(pattern.upper() in query_clean for pattern in prep_patterns)

    async def execute(self, query: str, *params: Any) -> int:
        """Enhanced execute with prepared statement optimization."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        await self._ensure_pool()
        assert self.pool
        
        try:
            start = time.perf_counter()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("postgres_query_ms", duration)
            metrics.inc("postgres_query_count")
            latencies.record("postgres_query", duration)
            await self.circuit_breaker.record_success()
            
            return int(result.split(" ")[-1])
        except asyncpg.PostgresError:
            await self.circuit_breaker.record_failure()
            await self.connect()
            async with self.pool.acquire() as conn:
                result = await conn.execute(query, *params)
            await self.circuit_breaker.record_success()
            return int(result.split(" ")[-1])
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e

    async def execute_many_optimized(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 4,
        batch_size: int = 1000,
    ) -> int:
        """Optimized execute_many with batching and prepared statements."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("Postgres circuit open")
        
        await self._ensure_pool()
        assert self.pool
        
        params_list = list(params_seq)
        if not params_list:
            return 0
        
        total_processed = 0
        
        # Process in batches for better memory management
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
                    async with self.pool.acquire() as conn:
                        await conn.executemany(query, chunk)
                    return len(chunk)

                results = await asyncio.gather(*[run_chunk(c) for c in chunks])
                total_processed += sum(results)
            else:
                # Sequential processing for smaller batches
                try:
                    start = time.perf_counter()
                    async with self.pool.acquire() as conn:
                        await conn.executemany(query, batch)
                    
                    duration = (time.perf_counter() - start) * 1000
                    metrics.inc("postgres_query_ms", duration)
                    metrics.inc("postgres_query_count")
                    latencies.record("postgres_query", duration)
                    await self.circuit_breaker.record_success()
                    total_processed += len(batch)
                    
                except asyncpg.PostgresError:
                    await self.circuit_breaker.record_failure()
                    await self.connect()
                    async with self.pool.acquire() as conn:
                        await conn.executemany(query, batch)
                    await self.circuit_breaker.record_success()
                    total_processed += len(batch)
                except Exception as e:
                    await self.circuit_breaker.record_failure()
                    raise QueryError(str(e)) from e
        
        return total_processed

    # Override existing execute_many to use optimized version
    async def execute_many(
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
    ) -> int:
        return await self.execute_many_optimized(
            query, params_seq, concurrency=concurrency, batch_size=1000
        )

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
        """Perform health checks on connection pools."""
        async def check_pool(pool: asyncpg.pool.Pool, pool_name: str) -> bool:
            try:
                async with asyncio.timeout(5.0):
                    async with pool.acquire() as conn:
                        await conn.execute("SELECT 1")
                metrics.set(f"postgres_pool_{pool_name}_healthy", 1.0)
                return True
            except Exception as e:
                print(f"Warning: Pool {pool_name} health check failed: {e}")
                metrics.set(f"postgres_pool_{pool_name}_healthy", 0.0)
                return False
        
        if self.pool:
            await check_pool(self.pool, "main")
        
        if self.replica_pool:
            await check_pool(self.replica_pool, "replica")

    async def health_check(self) -> bool:
        """Enhanced health check with detailed reporting."""
        if not await self.circuit_breaker.allow():
            return False
        try:
            # Check main pool
            main_healthy = False
            if self.pool:
                try:
                    async with asyncio.timeout(3.0):
                        async with self.pool.acquire() as conn:
                            await conn.execute("SELECT 1")
                    main_healthy = True
                except Exception:
                    pass
            
            # Check replica pool
            replica_healthy = True  # Default to healthy if no replica
            if self.replica_pool:
                try:
                    async with asyncio.timeout(3.0):
                        async with self.replica_pool.acquire() as conn:
                            await conn.execute("SELECT 1")
                except Exception:
                    replica_healthy = False
            
            overall_healthy = main_healthy and replica_healthy
            await self.circuit_breaker.record_success() if overall_healthy else await self.circuit_breaker.record_failure()
            
            # Update metrics
            metrics.set("postgres_main_pool_healthy", 1.0 if main_healthy else 0.0)
            metrics.set("postgres_replica_pool_healthy", 1.0 if replica_healthy else 0.0)
            
            return overall_healthy
            
        except Exception:
            await self.circuit_breaker.record_failure()
            return False

    def report_pool_status(self) -> None:
        """Enhanced pool status reporting."""
        if not self.pool:
            return
        try:
            # Main pool stats
            in_use = self.pool._working_conn_count  # type: ignore[attr-defined]
            max_size = self.pool._maxsize  # type: ignore[attr-defined]
            min_size = self.pool._minsize  # type: ignore[attr-defined]
            
            metrics.set("postgres_pool_in_use", float(in_use))
            metrics.set("postgres_pool_max", float(max_size))
            metrics.set("postgres_pool_min", float(min_size))
            metrics.set("postgres_pool_utilization", float(in_use) / float(max_size) if max_size > 0 else 0.0)
            
            # Replica pool stats
            if self.replica_pool:
                replica_in_use = self.replica_pool._working_conn_count  # type: ignore[attr-defined]
                replica_max_size = self.replica_pool._maxsize  # type: ignore[attr-defined]
                
                metrics.set("postgres_replica_pool_in_use", float(replica_in_use))
                metrics.set("postgres_replica_pool_max", float(replica_max_size))
                metrics.set("postgres_replica_pool_utilization", 
                          float(replica_in_use) / float(replica_max_size) if replica_max_size > 0 else 0.0)
            
            # Cache stats
            cache_info = {
                "prepared_statements": len(self._prepared),
                "cache_size": len(self.query_cache.store) if hasattr(self.query_cache, 'store') else 0,
            }
            metrics.set("postgres_prepared_statements_count", float(cache_info["prepared_statements"]))
        except Exception as e:
            print(f"Warning: Failed to report pool status: {e}")

    async def adjust_pool_size(self) -> None:
        """Enhanced pool size adjustment based on load and performance."""
        if not self.pool:
            return
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0
        
        try:
            current_max = self.pool._maxsize  # type: ignore[attr-defined]
            current_in_use = self.pool._working_conn_count  # type: ignore[attr-defined]
            utilization = current_in_use / current_max if current_max > 0 else 0
            
            # Adjust based on load and utilization
            if load > 4 and current_max > self.cfg.min_pool_size:
                new_size = max(self.cfg.min_pool_size, current_max - 2)
                self.pool._maxsize = new_size  # type: ignore[attr-defined]
                metrics.inc("postgres_pool_size_decreased")