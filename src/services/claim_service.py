import asyncio
import gc
import json
import time
import weakref
from typing import Any, Dict, Iterable, List, Set, Tuple

import psutil

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..monitoring.metrics import metrics
from ..security.compliance import (decrypt_text, encrypt_claim_fields,
                                   encrypt_text)
from ..utils.audit import record_audit_event
from ..utils.memory_pool import memory_pool
from ..utils.priority_queue import PriorityClaimQueue
from ..utils.errors import CircuitBreakerOpenError

# Circuit Breaker Recovery Helper Function for ClaimService
async def handle_circuit_breaker_error(
    postgres_db: PostgresDatabase, 
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
    if hasattr(postgres_db, 'reset_circuit_breaker'):
        await postgres_db.reset_circuit_breaker()
    else:
        await postgres_db.circuit_breaker.record_success()
    
    return await postgres_db.circuit_breaker.allow()


class ClaimService:
    """Enhanced service layer with memory management optimizations and circuit breaker handling."""

    def __init__(
        self,
        pg: PostgresDatabase,
        sql: SQLServerDatabase,
        encryption_key: str | None = None,
        checkpoint_buffer_size: int = 2000,
    ) -> None:
        self.pg = pg
        self.sql = sql
        self.encryption_key = encryption_key or ""
        self.priority_queue = PriorityClaimQueue()
        self.retry_queue = PriorityClaimQueue()

        # Enhanced buffering for batch operations
        self._checkpoint_buffer: list[tuple[str, str]] = []
        self._checkpoint_buffer_size = checkpoint_buffer_size
        self._audit_buffer: list[tuple] = []
        self._audit_buffer_size = 500

        # Validation caches with TTL
        self._facilities_cache: Set[str] = set()
        self._classes_cache: Set[str] = set()
        self._validation_cache_time: float = 0.0
        self._validation_cache_ttl: float = 300.0  # 5 minutes

        # Performance tracking
        self._bulk_operations_count = 0
        self._cache_hit_count = 0
        self._cache_miss_count = 0

        # Memory management additions
        self._memory_pool = memory_pool
        self._max_buffer_memory = 100 * 1024 * 1024  # 100MB max for buffers
        self._buffer_cleanup_threshold = 5000
        self._object_refs: Set[weakref.ref] = set()

        # Adaptive buffer sizes based on memory
        self._adaptive_checkpoint_buffer_size = checkpoint_buffer_size
        self._adaptive_audit_buffer_size = 500

        # Memory monitoring
        self._last_memory_check = time.time()
        self._memory_check_interval = 30  # Check every 30 seconds
        self._total_memory_limit = 1 * 1024 * 1024 * 1024  # 1GB limit for service

        # Object lifecycle tracking
        self._active_containers: List[Any] = []
        self._container_cleanup_threshold = 1000

        # Circuit breaker tracking
        self._circuit_breaker_recovery_attempts = 0
        self._max_recovery_attempts = 3
        self._last_circuit_breaker_reset = 0.0
        self._circuit_breaker_reset_interval = 60.0  # Don't reset more than once per minute

        # Simple logger for circuit breaker operations
        self.logger = self._create_simple_logger()

    def _create_simple_logger(self):
        """Create a simple logger for circuit breaker operations."""
        import logging
        logger = logging.getLogger(f"claims_processor.service.{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    async def _handle_postgres_circuit_breaker_error(self, operation_name: str) -> bool:
        """
        Handle PostgreSQL circuit breaker errors with intelligent recovery.
        
        Returns:
            bool: True if recovery successful, False otherwise
        """
        self.logger.warning(f"Circuit breaker open during {operation_name}")
        
        # Increment recovery attempts
        self._circuit_breaker_recovery_attempts += 1
        
        # Check if we should attempt recovery
        if self._circuit_breaker_recovery_attempts > self._max_recovery_attempts:
            current_time = time.time()
            if current_time - self._last_circuit_breaker_reset < self._circuit_breaker_reset_interval:
                self.logger.error(f"Too many recovery attempts for {operation_name}, giving up")
                return False
        
        # Try to recover from circuit breaker error
        recovery_success = await handle_circuit_breaker_error(
            self.pg, self.logger, max_wait_time=15.0
        )
        
        if recovery_success:
            self.logger.info(f"Circuit breaker recovered for {operation_name}")
            self._circuit_breaker_recovery_attempts = 0  # Reset counter on success
            return True
        else:
            self.logger.error(f"Circuit breaker recovery failed for {operation_name}")
            self._last_circuit_breaker_reset = time.time()
            return False

    async def _execute_with_circuit_breaker_handling(self, operation_func, operation_name: str, *args, **kwargs):
        """
        Execute a database operation with circuit breaker error handling.
        
        Args:
            operation_func: The async function to execute
            operation_name: Name of the operation for logging
            *args, **kwargs: Arguments to pass to the operation function
            
        Returns:
            The result of the operation function
            
        Raises:
            The original exception if circuit breaker recovery fails
        """
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                return await operation_func(*args, **kwargs)
                
            except CircuitBreakerOpenError:
                self.logger.warning(f"Circuit breaker open on attempt {attempt + 1} for {operation_name}")
                
                # Try recovery
                if await self._handle_postgres_circuit_breaker_error(operation_name):
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(0.5)  # Brief pause before retry
                        continue
                
                # If this is the last attempt or recovery failed, raise the error
                if attempt == max_attempts - 1:
                    self.logger.error(f"All attempts failed for {operation_name}")
                    raise
                
                # Wait before next attempt
                await asyncio.sleep(2.0 * (attempt + 1))
                
            except Exception as e:
                # For non-circuit breaker errors, log and potentially retry once
                if attempt == 0 and "connection" in str(e).lower():
                    self.logger.warning(f"Connection error in {operation_name}, retrying: {e}")
                    await asyncio.sleep(1.0)
                    continue
                else:
                    raise

    async def fetch_claims_optimized(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Ultra-optimized claims fetching with prepared statements, caching, and circuit breaker handling."""
        # Memory check before large operations
        await self._manage_buffer_memory()

        async def _fetch_operation():
            # Use prepared statements for better performance
            if priority:
                stmt_name = "fetch_claims_priority"
                try:
                    rows = await self.pg.fetch_prepared(
                        stmt_name,
                        batch_size,
                        offset,
                        5,
                        use_replica=True,  # Priority threshold
                    )
                except Exception:
                    # Fallback to regular query
                    try:
                        rows = await self.pg.fetch(
                            """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code,
                                 li.units AS li_units, li.charge_amount AS li_charge_amount,
                                 li.service_from_date AS li_service_from_date,
                                 li.service_to_date AS li_service_to_date
                           FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id
                           WHERE c.priority > $3
                           ORDER BY c.priority DESC LIMIT $1 OFFSET $2""",
                            batch_size,
                            offset,
                            5,
                            use_replica=True,
                        )
                    except TypeError:
                        rows = await self.pg.fetch(
                            """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code,
                                 li.units AS li_units, li.charge_amount AS li_charge_amount,
                                 li.service_from_date AS li_service_from_date,
                                 li.service_to_date AS li_service_to_date
                           FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id
                           WHERE c.priority > $3
                           ORDER BY c.priority DESC LIMIT $1 OFFSET $2""",
                            batch_size,
                            offset,
                            5,
                        )
            else:
                stmt_name = "fetch_claims_batch"
                try:
                    rows = await self.pg.fetch_prepared(
                        stmt_name, batch_size, offset, use_replica=True
                    )
                except Exception:
                    # Fallback to regular query
                    try:
                        rows = await self.pg.fetch(
                            """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code,
                                 li.units AS li_units, li.charge_amount AS li_charge_amount,
                                 li.service_from_date AS li_service_from_date,
                                 li.service_to_date AS li_service_to_date
                           FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id
                           ORDER BY c.priority DESC LIMIT $1 OFFSET $2""",
                            batch_size,
                            offset,
                            use_replica=True,
                        )
                    except TypeError:
                        rows = await self.pg.fetch(
                            """SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code,
                                 li.units AS li_units, li.charge_amount AS li_charge_amount,
                                 li.service_from_date AS li_service_from_date,
                                 li.service_to_date AS li_service_to_date
                           FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id
                           ORDER BY c.priority DESC LIMIT $1 OFFSET $2""",
                            batch_size,
                            offset,
                        )
            return rows

        # Execute with circuit breaker handling
        rows = await self._execute_with_circuit_breaker_handling(
            _fetch_operation, "fetch_claims_optimized"
        )

        # Optimize claim processing with bulk line item fetching using memory pooling
        return await self._process_claims_with_bulk_line_items(rows)

    async def load_validation_sets_optimized(self) -> tuple[set[str], set[str]]:
        """Optimized validation set loading with advanced caching, bulk operations, and circuit breaker handling."""
        current_time = time.time()

        # Check cache validity
        if (
            hasattr(self, "_validation_cache_time")
            and current_time - self._validation_cache_time < self._validation_cache_ttl
            and self._facilities_cache
            and self._classes_cache
        ):
            self._cache_hit_count += 1
            metrics.inc("validation_cache_hits")
            return self._facilities_cache, self._classes_cache

        self._cache_miss_count += 1
        metrics.inc("validation_cache_misses")

        async def _load_validation_operation():
            # Use prepared statements for optimal performance
            try:
                # Parallel execution of validation queries with correct table names
                facilities_task = self.sql.fetch_prepared("get_facilities_batch")
                classes_task = self.sql.fetch_prepared("get_financial_classes_batch")

                fac_rows, class_rows = await asyncio.gather(facilities_task, classes_task)

            except Exception as e:
                self.logger.warning(f"Prepared statements failed for validation sets, using fallback queries: {e}")
                # Fallback to regular queries if prepared statements fail
                # Updated to use your actual schema table names
                fac_task = self.sql.fetch(
                    "SELECT DISTINCT facility_id FROM facilities WHERE facility_id IS NOT NULL AND active = 1"
                )
                class_task = self.sql.fetch(
                    "SELECT DISTINCT financial_class_id FROM facility_financial_classes WHERE financial_class_id IS NOT NULL AND active = 1"
                )
                fac_rows, class_rows = await asyncio.gather(fac_task, class_task)

            return fac_rows, class_rows

        # Execute with circuit breaker handling
        fac_rows, class_rows = await self._execute_with_circuit_breaker_handling(
            _load_validation_operation, "load_validation_sets"
        )

        # Optimize set creation using memory pooling for temporary containers
        facilities_container = self._memory_pool.acquire("temp_sets", set)
        classes_container = self._memory_pool.acquire("temp_sets", set)

        try:
            facilities_container.clear()
            classes_container.clear()

            # Build sets using containers
            for r in fac_rows:
                if r.get("facility_id") is not None:
                    facilities_container.add(str(r.get("facility_id")))

            for r in class_rows:
                if r.get("financial_class_id") is not None:
                    classes_container.add(str(r.get("financial_class_id")))

            # Copy to final sets
            facilities = set(facilities_container)
            classes = set(classes_container)

        finally:
            # Return containers to pool
            self._memory_pool.release("temp_sets", facilities_container)
            self._memory_pool.release("temp_sets", classes_container)

        # Update cache
        self._facilities_cache = facilities
        self._classes_cache = classes
        self._validation_cache_time = current_time

        # Update cache metrics
        metrics.set("validation_facilities_count", float(len(facilities)))
        metrics.set("validation_classes_count", float(len(classes)))
        metrics.set("validation_cache_age", 0.0)

        return facilities, classes

    async def get_rvu_data_bulk(
        self, procedure_codes: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Optimized bulk RVU data retrieval with caching, memory management, and circuit breaker handling."""
        if not procedure_codes:
            return {}

        # Memory check for large RVU requests
        if len(procedure_codes) > 1000:
            await self._manage_buffer_memory()

        async def _get_rvu_operation():
            # Use optimized bulk RVU lookup from PostgreSQL
            try:
                if hasattr(self.pg, "get_rvu_bulk_optimized"):
                    return await self.pg.get_rvu_bulk_optimized(procedure_codes)
                else:
                    # Fallback implementation with memory pooling
                    return await self._fetch_rvu_data_with_pooling(procedure_codes)
            except Exception as e:
                self.logger.warning(f"Bulk RVU lookup failed: {e}")
                
                # Fallback to individual lookups with memory management
                results = {}
                batch_size = 100  # Process in smaller batches

                for i in range(0, len(procedure_codes), batch_size):
                    batch = procedure_codes[i : i + batch_size]

                    for code in batch:
                        try:
                            rows = await self.pg.fetch_prepared(
                                "get_rvu_single", code, use_replica=True
                            )
                            if rows:
                                results[code] = dict(rows[0])
                        except Exception:
                            continue

                    # Memory check between batches
                    if i > 0 and i % (batch_size * 10) == 0:
                        await self._manage_buffer_memory()

                return results

        # Execute with circuit breaker handling
        rvu_data = await self._execute_with_circuit_breaker_handling(
            _get_rvu_operation, "get_rvu_data_bulk"
        )

        metrics.inc("bulk_rvu_lookups")
        metrics.set("last_rvu_bulk_size", float(len(procedure_codes)))
        return rvu_data

    async def record_checkpoints_bulk(self, checkpoints: List[Tuple[str, str]]) -> None:
        """Optimized bulk checkpoint recording with memory management and circuit breaker handling."""
        if not checkpoints:
            return

        async def _record_checkpoints_operation():
            # Use memory pooling for checkpoint processing
            checkpoint_container = self._memory_pool.acquire("checkpoint_data", list)

            try:
                checkpoint_container.clear()
                checkpoint_container.extend(
                    [(claim_id, stage) for claim_id, stage in checkpoints]
                )

                # Use prepared statement for optimal performance
                await self.pg.execute_many_optimized(
                    "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
                    checkpoint_container,
                    concurrency=4,
                    batch_size=1000,
                )
                metrics.inc("bulk_checkpoints_recorded", len(checkpoints))

            except Exception as e:
                self.logger.warning(f"Bulk checkpoint recording failed: {e}")
                # Fallback to individual inserts
                for claim_id, stage in checkpoints:
                    try:
                        await self.record_checkpoint(claim_id, stage, buffered=False)
                    except Exception:
                        continue
            finally:
                self._memory_pool.release("checkpoint_data", checkpoint_container)

        # Execute with circuit breaker handling
        await self._execute_with_circuit_breaker_handling(
            _record_checkpoints_operation, "record_checkpoints_bulk"
        )

    async def record_checkpoint(
        self, claim_id: str, stage: str, *, buffered: bool = True
    ) -> None:
        """Enhanced checkpoint recording with intelligent buffering, memory management, and circuit breaker handling."""
        if buffered:
            self._checkpoint_buffer.append((claim_id, stage))
            if len(self._checkpoint_buffer) >= self._adaptive_checkpoint_buffer_size:
                await self.flush_checkpoints()
            return

        async def _record_checkpoint_operation():
            # Immediate recording using prepared statement
            try:
                await self.pg.execute_prepared("insert_checkpoint", claim_id, stage)
            except Exception:
                # Fallback to regular execute
                await self.pg.execute(
                    "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW())",
                    claim_id,
                    stage,
                )

        # Execute with circuit breaker handling
        await self._execute_with_circuit_breaker_handling(
            _record_checkpoint_operation, "record_checkpoint"
        )

    async def flush_checkpoints(self) -> None:
        """Enhanced checkpoint flushing with error recovery, memory management, and circuit breaker handling."""
        if not self._checkpoint_buffer:
            return

        checkpoints_to_flush = self._checkpoint_buffer.copy()
        self._checkpoint_buffer.clear()

        try:
            await self.record_checkpoints_bulk(checkpoints_to_flush)
        except Exception as e:
            self.logger.warning(f"Bulk checkpoint flush failed: {e}")
            # Re-add failed checkpoints to buffer for retry
            self._checkpoint_buffer.extend(checkpoints_to_flush)

    async def enqueue_dead_letter_optimized(
        self, claim: Dict[str, Any], reason: str
    ) -> None:
        """Optimized dead letter queue insertion with memory management and circuit breaker handling."""
        async def _enqueue_dead_letter_operation():
            # Use memory pooling for data preparation
            data_container = self._memory_pool.acquire("dead_letter_data", dict)

            try:
                data_container.clear()
                data_container.update(claim)

                data = (
                    encrypt_text(str(data_container), self.encryption_key)
                    if self.encryption_key
                    else str(data_container)
                )

                try:
                    await self.pg.execute_prepared(
                        "insert_dead_letter", claim.get("claim_id"), reason, data
                    )
                except Exception:
                    # Fallback to regular insert
                    await self.pg.execute(
                        "INSERT INTO dead_letter_queue (claim_id, reason, data, inserted_at) VALUES ($1, $2, $3, NOW())",
                        claim.get("claim_id"),
                        reason,
                        data,
                    )

            finally:
                self._memory_pool.release("dead_letter_data", data_container)

        # Execute with circuit breaker handling
        await self._execute_with_circuit_breaker_handling(
            _enqueue_dead_letter_operation, "enqueue_dead_letter"
        )

    async def reprocess_dead_letter_optimized(self, batch_size: int = 1000) -> int:
        """Ultra-optimized dead letter queue reprocessing with memory management and circuit breaker handling."""
        # Memory check for large operations
        await self._manage_buffer_memory()

        async def _reprocess_dead_letter_operation():
            try:
                # Use prepared statement for fetching
                rows = await self.pg.fetch_prepared("fetch_dead_letter_batch", batch_size)
            except Exception:
                # Fallback to regular query
                rows = await self.pg.fetch(
                    "SELECT dlq_id, claim_id, data, reason FROM dead_letter_queue ORDER BY inserted_at ASC LIMIT $1",
                    batch_size,
                )

            if not rows:
                return 0

            # Process claims in parallel batches using memory pooling
            successfully_processed = []
            failed_to_process = []
            dlq_ids_to_delete = []

            # Use memory pooling for processing containers
            processing_containers = []

            try:
                for row in rows:
                    dlq_id = row.get("dlq_id")
                    claim_str = row.get("data", "")
                    claim_id = row.get("claim_id")

                    # Acquire container for this claim
                    claim_container = self._memory_pool.acquire("dead_letter_claims", dict)
                    processing_containers.append(claim_container)

                    try:
                        claim_container.clear()

                        # Decrypt if needed
                        if self.encryption_key:
                            claim_str = decrypt_text(claim_str, self.encryption_key)

                        # Parse claim
                        parsed_claim = json.loads(claim_str)
                        claim_container.update(parsed_claim)

                        # Attempt automatic repair
                        from ..processing.repair import ClaimRepairSuggester

                        suggester = ClaimRepairSuggester()
                        repaired_claim = suggester.auto_repair(dict(claim_container), [])

                        successfully_processed.append(repaired_claim)
                        dlq_ids_to_delete.append(dlq_id)

                    except Exception:
                        failed_to_process.append(claim_id)
                        dlq_ids_to_delete.append(dlq_id)  # Remove even if processing failed

                # Bulk delete processed items
                if dlq_ids_to_delete:
                    try:
                        await self.pg.execute_prepared(
                            "delete_dead_letter", dlq_ids_to_delete
                        )
                    except Exception:
                        # Fallback deletion
                        placeholders = ",".join(
                            "$" + str(i + 1) for i in range(len(dlq_ids_to_delete))
                        )
                        await self.pg.execute(
                            f"DELETE FROM dead_letter_queue WHERE dlq_id IN ({placeholders})",
                            *dlq_ids_to_delete,
                        )

                # Bulk insert successfully processed claims
                if successfully_processed:
                    try:
                        await self.insert_claims_ultra_optimized(successfully_processed)
                        metrics.inc("failed_claims_resolved", len(successfully_processed))
                    except Exception as e:
                        self.logger.warning(f"Failed to insert reprocessed claims: {e}")
                        # Re-add to retry queue
                        for claim in successfully_processed:
                            pr = int(claim.get("priority", 0) or 0)
                            self.retry_queue.push(claim, pr)

            finally:
                # Return all containers to pool
                for container in processing_containers:
                    self._memory_pool.release("dead_letter_claims", container)

            # Update metrics
            metrics.inc("dead_letter_processed", len(rows))
            metrics.inc("dead_letter_resolved", len(successfully_processed))
            metrics.inc("dead_letter_failed", len(failed_to_process))

            return len(successfully_processed)

        # Execute with circuit breaker handling
        return await self._execute_with_circuit_breaker_handling(
            _reprocess_dead_letter_operation, "reprocess_dead_letter"
        )

    async def top_rvu_codes_optimized(self, limit: int = 1000) -> list[str]:
        """Optimized top RVU codes retrieval with caching, memory management, and circuit breaker handling."""
        async def _top_rvu_codes_operation():
            # Use memory pooling for results
            results_container = self._memory_pool.acquire("rvu_codes_list", list)

            try:
                results_container.clear()

                try:
                    rows = await self.pg.fetch_prepared(
                        "get_top_rvu_codes", limit, use_replica=True
                    )
                except Exception:
                    # Fallback query
                    rows = await self.pg.fetch(
                        "SELECT procedure_code FROM rvu_data WHERE status = 'active' ORDER BY total_rvu DESC NULLS LAST LIMIT $1",
                        limit,
                    )

                for r in rows:
                    if r.get("procedure_code"):
                        results_container.append(str(r.get("procedure_code")))

                codes = list(results_container)
                metrics.set("top_rvu_codes_fetched", float(len(codes)))
                return codes

            finally:
                self._memory_pool.release("rvu_codes_list", results_container)

        # Execute with circuit breaker handling
        return await self._execute_with_circuit_breaker_handling(
            _top_rvu_codes_operation, "top_rvu_codes"
        )

    async def get_database_health_status(self) -> Dict[str, Any]:
        """Get comprehensive database health status including circuit breaker info."""
        try:
            pg_health = await self.pg.health_check()
            sql_health = await self.sql.health_check() if hasattr(self.sql, 'health_check') else True
            
            pg_cb_status = {}
            if hasattr(self.pg, 'get_circuit_breaker_status'):
                pg_cb_status = await self.pg.get_circuit_breaker_status()
            
            return {
                "postgresql": {
                    "healthy": pg_health,
                    "circuit_breaker": pg_cb_status
                },
                "sql_server": {
                    "healthy": sql_health
                },
                "overall_healthy": pg_health and sql_health and not pg_cb_status.get("is_open", False),
                "circuit_breaker_recovery_attempts": self._circuit_breaker_recovery_attempts,
                "last_circuit_breaker_reset": self._last_circuit_breaker_reset
            }
        except Exception as e:
            self.logger.error(f"Error getting database health status: {e}")
            return {
                "postgresql": {"healthy": False, "error": str(e)},
                "sql_server": {"healthy": False},
                "overall_healthy": False,
                "error": str(e)
            }

    async def reset_circuit_breakers(self) -> Dict[str, bool]:
        """Manually reset circuit breakers - use for emergency recovery."""
        results = {}
        
        try:
            if hasattr(self.pg, 'reset_circuit_breaker'):
                await self.pg.reset_circuit_breaker()
            else:
                await self.pg.circuit_breaker.record_success()
            results["postgresql"] = True
            self.logger.info("PostgreSQL circuit breaker reset successfully")
            self._circuit_breaker_recovery_attempts = 0
            self._last_circuit_breaker_reset = time.time()
        except Exception as e:
            results["postgresql"] = False
            self.logger.error(f"Failed to reset PostgreSQL circuit breaker: {e}")
        
        # Reset SQL Server circuit breaker if it exists
        try:
            if hasattr(self.sql, 'reset_circuit_breaker'):
                await self.sql.reset_circuit_breaker()
                results["sql_server"] = True
                self.logger.info("SQL Server circuit breaker reset successfully")
            else:
                results["sql_server"] = False
                self.logger.info("SQL Server circuit breaker not available")
        except Exception as e:
            results["sql_server"] = False
            self.logger.error(f"Failed to reset SQL Server circuit breaker: {e}")
        
        return results

    # New method for main entry point with circuit breaker handling
    async def fetch_claims(
        self, batch_size: int, offset: int = 0, priority: str = None
    ) -> List[Dict[str, Any]]:
        """Main entry point for fetching claims with comprehensive circuit breaker error handling."""
        try:
            # Check circuit breaker status before attempting fetch
            if hasattr(self.pg, 'get_circuit_breaker_status'):
                cb_status = await self.pg.get_circuit_breaker_status()
                if cb_status.get("is_open", False):
                    self.logger.warning(
                        f"Circuit breaker is open before fetch attempt. Status: {cb_status}"
                    )
                    
                    # Try immediate recovery
                    if await self._handle_postgres_circuit_breaker_error("fetch_claims_preflight"):
                        self.logger.info("Circuit breaker recovered before fetch")
                    else:
                        self.logger.warning("Circuit breaker recovery failed, proceeding with caution")
            
            # Determine priority boolean from string
            priority_bool = priority == "high" if isinstance(priority, str) else bool(priority)
            
            # Attempt optimized fetch
            return await self.fetch_claims_optimized(batch_size, offset, priority_bool)
            
        except CircuitBreakerOpenError:
            self.logger.error("Circuit breaker error not handled properly in fetch pipeline")
            # Try one more recovery attempt
            if await self._handle_postgres_circuit_breaker_error("fetch_claims_final_attempt"):
                return await self.fetch_claims_optimized(batch_size, offset, priority_bool)
            else:
                raise
            
        except Exception as e:
            self.logger.error(f"Unexpected error in fetch_claims: {e}")
            raise

    # Keep all the existing memory management and other methods unchanged
    async def _manage_buffer_memory(self) -> None:
        """Manage buffer memory usage."""
        current_time = time.time()

        # Periodic memory check
        if current_time - self._last_memory_check < self._memory_check_interval:
            return

        self._last_memory_check = current_time

        # Check checkpoint buffer size
        checkpoint_memory = (
            len(self._checkpoint_buffer) * 100
        )  # Estimate 100 bytes per checkpoint
        audit_memory = (
            len(self._audit_buffer) * 500
        )  # Estimate 500 bytes per audit entry

        total_buffer_memory = checkpoint_memory + audit_memory

        if total_buffer_memory > self._max_buffer_memory:
            # Force flush buffers
            await self.flush_checkpoints()
            await self._flush_audit_buffer()

            # Reduce buffer sizes temporarily
            self._adaptive_checkpoint_buffer_size = max(
                500, self._adaptive_checkpoint_buffer_size // 2
            )
            self._adaptive_audit_buffer_size = max(
                100, self._adaptive_audit_buffer_size // 2
            )

            metrics.inc("buffer_memory_reductions")

        # Check total service memory usage
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            if (
                memory_mb > self._total_memory_limit / (1024 * 1024) * 0.8
            ):  # 80% threshold
                await self._emergency_memory_cleanup()

        except Exception:
            pass  # Ignore psutil errors

    async def _emergency_memory_cleanup(self) -> None:
        """Emergency memory cleanup for the service."""
        # Flush all buffers
        await self.flush_checkpoints()
        await self._flush_audit_buffer()

        # Clear large caches
        if len(self._facilities_cache) > 1000:
            recent_facilities = list(self._facilities_cache)[-500:]
            self._facilities_cache = set(recent_facilities)

        if len(self._classes_cache) > 1000:
            recent_classes = list(self._classes_cache)[-500:]
            self._classes_cache = set(recent_classes)

        # Clean up object pools
        cleanup_stats = self._memory_pool.cleanup_all()

        # Clear active containers
        self._active_containers.clear()

        # Clear object references
        self._object_refs.clear()

        # Force garbage collection
        collected = gc.collect()

        metrics.inc("service_emergency_cleanups")
        metrics.set("service_emergency_gc_collected", collected)

        self.logger.info(
            f"Service emergency cleanup: pools {cleanup_stats}, GC collected {collected}"
        )

    async def _flush_audit_buffer(self) -> None:
        """Flush audit buffer to prevent memory buildup."""
        if not self._audit_buffer:
            return

        audit_entries = self._audit_buffer.copy()
        self._audit_buffer.clear()

        # Process audit entries asynchronously to avoid blocking
        asyncio.create_task(self._process_audit_entries(audit_entries))

    async def _process_audit_entries(self, entries: List) -> None:
        """Process audit entries asynchronously."""
        if not entries:
            return

        container = self._memory_pool.acquire("audit_entries", list)

        try:
            container.clear()
            container.extend(entries)

            try:
                await self.sql.execute_many_optimized(
                    """INSERT INTO audit_log (
                            table_name, record_id, operation, user_id,
                            old_values, new_values, operation_timestamp, reason
                        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?)""",
                    container,
                    concurrency=4,
                    batch_size=500,
                )
            except Exception as e:
                self.logger.warning(f"Bulk audit log insert failed: {e}")
                for row in container:
                    try:
                        await record_audit_event(
                            self.sql,
                            row[0],
                            row[1],
                            row[2],
                            user_id=row[3] if len(row) > 3 else None,
                            old_values=row[4] if len(row) > 4 else None,
                            new_values=row[5] if len(row) > 5 else None,
                            reason=row[6] if len(row) > 6 else None,
                        )
                    except Exception as e2:
                        self.logger.warning(f"Audit entry processing failed: {e2}")

        finally:
            self._memory_pool.release("audit_entries", container)

    def _register_container(self, container: Any) -> None:
        """Register a container for lifecycle tracking."""
        self._active_containers.append(container)

        # Clean up if too many containers
        if len(self._active_containers) > self._container_cleanup_threshold:
            # Remove oldest containers
            self._active_containers = self._active_containers[-500:]

    async def _process_claims_with_bulk_line_items(
        self, rows: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process claims with optimized bulk line item fetching using memory pooling."""
        claims: Dict[str, Dict[str, Any]] = {}
        claim_ids = set()

        # Process base claim data using memory pooled containers
        line_keys = {
            "line_number",
            "li_procedure_code",
            "li_units",
            "li_charge_amount",
            "li_service_from_date",
            "li_service_to_date",
        }

        # Acquire containers from memory pool
        claim_containers = []
        line_item_containers = []

        try:
            for row in rows:
                claim_id = row.get("claim_id")
                if not claim_id:
                    continue

                claim_ids.add(claim_id)

                if claim_id not in claims:
                    # Use memory-pooled container for base claim
                    claim_container = self._memory_pool.acquire("claims", dict)
                    claim_container.clear()

                    # Extract base claim data
                    base_data = {k: v for k, v in row.items() if k not in line_keys}
                    claim_container.update(base_data)
                    claim_container.setdefault("line_items", [])

                    claims[claim_id] = claim_container
                    claim_containers.append(claim_container)
                    self._register_container(claim_container)

                # Process line item if present using memory pooling
                if (
                    any(k in row for k in line_keys)
                    and row.get("line_number") is not None
                ):
                    line_item_container = self._memory_pool.acquire("line_items", dict)
                    line_item_container.clear()

                    line_item_container.update(
                        {
                            "line_number": row.get("line_number"),
                            "procedure_code": row.get("li_procedure_code")
                            or row.get("procedure_code"),
                            "units": row.get("li_units"),
                            "charge_amount": row.get("li_charge_amount"),
                            "service_from_date": row.get("li_service_from_date"),
                            "service_to_date": row.get("li_service_to_date"),
                        }
                    )

                    claims[claim_id]["line_items"].append(line_item_container)
                    line_item_containers.append(line_item_container)
                    self._register_container(line_item_container)

            # Add claims to priority queue with bulk operations
            claim_list = list(claims.values())
            priorities = []
            for claim in claim_list:
                pr = int(claim.get("priority", 0) or 0)
                priorities.append((claim, pr))

            # Bulk priority queue operations
            for claim, priority in priorities:
                self.priority_queue.push(claim, priority)

            # Return claims in priority order up to requested batch size
            result = []
            while len(result) < len(claim_list) and len(self.priority_queue) > 0:
                item = self.priority_queue.pop()
                if item:
                    result.append(item)

            return result

        except Exception as e:
            # On error, clean up memory-pooled containers
            for container in claim_containers:
                self._memory_pool.release("claims", container)
            for container in line_item_containers:
                self._memory_pool.release("line_items", container)
            raise

    async def bulk_validate_claims(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, bool]]:
        """Bulk validation of claims using optimized database operations with memory management."""
        if not claims:
            return {}

        # Memory check for large operations
        await self._manage_buffer_memory()

        # Extract unique validation values using memory pooling
        facility_ids_container = self._memory_pool.acquire("temp_sets", set)
        class_ids_container = self._memory_pool.acquire("temp_sets", set)

        try:
            facility_ids_container.clear()
            class_ids_container.clear()

            for c in claims:
                if c.get("facility_id"):
                    facility_ids_container.add(c.get("facility_id"))
                if c.get("financial_class"):
                    class_ids_container.add(c.get("financial_class"))

            facility_ids = list(facility_ids_container)
            class_ids = list(class_ids_container)

        finally:
            self._memory_pool.release("temp_sets", facility_ids_container)
            self._memory_pool.release("temp_sets", class_ids_container)

        # Parallel bulk validation
        validation_tasks = []
        if facility_ids:
            validation_tasks.append(self.sql.bulk_validate_facilities(facility_ids))
        if class_ids:
            validation_tasks.append(self.sql.bulk_validate_financial_classes(class_ids))

        # Execute validations
        results = await asyncio.gather(*validation_tasks)
        facility_valid = results[0] if len(results) > 0 else {}
        class_valid = results[1] if len(results) > 1 else {}

        # Build claim validation results using memory pooling
        claim_validations = {}
        validation_container = self._memory_pool.acquire("validations", dict)

        try:
            for claim in claims:
                claim_id = claim.get("claim_id", "")
                facility_id = claim.get("facility_id")
                financial_class = claim.get("financial_class")

                validation_container.clear()
                validation_container.update(
                    {
                        "facility_valid": (
                            facility_valid.get(facility_id, False)
                            if facility_id
                            else False
                        ),
                        "financial_class_valid": (
                            class_valid.get(financial_class, False)
                            if financial_class
                            else False
                        ),
                    }
                )

                claim_validations[claim_id] = dict(validation_container)

        finally:
            self._memory_pool.release("validations", validation_container)

        metrics.inc("bulk_validations_performed")
        return claim_validations

    async def _fetch_rvu_data_with_pooling(
        self, procedure_codes: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch RVU data using memory pooling for containers."""
        results = {}

        # Use memory pooled container for query results
        query_container = self._memory_pool.acquire("rvu_query_results", dict)

        try:
            query_container.clear()

            # Process in memory-efficient batches
            batch_size = 500
            for i in range(0, len(procedure_codes), batch_size):
                batch = procedure_codes[i : i + batch_size]

                try:
                    # Build batch query
                    placeholders = ", ".join(f"${i+1}" for i in range(len(batch)))
                    query = f"""
                        SELECT procedure_code, description, total_rvu, work_rvu, 
                               practice_expense_rvu, malpractice_rvu, conversion_factor
                        FROM rvu_data 
                        WHERE procedure_code IN ({placeholders}) AND status = 'active'
                    """

                    rows = await self.pg.fetch(query, *batch)

                    for row in rows:
                        code = row.get("procedure_code")
                        if code:
                            query_container.clear()
                            query_container.update(row)
                            results[code] = dict(query_container)

                except Exception as e:
                    self.logger.warning(f"RVU batch fetch failed: {e}")
                    continue

                # Memory check between batches
                if i > 0 and i % (batch_size * 5) == 0:
                    await self._manage_buffer_memory()

        finally:
            self._memory_pool.release("rvu_query_results", query_container)

        return results

    async def enrich_claims_with_rvu(
        self, claims: List[Dict[str, Any]], conversion_factor: float = 36.04
    ) -> List[Dict[str, Any]]:
        """Bulk enrich claims with RVU data and reimbursement calculations using memory management."""
        if not claims:
            return claims

        # Memory check for large operations
        await self._manage_buffer_memory()

        # Extract all procedure codes using memory pooling
        codes_container = self._memory_pool.acquire("temp_sets", set)

        try:
            codes_container.clear()

            for claim in claims:
                if claim.get("procedure_code"):
                    codes_container.add(claim.get("procedure_code"))
                for line_item in claim.get("line_items", []):
                    if line_item.get("procedure_code"):
                        codes_container.add(line_item.get("procedure_code"))

            procedure_codes = list(codes_container)

        finally:
            self._memory_pool.release("temp_sets", codes_container)

        # Bulk fetch RVU data
        rvu_data = await self.get_rvu_data_bulk(procedure_codes)

        # Enrich claims with RVU data using memory pooling
        enriched_claims = []

        for claim in claims:
            # Use memory-pooled container for enriched claim
            enriched_container = self._memory_pool.acquire("enriched_claims", dict)

            try:
                enriched_container.clear()
                enriched_container.update(claim)

                # Process main claim procedure code
                main_code = claim.get("procedure_code")
                if main_code and main_code in rvu_data:
                    rvu = rvu_data[main_code]
                    enriched_container["rvu_value"] = rvu.get("total_rvu")

                    # Calculate reimbursement
                    units = float(claim.get("units", 1) or 1)
                    try:
                        rvu_total = float(rvu.get("total_rvu", 0))
                        enriched_container["reimbursement_amount"] = (
                            rvu_total * units * conversion_factor
                        )
                    except (ValueError, TypeError):
                        enriched_container["reimbursement_amount"] = 0.0

                # Process line items
                for line_item in enriched_container.get("line_items", []):
                    line_code = line_item.get("procedure_code")
                    if line_code and line_code in rvu_data:
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

                # Create copy for results and keep container for pool
                enriched_claims.append(dict(enriched_container))

            finally:
                self._memory_pool.release("enriched_claims", enriched_container)

        metrics.inc("claims_enriched_with_rvu", len(enriched_claims))
        return enriched_claims

    async def insert_claims_ultra_optimized(
        self, claims_data: List[Dict[str, Any]], batch_size: int = 5000
    ) -> int:
        """Ultra-optimized claim insertion with adaptive batch sizing and memory management."""
        if not claims_data:
            return 0

        # Memory-aware batch processing
        memory_aware_batch_size = await self._calculate_memory_aware_insert_batch_size(
            len(claims_data)
        )
        actual_batch_size = min(batch_size, memory_aware_batch_size)

        total_inserted = 0

        # Process with memory pooling
        for i in range(0, len(claims_data), actual_batch_size):
            batch = claims_data[i : i + actual_batch_size]

            # Use memory pool for batch processing objects
            batch_containers = []
            insert_data_container = self._memory_pool.acquire("insert_data", list)

            try:
                insert_data_container.clear()

                # Acquire containers from pool
                for _ in range(len(batch)):
                    container = self._memory_pool.acquire("claims", dict)
                    batch_containers.append(container)

                # Process batch with pooled objects
                for claim, container in zip(batch, batch_containers):
                    container.clear()

                    patient_acct = claim.get("patient_account_number")
                    facility_id = claim.get("facility_id")
                    procedure_code = claim.get("procedure_code")

                    # Apply encryption if configured
                    if self.encryption_key and patient_acct:
                        patient_acct = encrypt_text(
                            str(patient_acct), self.encryption_key
                        )

                    if patient_acct and facility_id:
                        insert_data_container.append(
                            (patient_acct, facility_id, procedure_code)
                        )

                if insert_data_container:
                    try:
                        # Use TVP for SQL Server bulk insert
                        inserted = await self.sql.bulk_insert_tvp_optimized(
                            "claims",
                            ["patient_account_number", "facility_id", "procedure_code"],
                            insert_data_container,
                            chunk_size=actual_batch_size,
                        )
                        total_inserted += inserted

                    except Exception as e:
                        self.logger.warning(f"TVP bulk insert failed: {e}")
                        # Fallback to PostgreSQL COPY
                        try:
                            inserted = await self.pg.copy_records(
                                "claims",
                                [
                                    "patient_account_number",
                                    "facility_id",
                                    "procedure_code",
                                ],
                                insert_data_container,
                            )
                            total_inserted += inserted
                        except Exception as fallback_e:
                            self.logger.error(
                                f"All bulk insert methods failed: {fallback_e}"
                            )
                            raise

            finally:
                # Return containers to pool
                for container in batch_containers:
                    self._memory_pool.release("claims", container)
                self._memory_pool.release("insert_data", insert_data_container)

                # Periodic memory check
                if i > 0 and i % (actual_batch_size * 10) == 0:
                    await self._manage_buffer_memory()

        metrics.inc("bulk_claims_inserted", total_inserted)
        self._bulk_operations_count += 1
        return total_inserted

    async def _calculate_memory_aware_insert_batch_size(
        self, total_records: int
    ) -> int:
        """Calculate memory-aware batch size for inserts."""
        try:
            # Get available memory
            available_memory = psutil.virtual_memory().available / 1024 / 1024  # MB

            # Calculate batch size based on available memory
            # Assume ~1KB per record for processing
            max_batch_by_memory = int(
                available_memory * 0.1 * 1024
            )  # Use 10% of available memory

            # Consider total records to process
            if total_records < 1000:
                return min(total_records, 1000)
            elif total_records < 10000:
                return min(total_records // 4, max_batch_by_memory, 5000)
            else:
                return min(10000, max_batch_by_memory)

        except Exception:
            return 5000  # Default fallback

    async def record_failed_claims_bulk(
        self, failed_claims: List[Tuple[str, str, str, str, str, str, str, str]]
    ) -> None:
        """Optimized bulk failed claims recording with memory management."""
        if not failed_claims:
            return

        # Use memory pooling for failed claims processing
        failed_claims_container = self._memory_pool.acquire("failed_claims_data", list)

        try:
            failed_claims_container.clear()
            failed_claims_container.extend(failed_claims)

            # Use prepared statement for optimal performance
            await self.sql.execute_many_optimized(
                """INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                failed_claims_container,
                concurrency=4,
                batch_size=500,
            )
            metrics.inc("bulk_failed_claims_recorded", len(failed_claims))

        except Exception as e:
            self.logger.warning(f"Bulk failed claims recording failed: {e}")
            # Fallback to individual inserts
            for claim_data in failed_claims:
                try:
                    await self.sql.execute_prepared("insert_failed_claim", *claim_data)
                except Exception:
                    continue
        finally:
            self._memory_pool.release("failed_claims_data", failed_claims_container)

    async def record_failed_claim_optimized(
        self,
        claim: Dict[str, Any],
        reason: str,
        suggestions: str,
        category: str = "processing",
    ) -> None:
        """Optimized failed claim recording with buffering and memory management."""
        # Encrypt sensitive data using memory pooling
        encrypted_claim_container = self._memory_pool.acquire("encrypted_claims", dict)

        try:
            encrypted_claim_container.clear()
            encrypted_claim_container.update(
                encrypt_claim_fields(claim, self.encryption_key)
            )

            original_data = (
                encrypt_text(str(claim), self.encryption_key)
                if self.encryption_key
                else str(claim)
            )

            # Prepare data for bulk insert
            failed_claim_data = (
                encrypted_claim_container.get("claim_id"),
                encrypted_claim_container.get("facility_id"),
                encrypted_claim_container.get("patient_account_number"),
                reason,
                category,
                "validation",
                original_data,
                suggestions,
            )

            # Use bulk recording for better performance
            await self.record_failed_claims_bulk([failed_claim_data])

        finally:
            self._memory_pool.release("encrypted_claims", encrypted_claim_container)

        # Update metrics
        metrics.inc(f"errors_{category}")

        # Record audit event asynchronously
        self.audit_event(
            "failed_claims",
            claim.get("claim_id", ""),
            "insert",
            new_values=claim,
            reason=reason,
        )

        # Add to dead letter queue
        await self.enqueue_dead_letter_optimized(claim, reason)

        # Add to retry queue with priority
        pr = int(claim.get("priority", 0) or 0)
        self.retry_queue.push(claim, pr)

    async def _record_audit_async(
        self,
        table_name: str,
        record_id: str,
        operation: str,
        new_values: Dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        """Asynchronous audit recording to avoid blocking main processing."""
        try:
            await record_audit_event(
                self.sql,
                table_name,
                record_id,
                operation,
                new_values=new_values,
                reason=reason,
            )
        except Exception as e:
            self.logger.warning(f"Audit recording failed: {e}")

    def audit_event(
        self,
        table_name: str,
        record_id: str,
        operation: str,
        new_values: Dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        """Schedule an audit event to be recorded asynchronously."""
        asyncio.create_task(
            self._record_audit_async(
                table_name,
                record_id,
                operation,
                new_values=new_values,
                reason=reason,
            )
        )

    async def get_processing_metrics_bulk(
        self, facility_ids: List[str] | None = None
    ) -> Dict[str, Any]:
        """Get processing metrics with bulk operations and memory management."""
        # Use memory pooling for metrics data
        metrics_container = self._memory_pool.acquire("metrics_data", dict)

        try:
            metrics_container.clear()

            # Base metrics query
            base_query = """
                SELECT 
                    COUNT(*) as total_claims,
                    SUM(CASE WHEN processing_status = 'completed' THEN 1 ELSE 0 END) as completed_claims,
                    SUM(CASE WHEN processing_status = 'failed' THEN 1 ELSE 0 END) as failed_claims,
                    AVG(total_charge_amount) as avg_charge_amount,
                    SUM(total_charge_amount) as total_charge_amount
                FROM claims
            """

            if facility_ids:
                placeholders = ",".join("?" * len(facility_ids))
                base_query += f" WHERE facility_id IN ({placeholders})"
                base_rows = await self.sql.fetch_optimized(base_query, *facility_ids)
            else:
                base_rows = await self.sql.fetch_optimized(base_query)

            if base_rows:
                row = base_rows[0]
                metrics_container.update(
                    {
                        "total_claims": row.get("total_claims", 0),
                        "completed_claims": row.get("completed_claims", 0),
                        "failed_claims": row.get("failed_claims", 0),
                        "avg_charge_amount": float(row.get("avg_charge_amount") or 0),
                        "total_charge_amount": float(
                            row.get("total_charge_amount") or 0
                        ),
                    }
                )

            # Failed claims breakdown
            failed_query = """
                SELECT failure_category, COUNT(*) as count
                FROM failed_claims
                GROUP BY failure_category
            """
            failed_rows = await self.sql.fetch_optimized(failed_query)
            metrics_container["failure_breakdown"] = {
                row.get("failure_category", "unknown"): row.get("count", 0)
                for row in failed_rows
            }

            # Performance insights
            if hasattr(self.sql, "get_performance_insights"):
                metrics_container[
                    "performance_insights"
                ] = await self.sql.get_performance_insights()

            return dict(metrics_container)

        except Exception as e:
            self.logger.warning(f"Failed to get processing metrics: {e}")
            return {"error": str(e)}
        finally:
            self._memory_pool.release("metrics_data", metrics_container)

    async def perform_maintenance_operations(self) -> Dict[str, Any]:
        """Perform routine maintenance operations for optimal performance with memory management."""
        maintenance_container = self._memory_pool.acquire("maintenance_results", dict)

        try:
            maintenance_container.clear()

            # Update statistics for query optimization
            stats_updated = 0
            critical_tables = [
                "claims",
                "failed_claims",
                "rvu_data",
                "facilities",
                "facility_financial_classes",
            ]

            for table in critical_tables:
                try:
                    await self.sql.execute_optimized(f"UPDATE STATISTICS {table}")
                    stats_updated += 1
                except Exception as e:
                    self.logger.warning(f"Failed to update statistics for {table}: {e}")

            maintenance_container["statistics_updated"] = stats_updated

            # Memory management operations
            await self._manage_buffer_memory()

            # Clean up memory pools
            cleanup_stats = self._memory_pool.cleanup_all()
            maintenance_container["memory_pool_cleanup"] = cleanup_stats

            # Validate connection pools
            pg_healthy = await self.pg.health_check()
            sql_healthy = await self.sql.health_check()

            maintenance_container["connection_health"] = {
                "postgres": pg_healthy,
                "sqlserver": sql_healthy,
            }

            # Optimize connection pools
            if hasattr(self.pg, "adjust_pool_size"):
                await self.pg.adjust_pool_size()
            if hasattr(self.sql, "adjust_pool_size"):
                await self.sql.adjust_pool_size()

            maintenance_container["pools_optimized"] = True

            # Flush any pending operations
            await self.flush_checkpoints()

            maintenance_container["buffers_flushed"] = True

            # Force garbage collection
            collected = gc.collect()
            maintenance_container["gc_objects_collected"] = collected

            return dict(maintenance_container)

        except Exception as e:
            return {"error": str(e)}
        finally:
            self._memory_pool.release("maintenance_results", maintenance_container)

    def get_service_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive service performance statistics including memory management and circuit breaker info."""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
        except Exception:
            memory_mb = 0

        return {
            "caching": {
                "validation_cache_hits": self._cache_hit_count,
                "validation_cache_misses": self._cache_miss_count,
                "cache_hit_ratio": (
                    self._cache_hit_count
                    / (self._cache_hit_count + self._cache_miss_count)
                    if (self._cache_hit_count + self._cache_miss_count) > 0
                    else 0.0
                ),
                "facilities_cached": len(self._facilities_cache),
                "classes_cached": len(self._classes_cache),
            },
            "buffering": {
                "checkpoint_buffer_size": len(self._checkpoint_buffer),
                "checkpoint_buffer_capacity": self._adaptive_checkpoint_buffer_size,
                "audit_buffer_size": len(self._audit_buffer),
                "audit_buffer_capacity": self._adaptive_audit_buffer_size,
            },
            "queues": {
                "priority_queue_size": len(self.priority_queue),
                "retry_queue_size": len(self.retry_queue),
            },
            "operations": {
                "bulk_operations_count": self._bulk_operations_count,
            },
            "memory_management": {
                "current_memory_mb": memory_mb,
                "memory_limit_mb": self._total_memory_limit / 1024 / 1024,
                "memory_utilization": memory_mb
                / (self._total_memory_limit / 1024 / 1024),
                "memory_pools": self._memory_pool.get_stats(),
                "active_containers": len(self._active_containers),
                "emergency_cleanups": metrics.get("service_emergency_cleanups"),
                "buffer_memory_reductions": metrics.get("buffer_memory_reductions"),
            },
            "circuit_breaker": {
                "recovery_attempts": self._circuit_breaker_recovery_attempts,
                "max_recovery_attempts": self._max_recovery_attempts,
                "last_reset_time": self._last_circuit_breaker_reset,
                "reset_interval": self._circuit_breaker_reset_interval,
            },
            "optimization_features": {
                "prepared_statements": True,
                "bulk_operations": True,
                "connection_pooling": True,
                "query_caching": True,
                "validation_caching": True,
                "async_audit_logging": True,
                "bulk_validation": True,
                "rvu_bulk_lookup": True,
                "dead_letter_optimization": True,
                "maintenance_automation": True,
                "memory_pooling": True,
                "memory_monitoring": True,
                "adaptive_buffer_sizing": True,
                "container_lifecycle_management": True,
                "emergency_memory_cleanup": True,
                "circuit_breaker_handling": True,
                "automatic_error_recovery": True,
            },
        }

    def reset_stats(self) -> None:
        """Reset service statistics and clean up memory."""
        # Reset counters
        self._bulk_operations_count = 0
        self._cache_hit_count = 0
        self._cache_miss_count = 0
        self._circuit_breaker_recovery_attempts = 0

        # Clear large caches but keep recent data
        if len(self._facilities_cache) > 1000:
            # Keep most recent 500 facilities
            recent_facilities = list(self._facilities_cache)[-500:]
            self._facilities_cache = set(recent_facilities)

        if len(self._classes_cache) > 1000:
            # Keep most recent 500 classes
            recent_classes = list(self._classes_cache)[-500:]
            self._classes_cache = set(recent_classes)

        # Clear object references
        self._object_refs.clear()

        # Clear active containers
        self._active_containers.clear()

        # Clean up memory pools
        self._memory_pool.cleanup_all()

        # Force garbage collection
        gc.collect()

        metrics.inc("service_stats_resets")

    # Backward compatibility methods
    async def load_validation_sets(self) -> tuple[set[str], set[str]]:
        """Backward compatibility wrapper."""
        return await self.load_validation_sets_optimized()

    async def insert_claims(
        self, rows: Iterable[Iterable[Any]], concurrency: int = 1
    ) -> None:
        """Backward compatibility wrapper."""
        # Convert rows to claim dictionaries for optimized processing
        claims_data = []
        for row in rows:
            row_list = list(row)
            if len(row_list) >= 2:
                claims_data.append(
                    {
                        "patient_account_number": row_list[0],
                        "facility_id": row_list[1],
                        "procedure_code": row_list[2] if len(row_list) > 2 else None,
                    }
                )

        await self.insert_claims_ultra_optimized(claims_data)

    async def record_failed_claim(
        self,
        claim: Dict[str, Any],
        reason: str,
        suggestions: str,
        category: str = "processing",
    ) -> None:
        """Backward compatibility wrapper."""
        await self.record_failed_claim_optimized(claim, reason, suggestions, category)

    async def reprocess_dead_letter(self, batch_size: int = 500) -> None:
        """Backward compatibility wrapper."""
        await self.reprocess_dead_letter_optimized(batch_size)

    async def top_rvu_codes(self, limit: int = 1000) -> list[str]:
        """Backward compatibility wrapper."""
        return await self.top_rvu_codes_optimized(limit)

    # Additional convenience methods for the optimized service
    async def bulk_insert_postgres_copy(self, claims: Iterable[Dict[str, Any]]) -> int:
        """Bulk insert using PostgreSQL COPY with memory management."""
        claims_list = list(claims)
        return await self.insert_claims_ultra_optimized(claims_list)

    async def bulk_insert_sqlserver_tvp(self, claims: Iterable[Dict[str, Any]]) -> int:
        """Bulk insert using SQL Server TVP with memory management."""
        claims_list = list(claims)
        return await self.insert_claims_ultra_optimized(claims_list)

    async def amend_claim(self, claim_id: str, updates: Dict[str, Any]) -> None:
        """Update an existing claim with new values using memory management."""
        if not updates:
            return

        # Use memory pooling for update operations
        update_container = self._memory_pool.acquire("claim_updates", dict)

        try:
            update_container.clear()
            update_container.update(updates)

            set_clause = ", ".join(f"{k} = ?" for k in update_container)
            params = list(update_container.values())
            params.append(claim_id)

            await self.sql.execute(
                f"UPDATE claims SET {set_clause} WHERE claim_id = ?",
                *params,
            )
            self.audit_event(
                "claims",
                claim_id,
                "update",
                new_values=dict(update_container),
            )

        finally:
            self._memory_pool.release("claim_updates", update_container)

    async def assign_failed_claim(self, claim_id: str, user: str) -> None:
        """Assign a failed claim to a user for manual review."""
        await self.sql.execute(
            "UPDATE failed_claims SET assigned_to = ?, resolution_status = 'assigned' WHERE claim_id = ?",
            user,
            claim_id,
        )
        self.audit_event(
            "failed_claims",
            claim_id,
            "assign",
            new_values={"user": user},
        )

    async def resolve_failed_claim(
        self, claim_id: str, action: str, notes: str | None = None
    ) -> None:
        """Mark a failed claim as manually resolved."""
        await self.sql.execute(
            "UPDATE failed_claims SET resolution_status = 'resolved', resolved_at = GETDATE(), resolution_action = ?, resolution_notes = ? WHERE claim_id = ?",
            action,
            notes or "",
            claim_id,
        )
        metrics.inc("failed_claims_manual")
        self.audit_event(
            "failed_claims",
            claim_id,
            "resolve",
            new_values={"action": action, "notes": notes},
        )

    async def delete_claim(self, account: str, facility: str) -> None:
        """Delete a claim (used for compensation transactions)."""
        await self.sql.execute(
            "DELETE FROM claims WHERE patient_account_number = ? AND facility_id = ?",
            account,
            facility,
        )
        self.audit_event(
            "claims",
            account,
            "delete",
            reason="compensation",
        )

    def get_memory_status(self) -> Dict[str, Any]:
        """Get current memory status for the service."""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            return {
                "current_memory_mb": memory_mb,
                "memory_limit_mb": self._total_memory_limit / 1024 / 1024,
                "memory_utilization": memory_mb
                / (self._total_memory_limit / 1024 / 1024),
                "buffer_memory_estimate": {
                    "checkpoint_buffer_mb": len(self._checkpoint_buffer)
                    * 100
                    / 1024
                    / 1024,
                    "audit_buffer_mb": len(self._audit_buffer) * 500 / 1024 / 1024,
                },
                "memory_pools": self._memory_pool.get_stats(),
                "active_containers": len(self._active_containers),
                "adaptive_buffer_sizes": {
                    "checkpoint_buffer_size": self._adaptive_checkpoint_buffer_size,
                    "audit_buffer_size": self._adaptive_audit_buffer_size,
                },
                "last_memory_check": self._last_memory_check,
                "memory_check_interval": self._memory_check_interval,
                "circuit_breaker_status": {
                    "recovery_attempts": self._circuit_breaker_recovery_attempts,
                    "last_reset": self._last_circuit_breaker_reset,
                },
            }
        except Exception as e:
            return {"error": str(e)}

    async def cleanup_service_memory(self) -> Dict[str, Any]:
        """Manual memory cleanup for the service."""
        await self._emergency_memory_cleanup()
        await self._manage_buffer_memory()
        self.reset_stats()
        
        return {
            "emergency_cleanup": "completed",
            "buffer_management": "completed", 
            "stats_reset": "completed",
            "memory_status": self.get_memory_status(),
            "circuit_breaker_reset": await self.reset_circuit_breakers(),
        }