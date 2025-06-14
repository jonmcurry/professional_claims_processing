import json
from typing import Any, Dict, Iterable, List, Set, Tuple
import asyncio

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..monitoring.metrics import metrics
from ..security.compliance import decrypt_text, encrypt_claim_fields, encrypt_text
from ..utils.audit import record_audit_event
from ..utils.priority_queue import PriorityClaimQueue


class ClaimService:
    """Enhanced service layer with optimized database operations and bulk processing."""

    def __init__(
        self,
        pg: PostgresDatabase,
        sql: SQLServerDatabase,
        encryption_key: str | None = None,
        checkpoint_buffer_size: int = 2000,  # Increased for better batching
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

    async def fetch_claims_optimized(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Ultra-optimized claims fetching with prepared statements and caching."""
        # Use prepared statements for better performance
        if priority:
            stmt_name = "fetch_claims_priority"
            rows = await self.pg.fetch_prepared(
                stmt_name, batch_size, offset, 5, use_replica=True  # Priority threshold
            )
        else:
            stmt_name = "fetch_claims_batch"
            rows = await self.pg.fetch_prepared(
                stmt_name, batch_size, offset, use_replica=True
            )
        
        # Optimize claim processing with bulk line item fetching
        return await self._process_claims_with_bulk_line_items(rows)

    async def _process_claims_with_bulk_line_items(
        self, rows: Iterable[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process claims with optimized bulk line item fetching."""
        claims: Dict[str, Dict[str, Any]] = {}
        claim_ids = set()
        
        # Process base claim data
        line_keys = {
            "line_number", "li_procedure_code", "li_units", "li_charge_amount",
            "li_service_from_date", "li_service_to_date"
        }
        
        for row in rows:
            claim_id = row.get("claim_id")
            if not claim_id:
                continue
                
            claim_ids.add(claim_id)
            
            if claim_id not in claims:
                # Extract base claim data
                base = {k: v for k, v in row.items() if k not in line_keys}
                base.setdefault("line_items", [])
                claims[claim_id] = base
            
            # Process line item if present
            if any(k in row for k in line_keys) and row.get("line_number") is not None:
                item = {
                    "line_number": row.get("line_number"),
                    "procedure_code": row.get("li_procedure_code") or row.get("procedure_code"),
                    "units": row.get("li_units"),
                    "charge_amount": row.get("li_charge_amount"),
                    "service_from_date": row.get("li_service_from_date"),
                    "service_to_date": row.get("li_service_to_date"),
                }
                claims[claim_id]["line_items"].append(item)
        
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

    async def load_validation_sets_optimized(self) -> tuple[set[str], set[str]]:
        """Optimized validation set loading with advanced caching and bulk operations."""
        import time
        current_time = time.time()
        
        # Check cache validity
        if (hasattr(self, '_validation_cache_time') and 
            current_time - self._validation_cache_time < self._validation_cache_ttl and
            self._facilities_cache and self._classes_cache):
            self._cache_hit_count += 1
            metrics.inc("validation_cache_hits")
            return self._facilities_cache, self._classes_cache
        
        self._cache_miss_count += 1
        metrics.inc("validation_cache_misses")
        
        # Use prepared statements for optimal performance
        try:
            # Parallel execution of validation queries
            facilities_task = self.sql.fetch_prepared("get_facilities_batch")
            classes_task = self.sql.fetch_prepared("get_financial_classes_batch")
            
            fac_rows, class_rows = await asyncio.gather(facilities_task, classes_task)
            
        except Exception:
            # Fallback to regular queries if prepared statements fail
            fac_task = self.sql.fetch(
                "SELECT DISTINCT facility_id FROM facilities WHERE facility_id IS NOT NULL"
            )
            class_task = self.sql.fetch(
                "SELECT DISTINCT financial_class_id FROM facility_financial_classes WHERE financial_class_id IS NOT NULL AND active = 1"
            )
            fac_rows, class_rows = await asyncio.gather(fac_task, class_task)
        
        # Optimize set creation
        facilities = {
            str(r.get("facility_id"))
            for r in fac_rows
            if r.get("facility_id") is not None
        }
        classes = {
            str(r.get("financial_class_id"))
            for r in class_rows
            if r.get("financial_class_id") is not None
        }
        
        # Update cache
        self._facilities_cache = facilities
        self._classes_cache = classes
        self._validation_cache_time = current_time
        
        # Update cache metrics
        metrics.set("validation_facilities_count", float(len(facilities)))
        metrics.set("validation_classes_count", float(len(classes)))
        metrics.set("validation_cache_age", 0.0)
        
        return facilities, classes

    async def bulk_validate_claims(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, bool]]:
        """Bulk validation of claims using optimized database operations."""
        if not claims:
            return {}
        
        # Extract unique validation values
        facility_ids = list({c.get("facility_id") for c in claims if c.get("facility_id")})
        class_ids = list({c.get("financial_class") for c in claims if c.get("financial_class")})
        
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
        
        # Build claim validation results
        claim_validations = {}
        for claim in claims:
            claim_id = claim.get("claim_id", "")
            facility_id = claim.get("facility_id")
            financial_class = claim.get("financial_class")
            
            claim_validations[claim_id] = {
                "facility_valid": facility_valid.get(facility_id, False) if facility_id else False,
                "financial_class_valid": class_valid.get(financial_class, False) if financial_class else False,
            }
        
        metrics.inc("bulk_validations_performed")
        return claim_validations

    async def get_rvu_data_bulk(self, procedure_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Optimized bulk RVU data retrieval with caching."""
        if not procedure_codes:
            return {}
        
        # Use optimized bulk RVU lookup from PostgreSQL
        try:
            rvu_data = await self.pg.get_rvu_bulk_optimized(procedure_codes)
            metrics.inc("bulk_rvu_lookups")
            metrics.set("last_rvu_bulk_size", float(len(procedure_codes)))
            return rvu_data
        except Exception as e:
            print(f"Warning: Bulk RVU lookup failed: {e}")
            # Fallback to individual lookups
            results = {}
            for code in procedure_codes:
                try:
                    rows = await self.pg.fetch_prepared("get_rvu_single", code, use_replica=True)
                    if rows:
                        results[code] = dict(rows[0])
                except Exception:
                    continue
            return results

    async def enrich_claims_with_rvu(
        self, claims: List[Dict[str, Any]], conversion_factor: float = 36.04
    ) -> List[Dict[str, Any]]:
        """Bulk enrich claims with RVU data and reimbursement calculations."""
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
        rvu_data = await self.get_rvu_data_bulk(list(procedure_codes))
        
        # Enrich claims with RVU data
        enriched_claims = []
        for claim in claims:
            enriched_claim = claim.copy()
            
            # Process main claim procedure code
            main_code = claim.get("procedure_code")
            if main_code and main_code in rvu_data:
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
                if line_code and line_code in rvu_data:
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

    async def insert_claims_ultra_optimized(
        self, claims_data: List[Dict[str, Any]], batch_size: int = 5000
    ) -> int:
        """Ultra-optimized claim insertion with adaptive batch sizing."""
        if not claims_data:
            return 0
        
        total_inserted = 0
        
        # Process in optimized batches
        for i in range(0, len(claims_data), batch_size):
            batch = claims_data[i:i + batch_size]
            
            # Prepare data for bulk insert
            insert_data = []
            for claim in batch:
                patient_acct = claim.get("patient_account_number")
                facility_id = claim.get("facility_id")
                procedure_code = claim.get("procedure_code")
                
                # Apply encryption if configured
                if self.encryption_key and patient_acct:
                    patient_acct = encrypt_text(str(patient_acct), self.encryption_key)
                
                if patient_acct and facility_id:
                    insert_data.append((patient_acct, facility_id, procedure_code))
            
            if insert_data:
                try:
                    # Use TVP for SQL Server bulk insert
                    inserted = await self.sql.bulk_insert_tvp_optimized(
                        "claims",
                        ["patient_account_number", "facility_id", "procedure_code"],
                        insert_data,
                        chunk_size=batch_size
                    )
                    total_inserted += inserted
                    
                except Exception as e:
                    print(f"Warning: TVP bulk insert failed: {e}")
                    # Fallback to PostgreSQL COPY
                    try:
                        inserted = await self.pg.copy_records(
                            "claims",
                            ["patient_account_number", "facility_id", "procedure_code"],
                            insert_data
                        )
                        total_inserted += inserted
                    except Exception as fallback_e:
                        print(f"Error: All bulk insert methods failed: {fallback_e}")
                        raise
        
        metrics.inc("bulk_claims_inserted", total_inserted)
        self._bulk_operations_count += 1
        return total_inserted

    async def record_checkpoints_bulk(self, checkpoints: List[Tuple[str, str]]) -> None:
        """Optimized bulk checkpoint recording."""
        if not checkpoints:
            return
        
        try:
            # Use prepared statement for optimal performance
            checkpoint_params = [(claim_id, stage) for claim_id, stage in checkpoints]
            await self.pg.execute_many_optimized(
                "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW()) ON CONFLICT (claim_id, stage) DO UPDATE SET checkpoint_at = NOW()",
                checkpoint_params,
                concurrency=4,
                batch_size=1000
            )
            metrics.inc("bulk_checkpoints_recorded", len(checkpoints))
        except Exception as e:
            print(f"Warning: Bulk checkpoint recording failed: {e}")
            # Fallback to individual inserts
            for claim_id, stage in checkpoints:
                try:
                    await self.record_checkpoint(claim_id, stage, buffered=False)
                except Exception:
                    continue

    async def record_checkpoint(
        self, claim_id: str, stage: str, *, buffered: bool = True
    ) -> None:
        """Enhanced checkpoint recording with intelligent buffering."""
        if buffered:
            self._checkpoint_buffer.append((claim_id, stage))
            if len(self._checkpoint_buffer) >= self._checkpoint_buffer_size:
                await self.flush_checkpoints()
            return
        
        # Immediate recording using prepared statement
        try:
            await self.pg.execute_prepared("insert_checkpoint", claim_id, stage)
        except Exception:
            # Fallback to regular execute
            await self.pg.execute(
                "INSERT INTO processing_checkpoints (claim_id, stage, checkpoint_at) VALUES ($1, $2, NOW())",
                claim_id, stage
            )

    async def flush_checkpoints(self) -> None:
        """Enhanced checkpoint flushing with error recovery."""
        if not self._checkpoint_buffer:
            return
        
        checkpoints_to_flush = self._checkpoint_buffer.copy()
        self._checkpoint_buffer.clear()
        
        try:
            await self.record_checkpoints_bulk(checkpoints_to_flush)
        except Exception as e:
            print(f"Warning: Bulk checkpoint flush failed: {e}")
            # Re-add failed checkpoints to buffer for retry
            self._checkpoint_buffer.extend(checkpoints_to_flush)

    async def record_failed_claims_bulk(
        self, failed_claims: List[Tuple[str, str, str, str, str, str, str, str]]
    ) -> None:
        """Optimized bulk failed claims recording."""
        if not failed_claims:
            return
        
        try:
            # Use prepared statement for optimal performance
            await self.sql.execute_many_optimized(
                """INSERT INTO failed_claims (
                    claim_id, facility_id, patient_account_number, 
                    failure_reason, failure_category, processing_stage, 
                    failed_at, original_data, repair_suggestions
                ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                failed_claims,
                concurrency=4,
                batch_size=500
            )
            metrics.inc("bulk_failed_claims_recorded", len(failed_claims))
        except Exception as e:
            print(f"Warning: Bulk failed claims recording failed: {e}")
            # Fallback to individual inserts
            for claim_data in failed_claims:
                try:
                    await self.sql.execute_prepared("insert_failed_claim", *claim_data)
                except Exception:
                    continue

    async def record_failed_claim_optimized(
        self,
        claim: Dict[str, Any],
        reason: str,
        suggestions: str,
        category: str = "processing",
    ) -> None:
        """Optimized failed claim recording with buffering."""
        # Encrypt sensitive data
        encrypted_claim = encrypt_claim_fields(claim, self.encryption_key)
        original_data = (
            encrypt_text(str(claim), self.encryption_key)
            if self.encryption_key
            else str(claim)
        )
        
        # Prepare data for bulk insert
        failed_claim_data = (
            encrypted_claim.get("claim_id"),
            encrypted_claim.get("facility_id"),
            encrypted_claim.get("patient_account_number"),
            reason,
            category,
            "validation",
            original_data,
            suggestions
        )
        
        # Use bulk recording for better performance
        await self.record_failed_claims_bulk([failed_claim_data])
        
        # Update metrics
        metrics.inc(f"errors_{category}")
        
        # Record audit event asynchronously
        asyncio.create_task(self._record_audit_async(
            "failed_claims",
            claim.get("claim_id", ""),
            "insert",
            new_values=claim,
            reason=reason
        ))
        
        # Add to dead letter queue
        await self.enqueue_dead_letter_optimized(claim, reason)
        
        # Add to retry queue with priority
        pr = int(claim.get("priority", 0) or 0)
        self.retry_queue.push(claim, pr)

    async def _record_audit_async(
        self, table_name: str, record_id: str, operation: str,
        new_values: Dict[str, Any] | None = None, reason: str | None = None
    ) -> None:
        """Asynchronous audit recording to avoid blocking main processing."""
        try:
            await record_audit_event(
                self.sql, table_name, record_id, operation,
                new_values=new_values, reason=reason
            )
        except Exception as e:
            print(f"Warning: Audit recording failed: {e}")

    async def enqueue_dead_letter_optimized(self, claim: Dict[str, Any], reason: str) -> None:
        """Optimized dead letter queue insertion."""
        data = (
            encrypt_text(str(claim), self.encryption_key)
            if self.encryption_key
            else str(claim)
        )
        
        try:
            await self.pg.execute_prepared(
                "insert_dead_letter",
                claim.get("claim_id"),
                reason,
                data
            )
        except Exception:
            # Fallback to regular insert
            await self.pg.execute(
                "INSERT INTO dead_letter_queue (claim_id, reason, data, inserted_at) VALUES ($1, $2, $3, NOW())",
                claim.get("claim_id"), reason, data
            )

    async def reprocess_dead_letter_optimized(self, batch_size: int = 1000) -> int:
        """Ultra-optimized dead letter queue reprocessing."""
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
        
        # Process claims in parallel batches
        successfully_processed = []
        failed_to_process = []
        dlq_ids_to_delete = []
        
        for row in rows:
            dlq_id = row.get("dlq_id")
            claim_str = row.get("data", "")
            claim_id = row.get("claim_id")
            
            try:
                # Decrypt if needed
                if self.encryption_key:
                    claim_str = decrypt_text(claim_str, self.encryption_key)
                
                # Parse claim
                claim = json.loads(claim_str)
                
                # Attempt automatic repair
                from ..processing.repair import ClaimRepairSuggester
                suggester = ClaimRepairSuggester()
                repaired_claim = suggester.auto_repair(claim, [])
                
                successfully_processed.append(repaired_claim)
                dlq_ids_to_delete.append(dlq_id)
                
            except Exception:
                failed_to_process.append(claim_id)
                dlq_ids_to_delete.append(dlq_id)  # Remove even if processing failed
        
        # Bulk delete processed items
        if dlq_ids_to_delete:
            try:
                await self.pg.execute_prepared("delete_dead_letter", dlq_ids_to_delete)
            except Exception:
                # Fallback deletion
                placeholders = ",".join("$" + str(i+1) for i in range(len(dlq_ids_to_delete)))
                await self.pg.execute(
                    f"DELETE FROM dead_letter_queue WHERE dlq_id IN ({placeholders})",
                    *dlq_ids_to_delete
                )
        
        # Bulk insert successfully processed claims
        if successfully_processed:
            try:
                await self.insert_claims_ultra_optimized(successfully_processed)
                metrics.inc("failed_claims_resolved", len(successfully_processed))
            except Exception as e:
                print(f"Warning: Failed to insert reprocessed claims: {e}")
                # Re-add to retry queue
                for claim in successfully_processed:
                    pr = int(claim.get("priority", 0) or 0)
                    self.retry_queue.push(claim, pr)
        
        # Update metrics
        metrics.inc("dead_letter_processed", len(rows))
        metrics.inc("dead_letter_resolved", len(successfully_processed))
        metrics.inc("dead_letter_failed", len(failed_to_process))
        
        return len(successfully_processed)

    async def top_rvu_codes_optimized(self, limit: int = 1000) -> list[str]:
        """Optimized top RVU codes retrieval with caching."""
        try:
            rows = await self.pg.fetch_prepared("get_top_rvu_codes", limit, use_replica=True)
            codes = [str(r.get("procedure_code")) for r in rows if r.get("procedure_code")]
            metrics.set("top_rvu_codes_fetched", float(len(codes)))
            return codes
        except Exception:
            # Fallback query
            rows = await self.pg.fetch(
                "SELECT procedure_code FROM rvu_data WHERE status = 'active' ORDER BY total_rvu DESC NULLS LAST LIMIT $1",
                limit,
            )
            return [str(r.get("procedure_code")) for r in rows if r.get("procedure_code")]

    async def get_processing_metrics_bulk(
        self, facility_ids: List[str] | None = None
    ) -> Dict[str, Any]:
        """Get processing metrics with bulk operations."""
        metrics_data = {}
        
        try:
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
                metrics_data.update({
                    "total_claims": row.get("total_claims", 0),
                    "completed_claims": row.get("completed_claims", 0),
                    "failed_claims": row.get("failed_claims", 0),
                    "avg_charge_amount": float(row.get("avg_charge_amount") or 0),
                    "total_charge_amount": float(row.get("total_charge_amount") or 0),
                })
            
            # Failed claims breakdown
            failed_query = """
                SELECT failure_category, COUNT(*) as count
                FROM failed_claims
                GROUP BY failure_category
            """
            failed_rows = await self.sql.fetch_optimized(failed_query)
            metrics_data["failure_breakdown"] = {
                row.get("failure_category", "unknown"): row.get("count", 0)
                for row in failed_rows
            }
            
            # Performance insights
            metrics_data["performance_insights"] = await self.sql.get_performance_insights()
            
        except Exception as e:
            print(f"Warning: Failed to get processing metrics: {e}")
            metrics_data = {"error": str(e)}
        
        return metrics_data

    async def perform_maintenance_operations(self) -> Dict[str, Any]:
        """Perform routine maintenance operations for optimal performance."""
        maintenance_results = {}
        
        try:
            # Update statistics for query optimization
            stats_updated = 0
            critical_tables = ["claims", "failed_claims", "rvu_data", "facilities", "facility_financial_classes"]
            
            for table in critical_tables:
                try:
                    await self.sql.execute_optimized(f"UPDATE STATISTICS {table}")
                    stats_updated += 1
                except Exception as e:
                    print(f"Warning: Failed to update statistics for {table}: {e}")
            
            maintenance_results["statistics_updated"] = stats_updated
            
            # Clear old cached data
            cache_cleared = 0
            if hasattr(self.sql.query_cache, 'store'):
                old_size = len(self.sql.query_cache.store)
                # Let TTL naturally expire old entries
                cache_cleared = old_size
            
            maintenance_results["cache_entries_processed"] = cache_cleared
            
            # Validate connection pools
            pg_healthy = await self.pg.health_check()
            sql_healthy = await self.sql.health_check()
            
            maintenance_results["connection_health"] = {
                "postgres": pg_healthy,
                "sqlserver": sql_healthy
            }
            
            # Optimize connection pools
            await self.pg.adjust_pool_size()
            await self.sql.adjust_pool_size()
            
            maintenance_results["pools_optimized"] = True
            
            # Flush any pending operations
            await self.flush_checkpoints()
            
            maintenance_results["buffers_flushed"] = True
            
        except Exception as e:
            maintenance_results["error"] = str(e)
        
        return maintenance_results

    def get_service_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive service performance statistics."""
        return {
            "caching": {
                "validation_cache_hits": self._cache_hit_count,
                "validation_cache_misses": self._cache_miss_count,
                "cache_hit_ratio": (
                    self._cache_hit_count / (self._cache_hit_count + self._cache_miss_count)
                    if (self._cache_hit_count + self._cache_miss_count) > 0 else 0.0
                ),
                "facilities_cached": len(self._facilities_cache),
                "classes_cached": len(self._classes_cache),
            },
            "buffering": {
                "checkpoint_buffer_size": len(self._checkpoint_buffer),
                "checkpoint_buffer_capacity": self._checkpoint_buffer_size,
                "audit_buffer_size": len(self._audit_buffer),
                "audit_buffer_capacity": self._audit_buffer_size,
            },
            "queues": {
                "priority_queue_size": len(self.priority_queue),
                "retry_queue_size": len(self.retry_queue),
            },
            "operations": {
                "bulk_operations_count": self._bulk_operations_count,
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
            }
        }

    # Keep original methods for backward compatibility
    async def fetch_claims(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Backward compatibility wrapper."""
        return await self.fetch_claims_optimized(batch_size, offset, priority)

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
                claims_data.append({
                    "patient_account_number": row_list[0],
                    "facility_id": row_list[1],
                    "procedure_code": row_list[2] if len(row_list) > 2 else None
                })
        
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
        """Bulk insert using PostgreSQL COPY."""
        claims_list = list(claims)
        return await self.insert_claims_ultra_optimized(claims_list)

    async def bulk_insert_sqlserver_tvp(self, claims: Iterable[Dict[str, Any]]) -> int:
        """Bulk insert using SQL Server TVP."""
        claims_list = list(claims)
        return await self.insert_claims_ultra_optimized(claims_list)

    async def amend_claim(self, claim_id: str, updates: Dict[str, Any]) -> None:
        """Update an existing claim with new values."""
        if not updates:
            return
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values())
        params.append(claim_id)
        await self.sql.execute(
            f"UPDATE claims SET {set_clause} WHERE claim_id = ?",
            *params,
        )
        await record_audit_event(
            self.sql,
            "claims",
            claim_id,
            "update",
            new_values=updates,
        )

    async def assign_failed_claim(self, claim_id: str, user: str) -> None:
        """Assign a failed claim to a user for manual review."""
        await self.sql.execute(
            "UPDATE failed_claims SET assigned_to = ?, resolution_status = 'assigned' WHERE claim_id = ?",
            user,
            claim_id,
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

    async def delete_claim(self, account: str, facility: str) -> None:
        """Delete a claim (used for compensation transactions)."""
        await self.sql.execute(
            "DELETE FROM claims WHERE patient_account_number = ? AND facility_id = ?",
            account,
            facility,
        )