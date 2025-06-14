import json
from typing import Any, Dict, Iterable, List
import asyncio

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..monitoring.metrics import metrics
from ..security.compliance import decrypt_text, encrypt_claim_fields, encrypt_text
from ..utils.audit import record_audit_event
from ..utils.priority_queue import PriorityClaimQueue


class ClaimService:
    """Service layer for database interactions related to claims with enhanced bulk operations."""

    def __init__(
        self,
        pg: PostgresDatabase,
        sql: SQLServerDatabase,
        encryption_key: str | None = None,
        checkpoint_buffer_size: int = 1000,  # Increased buffer size
    ) -> None:
        self.pg = pg
        self.sql = sql
        self.encryption_key = encryption_key or ""
        self.priority_queue = PriorityClaimQueue()
        self.retry_queue = PriorityClaimQueue()
        self._checkpoint_buffer: list[tuple[str, str]] = []
        self._checkpoint_buffer_size = checkpoint_buffer_size

    async def fetch_claims_optimized(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Optimized claims fetching with reduced memory footprint."""
        # Use COPY for large batches to improve performance
        if batch_size > 5000:
            return await self._fetch_claims_copy(batch_size, offset, priority)
        
        return await self.fetch_claims(batch_size, offset, priority)

    async def _fetch_claims_copy(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Fetch claims using PostgreSQL COPY for better performance with large batches."""
        order_clause = "ORDER BY c.priority DESC" if priority else ""
        
        # Use a simpler query for COPY operations
        query = f"""
        SELECT c.claim_id, c.patient_account_number, c.facility_id, c.procedure_code,
               c.financial_class, c.date_of_birth, c.service_from_date, c.service_to_date,
               c.primary_diagnosis, c.priority, c.units
        FROM claims c
        {order_clause}
        LIMIT {batch_size} OFFSET {offset}
        """
        
        rows = await self.pg.copy_query(query)
        
        # Convert to the expected format
        claims = []
        for row in rows:
            claim = dict(row)
            claim.setdefault("line_items", [])
            
            # Add to priority queue if needed
            if priority:
                pr = int(claim.get("priority", 0) or 0)
                self.priority_queue.push(claim, pr)
            
            claims.append(claim)
        
        return claims

    async def fetch_claims(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of claims with associated line items."""
        order_clause = "ORDER BY c.priority DESC" if priority else ""
        query = (
            "SELECT c.*, li.line_number, li.procedure_code AS li_procedure_code, "
            "li.units AS li_units, li.charge_amount AS li_charge_amount, "
            "li.service_from_date AS li_service_from_date, "
            "li.service_to_date AS li_service_to_date "
            "FROM claims c LEFT JOIN claims_line_items li ON c.claim_id = li.claim_id "
            f"{order_clause} LIMIT $1 OFFSET $2"
        )
        rows = await self.pg.fetch(query, batch_size, offset)
        claims: Dict[str, Dict[str, Any]] = {}
        line_keys = {
            "line_number",
            "li_procedure_code",
            "li_units",
            "li_charge_amount",
            "li_service_from_date",
            "li_service_to_date",
        }
        for row in rows:
            claim_id = row.get("claim_id")
            if claim_id not in claims:
                base = {k: row[k] for k in row.keys() if k not in line_keys}
                base.setdefault("line_items", [])
                claims[claim_id] = base
            if any(k in row for k in line_keys) and row.get("line_number") is not None:
                item = {
                    "line_number": row.get("line_number"),
                    "procedure_code": row.get("li_procedure_code")
                    or row.get("procedure_code"),
                    "units": row.get("li_units"),
                    "charge_amount": row.get("li_charge_amount"),
                    "service_from_date": row.get("li_service_from_date"),
                    "service_to_date": row.get("li_service_to_date"),
                }
                claims[claim_id].setdefault("line_items", []).append(item)

        for claim in claims.values():
            pr = int(claim.get("priority", 0) or 0)
            self.priority_queue.push(claim, pr)

        results: List[Dict[str, Any]] = []
        while len(results) < batch_size and len(self.priority_queue) > 0:
            item = self.priority_queue.pop()
            if item:
                results.append(item)
        return results

    async def insert_claims_bulk_optimized(
        self, rows: Iterable[Iterable[Any]], concurrency: int = 4
    ) -> None:
        """Optimized bulk insert with automatic method selection."""
        rows_list = list(rows)
        if not rows_list:
            return
        
        # Choose optimal insertion method based on batch size
        if len(rows_list) > 10000:
            # Use PostgreSQL COPY for very large batches
            await self._insert_claims_postgres_copy_bulk(rows_list)
        elif len(rows_list) > 1000:
            # Use SQL Server TVP for medium batches
            await self._insert_claims_sql_server_tvp_bulk(rows_list)
        else:
            # Use standard bulk insert for smaller batches
            await self.insert_claims(rows_list, concurrency)

    async def _insert_claims_postgres_copy_bulk(self, rows: List[Iterable[Any]]) -> None:
        """Bulk insert using PostgreSQL COPY for maximum throughput."""
        processed = []
        for acct, facility in rows:
            if self.encryption_key:
                acct = encrypt_text(str(acct), self.encryption_key)
            processed.append((acct, facility))
        
        try:
            await self.pg.copy_records(
                "claims",
                ["patient_account_number", "facility_id"],
                processed,
            )
        except Exception:
            # Compensate by removing any partially inserted records
            for acct, facility in processed:
                await self.delete_claim(acct, facility)
            raise

    async def _insert_claims_sql_server_tvp_bulk(self, rows: List[Iterable[Any]]) -> None:
        """Bulk insert using SQL Server TVP for medium-sized batches."""
        processed = []
        for acct, facility in rows:
            if self.encryption_key:
                acct = encrypt_text(str(acct), self.encryption_key)
            processed.append((acct, facility))
        
        try:
            bulk = getattr(self.sql, "bulk_insert_tvp", None)
            if callable(bulk):
                await bulk(
                    "claims",
                    ["patient_account_number", "facility_id"],
                    processed,
                )
            else:
                # Fallback to execute_many with higher concurrency
                query = "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)"
                await self.sql.execute_many(query, processed, concurrency=8)
        except Exception:
            # Compensate by removing any partially inserted records
            for acct, facility in processed:
                await self.delete_claim(acct, facility)
            raise

    async def insert_claims(
        self, rows: Iterable[Iterable[Any]], concurrency: int = 1
    ) -> None:
        processed = []
        for acct, facility in rows:
            if self.encryption_key:
                acct = encrypt_text(str(acct), self.encryption_key)
            processed.append((acct, facility))
        try:
            bulk = getattr(self.sql, "bulk_insert_tvp", None)
            if callable(bulk):
                await bulk(
                    "claims",
                    ["patient_account_number", "facility_id"],
                    processed,
                )
            else:
                claims_data = [
                    {
                        "patient_account_number": acct,
                        "facility_id": facility,
                    }
                    for acct, facility in processed
                ]
                await self.bulk_insert_postgres_copy(claims_data)
        except Exception:
            for acct, facility in processed:
                await self.delete_claim(acct, facility)
            raise

    async def bulk_insert_postgres_copy(self, claims: Iterable[Dict[str, Any]]) -> int:
        """Bulk insert claims into PostgreSQL using COPY."""
        rows: list[tuple[Any, Any, Any]] = []
        for claim in claims:
            acct = claim.get("patient_account_number")
            if self.encryption_key and acct is not None:
                acct = encrypt_text(str(acct), self.encryption_key)
            rows.append(
                (
                    acct,
                    claim.get("facility_id"),
                    claim.get("procedure_code"),
                )
            )
        return await self.pg.copy_records(
            "claims",
            ["patient_account_number", "facility_id", "procedure_code"],
            rows,
        )

    async def bulk_insert_sqlserver_tvp(
        self, claims: Iterable[Dict[str, Any]]
    ) -> int:
        """Bulk insert claims into SQL Server using TVP."""
        tvp_data: list[tuple[Any, Any, Any]] = []
        for claim in claims:
            acct = claim.get("patient_account_number")
            if self.encryption_key and acct is not None:
                acct = encrypt_text(str(acct), self.encryption_key)
            tvp_data.append(
                (
                    acct,
                    claim.get("facility_id"),
                    claim.get("procedure_code"),
                )
            )

        bulk = getattr(self.sql, "bulk_insert_tvp", None)
        if not callable(bulk):
            raise AttributeError("SQLServer bulk_insert_tvp not available")
        return await bulk(
            "claims",
            ["patient_account_number", "facility_id", "procedure_code"],
            tvp_data,
        )

    async def delete_claim(self, account: str, facility: str) -> None:
        await self.sql.execute(
            "DELETE FROM claims WHERE patient_account_number = ? AND facility_id = ?",
            account,
            facility,
        )

    async def load_validation_sets_cached(self) -> tuple[set[str], set[str]]:
        """Load validation sets with intelligent caching."""
        # Check if we have cached data that's still fresh
        if hasattr(self, '_validation_cache_time'):
            import time
            if time.time() - self._validation_cache_time < 300:  # 5 minute cache
                return self._facilities_cache, self._classes_cache
        
        facilities, classes = await self.load_validation_sets()
        
        # Cache the results
        self._facilities_cache = facilities
        self._classes_cache = classes
        import time
        self._validation_cache_time = time.time()
        
        return facilities, classes

    async def load_validation_sets(self) -> tuple[set[str], set[str]]:
        """Load valid facility and financial class identifiers."""
        # Use parallel queries for better performance
        fac_task = self.sql.fetch(
            "SELECT DISTINCT facility_id FROM facilities WHERE facility_id IS NOT NULL"
        )
        class_task = self.sql.fetch(
            "SELECT DISTINCT financial_class_id FROM facility_financial_classes WHERE financial_class_id IS NOT NULL"
        )
        
        fac_rows, class_rows = await asyncio.gather(fac_task, class_task)
        
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
        return facilities, classes

    async def top_rvu_codes(self, limit: int = 1000) -> list[str]:
        """Return procedure codes for the top RVU records."""
        rows = await self.pg.fetch(
            "SELECT procedure_code FROM rvu_data ORDER BY total_rvu DESC LIMIT $1",
            limit,
        )
        return [str(r.get("procedure_code")) for r in rows if r.get("procedure_code")]

    async def record_checkpoint_batch(self, checkpoints: List[tuple[str, str]]) -> None:
        """Record multiple checkpoints in a single operation."""
        if not checkpoints:
            return
        
        await self.pg.execute_many(
            "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)",
            checkpoints,
            concurrency=4
        )

    async def record_checkpoint(
        self, claim_id: str, stage: str, *, buffered: bool = True
    ) -> None:
        """Persist a processing checkpoint for a claim."""
        if buffered:
            self._checkpoint_buffer.append((claim_id, stage))
            if len(self._checkpoint_buffer) >= self._checkpoint_buffer_size:
                await self.flush_checkpoints()
            return
        await self.pg.execute(
            "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)",
            claim_id,
            stage,
        )

    async def flush_checkpoints(self) -> None:
        if not self._checkpoint_buffer:
            return
        await self.pg.execute_many(
            "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)",
            self._checkpoint_buffer,
            concurrency=4
        )
        self._checkpoint_buffer.clear()

    async def record_failed_claims_batch(self, failed_claims_data: List[tuple]) -> None:
        """Record multiple failed claims in a single operation."""
        if not failed_claims_data:
            return
        
        await self.sql.execute_many(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, failure_category, processing_stage, failed_at, original_data, repair_suggestions)"
            " VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)",
            failed_claims_data,
            concurrency=4
        )

    async def record_failed_claim(
        self,
        claim: Dict[str, Any],
        reason: str,
        suggestions: str,
        category: str = "processing",
    ) -> None:
        encrypted_claim = encrypt_claim_fields(claim, self.encryption_key)
        await self.sql.execute(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, failure_category, processing_stage, failed_at, original_data, repair_suggestions)"
            " VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)",
            encrypted_claim.get("claim_id"),
            encrypted_claim.get("facility_id"),
            encrypted_claim.get("patient_account_number"),
            reason,
            category,
            "validation",
            (
                encrypt_text(str(claim), self.encryption_key)
                if self.encryption_key
                else str(claim)
            ),
            suggestions,
        )
        metrics.inc(f"errors_{category}")
        await record_audit_event(
            self.sql,
            "failed_claims",
            claim.get("claim_id", ""),
            "insert",
            new_values=claim,
            reason=reason,
        )
        await self.enqueue_dead_letter(claim, reason)
        pr = int(claim.get("priority", 0) or 0)
        self.retry_queue.push(claim, pr)

    async def stream_claims(
        self, batch_size: int, priority: bool = False
    ) -> Iterable[Dict[str, Any]]:
        """Yield claims in batches for stream processing."""
        offset = 0
        while True:
            rows = await self.fetch_claims_optimized(batch_size, offset=offset, priority=priority)
            if not rows:
                break
            for row in rows:
                yield row
            offset += batch_size

    async def enqueue_dead_letter(self, claim: Dict[str, Any], reason: str) -> None:
        """Store failed claims for future reprocessing."""
        data = (
            encrypt_text(str(claim), self.encryption_key)
            if self.encryption_key
            else str(claim)
        )
        await self.pg.execute(
            "INSERT INTO dead_letter_queue (claim_id, reason, data) VALUES ($1, $2, $3)",
            claim.get("claim_id"),
            reason,
            data,
        )

    async def reprocess_dead_letter(self, batch_size: int = 500) -> None:  # Increased batch size
        """Attempt to reprocess items from the dead letter queue."""
        rows = await self.pg.fetch(
            "SELECT claim_id, data FROM dead_letter_queue LIMIT $1",
            batch_size,
        )
        
        if not rows:
            return
        
        # Process in batches for better performance
        claims_to_retry = []
        claims_to_delete = []
        
        for row in rows:
            claim_str = row.get("data", "")
            claim_id = row.get("claim_id")
            
            if self.encryption_key:
                try:
                    claim_str = decrypt_text(claim_str, self.encryption_key)
                except Exception:
                    claims_to_delete.append(claim_id)
                    continue
            
            try:
                claim = json.loads(claim_str)
                claims_to_retry.append(claim)
                claims_to_delete.append(claim_id)
            except Exception:
                claims_to_delete.append(claim_id)
                continue
        
        # Bulk delete processed items from dead letter queue
        if claims_to_delete:
            delete_params = [(claim_id,) for claim_id in claims_to_delete]
            await self.pg.execute_many(
                "DELETE FROM dead_letter_queue WHERE claim_id = $1",
                delete_params,
                concurrency=2
            )
        
        # Add claims to retry queue
        for claim in claims_to_retry:
            pr = int(claim.get("priority", 0) or 0)
            self.retry_queue.push(claim, pr)

        # Process retry queue in batches
        processed = 0
        retry_batch = []
        
        while processed < batch_size and len(self.retry_queue) > 0:
            claim = self.retry_queue.pop()
            if claim:
                retry_batch.append(claim)
                processed += 1
                
                # Process in smaller batches
                if len(retry_batch) >= 50:
                    await self._process_retry_batch(retry_batch)
                    retry_batch = []
        
        # Process remaining claims in batch
        if retry_batch:
            await self._process_retry_batch(retry_batch)

    async def _process_retry_batch(self, claims: List[Dict[str, Any]]) -> None:
        """Process a batch of retry claims."""
        from ..processing.repair import ClaimRepairSuggester
        
        suggester = ClaimRepairSuggester()
        successful_claims = []
        failed_claims = []
        
        for claim in claims:
            try:
                # Attempt automatic repair
                repaired_claim = suggester.auto_repair(claim, [])
                successful_claims.append((
                    repaired_claim.get("patient_account_number"),
                    repaired_claim.get("facility_id")
                ))
            except Exception:
                pr = int(claim.get("priority", 0) or 0)
                self.retry_queue.push(claim, pr)
                failed_claims.append(claim)
        
        # Bulk insert successful claims
        if successful_claims:
            try:
                await self.insert_claims_bulk_optimized(successful_claims)
                metrics.inc("failed_claims_resolved", len(successful_claims))
            except Exception:
                metrics.inc("failed_claims_retry_failed", len(successful_claims))
                # Re-add to retry queue
                for i, (acct, facility) in enumerate(successful_claims):
                    if i < len(claims):
                        pr = int(claims[i].get("priority", 0) or 0)
                        self.retry_queue.push(claims[i], pr)
        
        metrics.inc("failed_claims_retry_failed", len(failed_claims))

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

    def get_service_stats(self) -> Dict[str, Any]:
        """Get service performance statistics."""
        return {
            "checkpoint_buffer_size": len(self._checkpoint_buffer),
            "checkpoint_buffer_max": self._checkpoint_buffer_size,
            "priority_queue_size": len(self.priority_queue),
            "retry_queue_size": len(self.retry_queue),
            "has_validation_cache": hasattr(self, '_validation_cache_time')
        }