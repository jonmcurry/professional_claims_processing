from typing import Any, Dict, Iterable, List
import json

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..monitoring.metrics import metrics
from ..security.compliance import decrypt_text, encrypt_claim_fields, encrypt_text
from ..utils.audit import record_audit_event
from ..utils.priority_queue import PriorityClaimQueue


class ClaimService:
    """Service layer for database interactions related to claims."""

    def __init__(
        self,
        pg: PostgresDatabase,
        sql: SQLServerDatabase,
        encryption_key: str | None = None,
        checkpoint_buffer_size: int = 100,
    ) -> None:
        self.pg = pg
        self.sql = sql
        self.encryption_key = encryption_key or ""
        self.priority_queue = PriorityClaimQueue()
        self.retry_queue = PriorityClaimQueue()
        self._checkpoint_buffer: list[tuple[str, str]] = []
        self._checkpoint_buffer_size = checkpoint_buffer_size

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
            elif "concurrency" in self.sql.execute_many.__code__.co_varnames:
                await self.sql.execute_many(
                    "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)",
                    processed,
                    concurrency=concurrency,
                )
            else:
                await self.sql.execute_many(
                    "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)",
                    processed,
                )
        except Exception:
            for acct, facility in processed:
                await self.delete_claim(acct, facility)
            raise

    async def delete_claim(self, account: str, facility: str) -> None:
        await self.sql.execute(
            "DELETE FROM claims WHERE patient_account_number = ? AND facility_id = ?",
            account,
            facility,
        )

    async def load_validation_sets(self) -> tuple[set[str], set[str]]:
        """Load valid facility and financial class identifiers."""
        fac_rows = await self.sql.fetch(
            "SELECT DISTINCT facility_id FROM facilities WHERE facility_id IS NOT NULL"
        )
        class_rows = await self.sql.fetch(
            "SELECT DISTINCT financial_class_id FROM facility_financial_classes WHERE financial_class_id IS NOT NULL"
        )
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
            "SELECT procedure_code FROM rvu_data LIMIT $1",
            limit,
        )
        return [str(r.get("procedure_code")) for r in rows if r.get("procedure_code")]

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
        )
        self._checkpoint_buffer.clear()

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
            rows = await self.fetch_claims(batch_size, offset=offset, priority=priority)
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

    async def reprocess_dead_letter(self, batch_size: int = 100) -> None:
        """Attempt to reprocess items from the dead letter queue."""
        rows = await self.pg.fetch(
            "SELECT claim_id, data FROM dead_letter_queue LIMIT $1",
            batch_size,
        )
        for row in rows:
            claim_str = row.get("data", "")
            claim_id = row.get("claim_id")
            if self.encryption_key:
                try:
                    claim_str = decrypt_text(claim_str, self.encryption_key)
                except Exception:
                    continue
            try:
                claim = json.loads(claim_str)
            except Exception:
                continue
            pr = int(claim.get("priority", 0) or 0)
            self.retry_queue.push(claim, pr)
            await self.pg.execute(
                "DELETE FROM dead_letter_queue WHERE claim_id = $1",
                claim_id,
            )

        processed = 0
        while processed < batch_size and len(self.retry_queue) > 0:
            claim = self.retry_queue.pop()
            try:
                from ..processing.repair import ClaimRepairSuggester

                suggester = ClaimRepairSuggester()
                claim = suggester.auto_repair(claim, [])
                await self.insert_claims(
                    [(claim.get("patient_account_number"), claim.get("facility_id"))]
                )
                metrics.inc("failed_claims_resolved")
                processed += 1
            except Exception:
                metrics.inc("failed_claims_retry_failed")
                pr = int(claim.get("priority", 0) or 0)
                self.retry_queue.push(claim, pr)
                break

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
