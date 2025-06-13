from typing import Any, Dict, Iterable, List

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..security.compliance import (
    encrypt_text,
    encrypt_claim_fields,
    decrypt_text,
)
from ..utils.audit import record_audit_event


class ClaimService:
    """Service layer for database interactions related to claims."""

    def __init__(
        self,
        pg: PostgresDatabase,
        sql: SQLServerDatabase,
        encryption_key: str | None = None,
    ) -> None:
        self.pg = pg
        self.sql = sql
        self.encryption_key = encryption_key or ""

    async def fetch_claims(
        self, batch_size: int, offset: int = 0, priority: bool = False
    ) -> List[Dict[str, Any]]:
        """Fetch a batch of claims optionally ordered by priority."""
        order_clause = "ORDER BY priority DESC" if priority else ""
        query = f"SELECT * FROM claims {order_clause} LIMIT $1 OFFSET $2"
        return await self.pg.fetch(query, batch_size, offset)

    async def insert_claims(self, rows: Iterable[Iterable[Any]]) -> None:
        processed = []
        for acct, facility in rows:
            if self.encryption_key:
                acct = encrypt_text(str(acct), self.encryption_key)
            processed.append((acct, facility))
        try:
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

    async def record_checkpoint(self, claim_id: str, stage: str) -> None:
        """Persist a processing checkpoint for a claim."""
        await self.pg.execute(
            "INSERT INTO processing_checkpoints (claim_id, stage) VALUES ($1, $2)",
            claim_id,
            stage,
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
        await record_audit_event(
            self.sql,
            "failed_claims",
            claim.get("claim_id", ""),
            "insert",
            new_values=claim,
            reason=reason,
        )
        await self.enqueue_dead_letter(claim, reason)

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
                claim = eval(claim_str)
                await self.insert_claims(
                    [(claim.get("patient_account_number"), claim.get("facility_id"))]
                )
                await self.pg.execute(
                    "DELETE FROM dead_letter_queue WHERE claim_id = $1",
                    claim_id,
                )
            except Exception:
                continue
