from typing import Any, Dict, Iterable, List

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase
from ..security.compliance import encrypt_text
from ..utils.audit import record_audit_event


class ClaimService:
    """Service layer for database interactions related to claims."""

    def __init__(self, pg: PostgresDatabase, sql: SQLServerDatabase, encryption_key: str | None = None) -> None:
        self.pg = pg
        self.sql = sql
        self.encryption_key = encryption_key or ""

    async def fetch_claims(self, batch_size: int) -> List[Dict[str, Any]]:
        return await self.pg.fetch("SELECT * FROM claims LIMIT $1", batch_size)

    async def insert_claims(self, rows: Iterable[Iterable[Any]]) -> None:
        await self.sql.execute_many(
            "INSERT INTO claims (patient_account_number, facility_id) VALUES (?, ?)",
            rows,
        )

    async def record_failed_claim(self, claim: Dict[str, Any], reason: str, suggestions: str) -> None:
        await self.sql.execute(
            "INSERT INTO failed_claims (claim_id, facility_id, patient_account_number, failure_reason, processing_stage, failed_at, original_data, repair_suggestions)"
            " VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)",
            claim.get("claim_id"),
            claim.get("facility_id"),
            claim.get("patient_account_number"),
            reason,
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
