import json
from typing import Any, Optional

from ..db.sql_server import SQLServerDatabase


async def record_audit_event(
    db: SQLServerDatabase,
    table_name: str,
    record_id: str,
    operation: str,
    user_id: Optional[str] = None,
    old_values: Optional[dict[str, Any]] = None,
    new_values: Optional[dict[str, Any]] = None,
    reason: Optional[str] = None,
) -> None:
    """Persist an audit trail entry to the audit_log table."""
    await db.execute(
        """
        INSERT INTO audit_log (
            table_name, record_id, operation, user_id,
            old_values, new_values, operation_timestamp, reason
        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), ?)
        """,
        table_name,
        record_id,
        operation,
        user_id,
        json.dumps(old_values) if old_values else None,
        json.dumps(new_values) if new_values else None,
        reason,
    )
