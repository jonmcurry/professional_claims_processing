from __future__ import annotations

from typing import Dict

from ..db.sql_server import SQLServerDatabase


async def calculate_potential_revenue_loss(db: SQLServerDatabase) -> float:
    """Return the total potential revenue loss from failed claims."""
    rows = await db.fetch(
        "SELECT SUM(potential_revenue_loss) AS total FROM failed_claims"
    )
    if rows and isinstance(rows[0], dict):
        return float(rows[0].get("total") or 0.0)
    return 0.0


async def revenue_loss_by_category(db: SQLServerDatabase) -> Dict[str, float]:
    """Return revenue loss aggregated by failure category."""
    rows = await db.fetch(
        "SELECT failure_category, SUM(potential_revenue_loss) AS total "
        "FROM failed_claims GROUP BY failure_category"
    )
    result: Dict[str, float] = {}
    for row in rows:
        category = row.get("failure_category") or "unknown"
        result[category] = float(row.get("total") or 0.0)
    return result
