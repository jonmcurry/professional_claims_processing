from __future__ import annotations

from typing import Any

from ..db.postgres import PostgresDatabase


async def explain(db: PostgresDatabase, query: str, *params: Any) -> str:
    """Return the query plan for the given statement."""
    rows = await db.fetch("EXPLAIN " + query, *params)
    if rows and isinstance(rows[0], dict) and "QUERY PLAN" in rows[0]:
        return "\n".join(r["QUERY PLAN"] for r in rows)
    return str(rows)


def has_seq_scan(plan: str) -> bool:
    """Simple heuristic to detect sequential scans."""
    return "Seq Scan" in plan
