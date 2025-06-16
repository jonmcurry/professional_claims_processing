from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from ..db.sql_server import SQLServerDatabase


async def failure_reason_counts(db: SQLServerDatabase) -> Dict[str, int]:
    """Return counts of failure reasons."""
    rows = await db.fetch("SELECT failure_reason FROM failed_claims")
    counter: Counter[str] = Counter(row.get("failure_reason") for row in rows)
    return dict(counter)


async def top_failure_reasons(
    db: SQLServerDatabase, limit: int = 5
) -> List[Tuple[str, int]]:
    """Return the most common failure reasons."""
    counts = await failure_reason_counts(db)
    return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]


async def failure_counts_by_category(db: SQLServerDatabase) -> Dict[str, int]:
    """Return counts of failures grouped by category."""
    rows = await db.fetch(
        "SELECT failure_category, COUNT(*) AS total FROM failed_claims GROUP BY failure_category"
    )
    return {row.get("failure_category"): row.get("total", 0) for row in rows}


async def failure_trend(db: SQLServerDatabase, days: int = 7) -> List[Tuple[str, int]]:
    """Return daily failure counts for the last N days."""
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = await db.fetch(
        "SELECT CAST(failed_at AS DATE) AS day, COUNT(*) AS total "
        "FROM failed_claims WHERE failed_at >= ? GROUP BY CAST(failed_at AS DATE) ORDER BY day",
        since,
    )
    return [(row.get("day"), row.get("total", 0)) for row in rows]
