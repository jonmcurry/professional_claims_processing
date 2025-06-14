from __future__ import annotations

from collections import Counter
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
