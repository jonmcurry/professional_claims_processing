from __future__ import annotations

import re
from typing import List

from ..db.postgres import PostgresDatabase
from .query_plan import explain, has_seq_scan
from .query_tracker import summary


async def recommend_indexes(
    db: PostgresDatabase, threshold_ms: float = 100.0
) -> List[str]:
    """Return simple index recommendations for slow queries."""
    suggestions: List[str] = []
    for stmt, avg_ms in summary().items():
        if avg_ms < threshold_ms:
            continue
        plan = await explain(db, stmt)
        if not has_seq_scan(plan):
            continue
        match = re.search(r"WHERE\s+(\w+)", stmt, re.IGNORECASE)
        if match:
            col = match.group(1)
            suggestions.append(f"CREATE INDEX ON claims({col});")
    return suggestions
