from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Tuple

_query_times: Dict[str, List[float]] = defaultdict(list)


def record(statement: str, duration_ms: float) -> None:
    """Record execution time for a SQL statement."""
    key = statement.split()[0].lower() if statement else "unknown"
    _query_times[key].append(duration_ms)


def summary() -> Dict[str, float]:
    """Return average time per statement type."""
    return {k: sum(v) / len(v) for k, v in _query_times.items() if v}


def top_slowest(n: int = 5) -> List[Tuple[str, float]]:
    """Return the slowest statements by average time."""
    stats = summary()
    return sorted(stats.items(), key=lambda x: x[1], reverse=True)[:n]


def reset() -> None:
    _query_times.clear()
