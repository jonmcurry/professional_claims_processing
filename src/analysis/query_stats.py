from __future__ import annotations

from typing import Dict

from ..monitoring.metrics import metrics


def summarize() -> Dict[str, float]:
    data = metrics.as_dict()
    pg_ms = data.get("postgres_query_ms", 0.0)
    pg_count = data.get("postgres_query_count", 0.0)
    sql_ms = data.get("sqlserver_query_ms", 0.0)
    sql_count = data.get("sqlserver_query_count", 0.0)
    summary = {}
    if pg_count:
        summary["postgres_avg_ms"] = pg_ms / pg_count
    if sql_count:
        summary["sqlserver_avg_ms"] = sql_ms / sql_count
    return summary


if __name__ == "__main__":
    print(summarize())
