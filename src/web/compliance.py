from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Tuple

from fastapi import Header, Request

from ..maintenance.enforce_retention_policy import ARCHIVE_PATH, RETENTION_YEARS
from ..analysis.failure_patterns import top_failure_reasons
from ..analysis.revenue import (
    calculate_potential_revenue_loss,
    revenue_loss_by_category,
)


class SimpleRouter:
    def __init__(self) -> None:
        self.routes: Dict[Tuple[str, str], Callable[..., Any]] = {}

    def get(self, path: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.routes[("GET", path)] = func
            return func

        return decorator


def create_compliance_router(
    db: Any,
    check_key: Callable[[Request, str], Any],
    check_role: Callable[[Request, str, str | None], Any],
    archive_path: str | None = None,
) -> SimpleRouter:
    """Return a router exposing compliance dashboards."""

    path = archive_path or os.getenv("ARCHIVE_PATH", str(ARCHIVE_PATH))
    router = SimpleRouter()

    @router.get("/compliance/dashboard")
    async def dashboard(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ) -> dict[str, Any]:
        role = x_user_role or request.headers.get("X-User-Role")
        await check_key(request, x_api_key)
        await check_role(request, request.url.path, role)
        rows = await db.fetch("SELECT COUNT(*) AS count FROM audit_log")
        count = rows[0]["count"] if rows else 0

        files: list[str] = []
        latest: str | None = None
        if os.path.isdir(path):
            files = [f for f in os.listdir(path) if f.startswith("claims_") and f.endswith(".jsonl")]
            if files:
                try:
                    latest_ts = max(
                        datetime.strptime(f.split("_")[1].split(".")[0], "%Y_%m_%d")
                        for f in files
                    )
                    latest = latest_ts.strftime("%Y-%m-%d")
                except Exception:
                    latest = None

        failure_patterns = await top_failure_reasons(db, limit=5)

        since = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
        trend_rows = await db.fetch(
            "SELECT summary_date, total_claims_processed, total_claims_failed "
            "FROM daily_processing_summary WHERE summary_date >= ? ORDER BY summary_date",
            since,
        )
        processing_trends = [
            {
                "date": row.get("summary_date"),
                "processed": row.get("total_claims_processed", 0),
                "failed": row.get("total_claims_failed", 0),
            }
            for row in trend_rows
        ]

        total_revenue = await calculate_potential_revenue_loss(db)
        revenue_by_category = await revenue_loss_by_category(db)

        return {
            "total_audit_events": count,
            "archive_files": len(files),
            "latest_archive": latest,
            "retention_years": RETENTION_YEARS,
            "failure_patterns": failure_patterns,
            "processing_trends": processing_trends,
            "revenue_impact": {
                "total": total_revenue,
                "by_category": revenue_by_category,
            },
        }

    return router