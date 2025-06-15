from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Dict, Tuple

from fastapi import Header, Request

from ..maintenance.enforce_retention_policy import ARCHIVE_PATH, RETENTION_YEARS


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

        return {
            "total_audit_events": count,
            "archive_files": len(files),
            "latest_archive": latest,
            "retention_years": RETENTION_YEARS,
        }

    return router