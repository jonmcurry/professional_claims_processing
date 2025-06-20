import json
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import Response

from ..config.config import AppConfig, create_default_config, load_config
from ..db.sql_server import SQLServerDatabase
from ..monitoring.metrics import metrics
from ..utils.cache import DistributedCache
from ..utils.tracing import start_trace, start_trace_from_traceparent
from .compliance import create_compliance_router
from .rate_limit import RateLimiter
from .status import batch_status, processing_status

try:
    from starlette.middleware.base import BaseHTTPMiddleware
except Exception:  # pragma: no cover - allow running without starlette

    class BaseHTTPMiddleware:
        def __init__(self, app=None, dispatch=None, **_: Any):
            self.app = app
            if dispatch is not None:
                self.dispatch = dispatch

        async def __call__(self, scope, receive, send):
            await self.dispatch(scope, receive, send)


import logging
import re
import time

from fastapi.responses import JSONResponse

from ..monitoring.profiling import start_profiling, stop_profiling
from ..utils.audit import record_audit_event
from ..utils.logging import RequestContextFilter


def create_app(
    sql_db: Optional[SQLServerDatabase] = None,
    pg_db: Optional["PostgresDatabase"] = None,
    redis_cache: Optional["DistributedCache"] = None,
    external_service: Optional[Any] = None,
    cfg: Optional["AppConfig"] = None,
    api_key: str | None = None,
    rate_limit_per_sec: int = 100,
) -> FastAPI:
    """Create and configure the FastAPI application used for monitoring.

    Parameters
    ----------
    sql_db : Optional[SQLServerDatabase]
        Pre-configured SQL Server connection. If ``None`` a new connection is
        created using settings from ``config.yaml``.
    pg_db : Optional["PostgresDatabase"]
        Optional PostgreSQL connection for health checks.
    redis_cache : Optional["DistributedCache"]
        Optional Redis cache instance for health checks.
    external_service : Optional[Any]
        Any additional external service with a ``health_check`` coroutine.
    cfg : Optional["AppConfig"]
        Pre-loaded configuration to avoid reading from disk.
    api_key : str | None
        API key required for all requests when provided.
    rate_limit_per_sec : int
        Requests per second allowed from a single client.

    Returns
    -------
    FastAPI
        Configured application instance ready to run.
    """
    app = FastAPI()
    if cfg is None:
        try:
            cfg = load_config()
        except Exception:
            cfg = create_default_config()
    sql = sql_db or SQLServerDatabase(cfg.sqlserver)
    if pg_db is None:
        from ..db.postgres import PostgresDatabase

        pg = PostgresDatabase(cfg.postgres)
    else:
        pg = pg_db
    required_key = api_key or cfg.security.api_key
    limiter = RateLimiter(rate_limit_per_sec)
    redis = redis_cache
    external = external_service
    logger = logging.getLogger("claims_processor")

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception", exc_info=exc)
        return JSONResponse(
            status_code=500, content={"detail": "Internal Server Error"}
        )

    # Map of endpoint paths to allowed roles. Endpoints not listed are open to
    # all roles.
    role_permissions: dict[str, set[str]] = {
        "/api/failed_claims": {"auditor", "user", "admin"},
        "/failed_claims": {"auditor", "user", "admin"},
        "/status": {"auditor", "user", "admin"},
        "/batch_status": {"auditor", "user", "admin"},
        "/health": {"auditor", "user", "admin"},
        "/readiness": {"auditor", "user", "admin"},
        "/compliance/dashboard": {"auditor", "user", "admin"},
        "/api/assign_failed_claim": {"user", "admin"},
        "/api/resolve_failed_claim": {"user", "admin"},
        "/metrics": {"admin"},
        "/profiling/start": {"admin"},
        "/profiling/stop": {"admin"},
    }

    async def _check_role(request: Request, path: str, role: str | None) -> None:
        """Validate that the provided role can access the given path."""
        if role is None:
            # When no role header is provided, allow access for backward
            # compatibility with internal tools.
            return
        allowed = role_permissions.get(path, {"auditor", "user", "admin"})
        current = role or "user"
        if current not in allowed:
            ip = getattr(
                getattr(request, "client", None), "host", None
            ) or request.headers.get("X-Forwarded-For", "unknown")
            await record_audit_event(
                sql,
                "auth",
                ip,
                "unauthorized",
                new_values={
                    "path": path,
                    "role": role,
                    "headers": dict(request.headers),
                },
            )
            raise HTTPException(status_code=403, detail="Forbidden")

    @app.middleware("http")
    async def trace_middleware(request: Request, call_next):
        traceparent = request.headers.get("traceparent")
        if traceparent:
            trace_id = start_trace_from_traceparent(traceparent)
        else:
            trace_id = start_trace(request.headers.get("X-Request-ID"))
        if not await limiter.allow():
            return Response(status_code=429, content="Too Many Requests")
        response = await call_next(request)
        response.headers["X-Trace-ID"] = trace_id
        return response

    class SanitizeMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            if request.headers.get("content-type", "").startswith("application/json"):
                body = await request.json()

                def _sanitize(value: Any) -> Any:
                    if isinstance(value, str):
                        return re.sub(r"[<>]", "", value)
                    if isinstance(value, dict):
                        return {k: _sanitize(v) for k, v in value.items()}
                    if isinstance(value, list):
                        return [_sanitize(v) for v in value]
                    return value

                sanitized = _sanitize(body)
                request._body = bytes(json.dumps(sanitized), "utf-8")  # type: ignore[attr-defined]
            response = await call_next(request)
            return response

    app.add_middleware(SanitizeMiddleware)

    class RequestLoggingMiddleware(BaseHTTPMiddleware):
        """Log incoming requests and responses with context."""

        def __init__(self, app: FastAPI) -> None:  # type: ignore[override]
            super().__init__(app)
            self.logger = logging.getLogger("claims_processor")
            if not any(
                isinstance(f, RequestContextFilter) for f in self.logger.filters
            ):
                self.logger.addFilter(RequestContextFilter())

        async def dispatch(self, request: Request, call_next):
            start = time.perf_counter()
            path = getattr(getattr(request, "url", None), "path", "unknown")
            method = getattr(request, "method", "GET")
            self.logger.info("request", extra={"path": path, "method": method})
            response = await call_next(request)
            latency_ms = (time.perf_counter() - start) * 1000
            self.logger.info(
                "response",
                extra={
                    "path": path,
                    "method": method,
                    "status": response.status_code,
                    "latency_ms": latency_ms,
                },
            )
            return response

    app.add_middleware(RequestLoggingMiddleware)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Content-Security-Policy", "default-src 'self'")
        return response

    async def _check_key(request: Request, x_api_key: str) -> None:
        if required_key and x_api_key != required_key:
            ip = getattr(
                getattr(request, "client", None), "host", None
            ) or request.headers.get("X-Forwarded-For", "unknown")
            await record_audit_event(
                sql,
                "auth",
                ip,
                "invalid_api_key",
                new_values={"path": request.url.path, "headers": dict(request.headers)},
            )
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.on_event("startup")
    async def startup() -> None:
        await sql.connect()
        await pg.connect()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await sql.close()
        await pg.close()

    compliance_router = create_compliance_router(sql, _check_key, _check_role)
    for (method, path), handler in compliance_router.routes.items():
        if method == "GET":
            app.get(path)(handler)

    @app.get("/api/failed_claims")
    async def api_failed_claims(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        rows = await sql.fetch(
            "SELECT TOP 100 * FROM failed_claims ORDER BY failed_at DESC"
        )
        return rows

    @app.post("/api/assign_failed_claim")
    async def assign_failed_claim(
        claim_id: str,
        user: str,
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        if not claim_id or not user:
            raise HTTPException(status_code=400, detail="claim_id and user required")
        await sql.execute(
            "UPDATE failed_claims SET assigned_to = ?, resolution_status = 'assigned' WHERE claim_id = ?",
            user,
            claim_id,
        )
        await record_audit_event(
            sql,
            "failed_claims",
            claim_id,
            "assign",
            new_values={"user": user},
        )
        return {"status": "assigned"}

    @app.post("/api/resolve_failed_claim")
    async def resolve_failed_claim(
        claim_id: str,
        action: str,
        notes: str = "",
        request: Request | None = None,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        request = request or Request({})
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        if not claim_id or not action:
            raise HTTPException(status_code=400, detail="claim_id and action required")
        await sql.execute(
            "UPDATE failed_claims SET resolution_status = 'resolved', resolved_at = GETDATE(), resolution_action = ?, resolution_notes = ? WHERE claim_id = ?",
            action,
            notes,
            claim_id,
        )
        metrics.inc("failed_claims_manual")
        await record_audit_event(
            sql,
            "failed_claims",
            claim_id,
            "resolve",
            new_values={"action": action, "notes": notes},
        )
        return {"status": "resolved"}

    @app.get("/failed_claims", response_class=HTMLResponse)
    async def failed_claims_page(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        rows = await sql.fetch(
            "SELECT TOP 100 * FROM failed_claims ORDER BY failed_at DESC"
        )
        html_rows = "".join(
            f"<tr><td>{r.get('claim_id')}</td><td>{r.get('failure_reason')}</td><td>{r.get('failed_at')}</td></tr>"
            for r in rows
        )
        page = f"""
        <html>
        <head><title>Failed Claims</title></head>
        <body>
        <h1>Failed Claims</h1>
        <table>
        <tr><th>Claim ID</th><th>Reason</th><th>Failed At</th></tr>
        {html_rows}
        </table>
        </body>
        </html>
        """
        return HTMLResponse(content=page)

    @app.get("/status")
    async def status(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        from .status import sync_status

        return {"processing": processing_status, "sync": sync_status}

    @app.get("/batch_status")
    async def get_batch_status(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        return batch_status

    @app.get("/health")
    async def health(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        status = {"sqlserver": await sql.health_check()}
        if redis:
            status["redis"] = await redis.health_check()
        if external and hasattr(external, "health_check"):
            status["external"] = await external.health_check()
        return status

    @app.get("/readiness")
    async def readiness(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        status = {
            "postgres": await pg.health_check(),
            "sqlserver": await sql.health_check(),
        }
        if redis:
            status["redis"] = await redis.health_check()
        if external and hasattr(external, "health_check"):
            status["external"] = await external.health_check()
        return status

    @app.get("/liveness")
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def get_metrics(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        return metrics.as_dict()

    @app.get("/profiling/start")
    async def profiling_start(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        start_profiling()
        return {"profiling": "started"}

    @app.get("/profiling/stop")
    async def profiling_stop(
        request: Request,
        x_api_key: str = Header(...),
        x_user_role: str | None = Header(None),
    ):
        role = x_user_role or request.headers.get("X-User-Role")
        await _check_key(request, x_api_key)
        await _check_role(request, request.url.path, role)
        stats = stop_profiling()
        return {"profiling": "stopped", "stats": stats}

    return app


try:
    app = create_app()
except Exception:
    # Fallback for test environments without optional dependencies
    app = FastAPI()
