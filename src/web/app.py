from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import Response
from ..config.config import load_config
from ..db.sql_server import SQLServerDatabase
from .status import processing_status, batch_status
from typing import Optional, Any
from .rate_limit import RateLimiter
from ..utils.tracing import (
    start_trace,
    start_trace_from_traceparent,
)
from ..monitoring.metrics import metrics
import json
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
from fastapi.responses import JSONResponse
import logging
import re
from ..monitoring.profiling import start_profiling, stop_profiling


def create_app(
    sql_db: Optional[SQLServerDatabase] = None,
    pg_db: Optional["PostgresDatabase"] = None,
    api_key: str | None = None,
    rate_limit_per_sec: int = 100,
) -> FastAPI:
    app = FastAPI()
    cfg = load_config()
    sql = sql_db or SQLServerDatabase(cfg.sqlserver)
    if pg_db is None:
        from ..db.postgres import PostgresDatabase

        pg = PostgresDatabase(cfg.postgres)
    else:
        pg = pg_db
    required_key = api_key or cfg.security.api_key
    limiter = RateLimiter(rate_limit_per_sec)
    logger = logging.getLogger("claims_processor")

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception", exc_info=exc)
        return JSONResponse(
            status_code=500, content={"detail": "Internal Server Error"}
        )

    def _check_role(required: str, role: str | None) -> None:
        if role is None:
            return
        current = role or "user"
        if required and current != required:
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

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Content-Security-Policy", "default-src 'self'")
        return response

    def _check_key(x_api_key: str) -> None:
        if required_key and x_api_key != required_key:
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.on_event("startup")
    async def startup() -> None:
        await sql.connect()
        await pg.connect()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await sql.close()
        await pg.close()

    @app.get("/api/failed_claims")
    async def api_failed_claims(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
        rows = await sql.fetch(
            "SELECT TOP 100 * FROM failed_claims ORDER BY failed_at DESC"
        )
        return rows

    @app.get("/failed_claims", response_class=HTMLResponse)
    async def failed_claims_page(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
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
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
        return processing_status

    @app.get("/batch_status")
    async def get_batch_status(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
        return batch_status

    @app.get("/health")
    async def health(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
        ok = await sql.health_check()
        return {"sqlserver": ok}

    @app.get("/readiness")
    async def readiness(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("user", x_user_role)
        pg_ok = await pg.health_check()
        sql_ok = await sql.health_check()
        return {"postgres": pg_ok, "sqlserver": sql_ok}

    @app.get("/liveness")
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def get_metrics(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("admin", x_user_role)
        return metrics.as_dict()

    @app.get("/profiling/start")
    async def profiling_start(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("admin", x_user_role)
        start_profiling()
        return {"profiling": "started"}

    @app.get("/profiling/stop")
    async def profiling_stop(
        x_api_key: str = Header(...), x_user_role: str | None = Header(None)
    ):
        _check_key(x_api_key)
        _check_role("admin", x_user_role)
        stats = stop_profiling()
        return {"profiling": "stopped", "stats": stats}

    return app


try:
    app = create_app()
except Exception:
    # Fallback for test environments without optional dependencies
    app = FastAPI()
