from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from ..config.config import load_config
from ..db.sql_server import SQLServerDatabase
from .status import processing_status
from typing import Optional


def create_app(sql_db: Optional[SQLServerDatabase] = None) -> FastAPI:
    app = FastAPI()
    cfg = load_config()
    sql = sql_db or SQLServerDatabase(cfg.sqlserver)

    @app.on_event("startup")
    async def startup() -> None:
        await sql.connect()

    @app.get("/api/failed_claims")
    async def api_failed_claims():
        rows = await sql.fetch(
            "SELECT TOP 100 * FROM failed_claims ORDER BY failed_at DESC"
        )
        return rows

    @app.get("/failed_claims", response_class=HTMLResponse)
    async def failed_claims_page():
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
    async def status():
        return processing_status

    @app.get("/health")
    async def health():
        ok = await sql.health_check()
        return {"sqlserver": ok}

    return app


app = create_app()
