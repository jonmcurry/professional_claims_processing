from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from ..config.config import load_config
from ..db.sql_server import SQLServerDatabase
from .status import processing_status
from typing import Optional


def create_app(sql_db: Optional[SQLServerDatabase] = None, api_key: str | None = None) -> FastAPI:
    app = FastAPI()
    cfg = load_config()
    sql = sql_db or SQLServerDatabase(cfg.sqlserver)
    required_key = api_key or cfg.security.api_key

    def _check_key(x_api_key: str) -> None:
        if required_key and x_api_key != required_key:
            raise HTTPException(status_code=401, detail="Invalid API key")

    @app.on_event("startup")
    async def startup() -> None:
        await sql.connect()

    @app.get("/api/failed_claims")
    async def api_failed_claims(x_api_key: str = Header(...)):
        _check_key(x_api_key)
        rows = await sql.fetch(
            "SELECT TOP 100 * FROM failed_claims ORDER BY failed_at DESC"
        )
        return rows

    @app.get("/failed_claims", response_class=HTMLResponse)
    async def failed_claims_page(x_api_key: str = Header(...)):
        _check_key(x_api_key)
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
    async def status(x_api_key: str = Header(...)):
        _check_key(x_api_key)
        return processing_status

    @app.get("/health")
    async def health(x_api_key: str = Header(...)):
        _check_key(x_api_key)
        ok = await sql.health_check()
        return {"sqlserver": ok}

    return app


app = create_app()
