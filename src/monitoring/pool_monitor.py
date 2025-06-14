from __future__ import annotations

import asyncio
from typing import Optional

from ..db.postgres import PostgresDatabase
from ..db.sql_server import SQLServerDatabase

_pg: Optional[PostgresDatabase] = None
_sql: Optional[SQLServerDatabase] = None
_task: Optional[asyncio.Task] = None


async def _collect(interval: float) -> None:
    while True:
        if _pg:
            _pg.report_pool_status()
        if _sql:
            _sql.report_pool_status()
        await asyncio.sleep(interval)


def start(
    pg: PostgresDatabase | None,
    sql: SQLServerDatabase | None,
    interval: float = 5.0,
) -> None:
    """Begin periodically collecting pool metrics."""
    global _task, _pg, _sql
    if _task:
        return
    _pg = pg
    _sql = sql
    loop = asyncio.get_event_loop()
    _task = loop.create_task(_collect(interval))


def stop() -> None:
    """Stop collecting pool metrics."""
    global _task
    if _task:
        _task.cancel()
        _task = None
