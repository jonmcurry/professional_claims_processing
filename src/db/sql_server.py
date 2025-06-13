import asyncio
import pyodbc
from typing import Iterable, Any

from .base import BaseDatabase
from ..config.config import SQLServerConfig


class SQLServerDatabase(BaseDatabase):
    def __init__(self, cfg: SQLServerConfig):
        self.cfg = cfg
        self.pool: list[pyodbc.Connection] = []
        self._lock = asyncio.Lock()

    async def connect(self, size: int = 5) -> None:
        for _ in range(size):
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.cfg.host},{self.cfg.port};"
                f"DATABASE={self.cfg.database};UID={self.cfg.user};PWD={self.cfg.password}",
                autocommit=False,
            )
            self.pool.append(conn)

    async def _acquire(self) -> pyodbc.Connection:
        async with self._lock:
            return self.pool.pop() if self.pool else None

    async def _release(self, conn: pyodbc.Connection) -> None:
        async with self._lock:
            self.pool.append(conn)

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        conn = await self._acquire()
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        await self._release(conn)
        return rows

    async def execute(self, query: str, *params: Any) -> int:
        conn = await self._acquire()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        rowcount = cursor.rowcount
        await self._release(conn)
        return rowcount

    async def execute_many(self, query: str, params_seq: Iterable[Iterable[Any]]) -> int:
        conn = await self._acquire()
        cursor = conn.cursor()
        cursor.executemany(query, list(params_seq))
        conn.commit()
        rowcount = cursor.rowcount
        await self._release(conn)
        return rowcount

