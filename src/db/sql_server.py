import asyncio
import pyodbc
from typing import Iterable, Any

from ..utils.errors import DatabaseConnectionError, QueryError

from .base import BaseDatabase
from ..config.config import SQLServerConfig


class SQLServerDatabase(BaseDatabase):
    def __init__(self, cfg: SQLServerConfig):
        self.cfg = cfg
        self.pool: list[pyodbc.Connection] = []
        self._lock = asyncio.Lock()

    async def connect(self, size: int = 5) -> None:
        try:
            for _ in range(size):
                conn = self._create_connection()
                self.pool.append(conn)
        except Exception as e:
            raise DatabaseConnectionError(str(e)) from e

    def _create_connection(self) -> pyodbc.Connection:
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.cfg.host},{self.cfg.port};"
            f"DATABASE={self.cfg.database};UID={self.cfg.user};PWD={self.cfg.password}",
            autocommit=False,
        )

    async def _acquire(self) -> pyodbc.Connection:
        async with self._lock:
            if self.pool:
                return self.pool.pop()
            return self._create_connection()

    async def _release(self, conn: pyodbc.Connection) -> None:
        async with self._lock:
            self.pool.append(conn)

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        conn = await self._acquire()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return rows
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return rows
        except Exception as e:
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute(self, query: str, *params: Any) -> int:
        conn = await self._acquire()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute_many(self, query: str, params_seq: Iterable[Iterable[Any]]) -> int:
        conn = await self._acquire()
        params_list = list(params_seq)
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def health_check(self) -> bool:
        try:
            await self.execute("SELECT 1")
            return True
        except DatabaseError:
            return False

