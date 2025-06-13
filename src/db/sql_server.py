import asyncio
import time
import pyodbc
from typing import Iterable, Any

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from ..utils.errors import DatabaseConnectionError, QueryError, CircuitBreakerOpenError

from .base import BaseDatabase
from ..config.config import SQLServerConfig
from ..monitoring.metrics import metrics


class SQLServerDatabase(BaseDatabase):
    def __init__(self, cfg: SQLServerConfig):
        self.cfg = cfg
        self.pool: list[pyodbc.Connection] = []
        self._lock = asyncio.Lock()
        self._prepared: dict[str, str] = {}
        self.circuit_breaker = CircuitBreaker()

    async def connect(self, size: int = 5) -> None:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        try:
            for _ in range(size):
                conn = self._create_connection()
                # pre-warm by executing a simple statement
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                self.pool.append(conn)
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise DatabaseConnectionError(str(e)) from e
        await self.circuit_breaker.record_success()

    def _create_connection(self) -> pyodbc.Connection:
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.cfg.host},{self.cfg.port};"
            f"DATABASE={self.cfg.database};UID={self.cfg.user};PWD={self.cfg.password}",
            autocommit=False,
        )

    async def _acquire(self) -> pyodbc.Connection:
        async with self._lock:
            if self.pool:
                conn = self.pool.pop()
            else:
                conn = self._create_connection()
            metrics.set("sqlserver_pool_size", float(len(self.pool)))
            return conn

    async def _release(self, conn: pyodbc.Connection) -> None:
        async with self._lock:
            self.pool.append(conn)
            metrics.set("sqlserver_pool_size", float(len(self.pool)))

    async def prepare(self, query: str) -> None:
        conn = await self._acquire()
        try:
            cursor = conn.cursor()
            cursor.prepare(query)
            self._prepared[query] = query
        finally:
            await self._release(conn)

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            await self.circuit_breaker.record_success()
            return rows
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            await self.circuit_breaker.record_success()
            return rows
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute(self, query: str, *params: Any) -> int:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            if query in self._prepared:
                cursor.execute(None, params)
            else:
                cursor.execute(query, params)
            conn.commit()
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            await self.circuit_breaker.record_success()
            return cursor.rowcount
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            if query in self._prepared:
                cursor.execute(None, params)
            else:
                cursor.execute(query, params)
            conn.commit()
            await self.circuit_breaker.record_success()
            return cursor.rowcount
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def execute_many(
        self, query: str, params_seq: Iterable[Iterable[Any]]
    ) -> int:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        conn = await self._acquire()
        params_list = list(params_seq)
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.fast_executemany = True
            if query in self._prepared:
                cursor.executemany(None, params_list)
            else:
                cursor.executemany(query, params_list)
            conn.commit()
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            await self.circuit_breaker.record_success()
            return cursor.rowcount
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.fast_executemany = True
            if query in self._prepared:
                cursor.executemany(None, params_list)
            else:
                cursor.executemany(query, params_list)
            conn.commit()
            await self.circuit_breaker.record_success()
            return cursor.rowcount
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise QueryError(str(e)) from e
        finally:
            await self._release(conn)

    async def health_check(self) -> bool:
        if not await self.circuit_breaker.allow():
            return False
        try:
            await self.execute("SELECT 1")
            await self.circuit_breaker.record_success()
            return True
        except DatabaseError:
            await self.circuit_breaker.record_failure()
            return False

    def report_pool_status(self) -> None:
        metrics.set("sqlserver_pool_size", float(len(self.pool)))

    async def close(self) -> None:
        async with self._lock:
            while self.pool:
                conn = self.pool.pop()
                conn.close()
