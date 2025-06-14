import asyncio
import os
import time
import pyodbc
from typing import Iterable, Any

from ..utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from ..utils.errors import DatabaseConnectionError, QueryError, CircuitBreakerOpenError
from ..utils.cache import InMemoryCache

from .base import BaseDatabase
from ..config.config import SQLServerConfig
from ..monitoring.metrics import metrics
from ..analysis.query_tracker import record as record_query
from ..monitoring.stats import latencies


class SQLServerDatabase(BaseDatabase):
    def __init__(self, cfg: SQLServerConfig):
        self.cfg = cfg
        self.pool: list[pyodbc.Connection] = []
        self._lock = asyncio.Lock()
        # Set of SQL statements that have been prepared on all connections
        self._prepared: set[str] = set()
        self.circuit_breaker = CircuitBreaker()
        # Simple read-through cache for query results
        self.query_cache = InMemoryCache(ttl=60)
        self.min_pool = cfg.min_pool_size
        self.max_pool = cfg.max_pool_size

    async def connect(self, size: int | None = None) -> None:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        try:
            pool_size = size or self.cfg.pool_size
            for _ in range(pool_size):
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
        """Create a new connection and prepare cached statements."""
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.cfg.host},{self.cfg.port};"
            f"DATABASE={self.cfg.database};UID={self.cfg.user};PWD={self.cfg.password}",
            autocommit=False,
        )
        if self._prepared:
            cursor = conn.cursor()
            for stmt in self._prepared:
                try:
                    cursor.prepare(stmt)
                except Exception:
                    pass
        return conn

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
        """Prepare a statement on all existing connections."""
        async with self._lock:
            if query in self._prepared:
                return
            self._prepared.add(query)
            conns = list(self.pool)
        for conn in conns:
            try:
                cursor = conn.cursor()
                cursor.prepare(query)
            except Exception:
                continue

    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        cache_key = query + str(params)
        cached = self.query_cache.get(cache_key)
        if cached is not None:
            return cached
        conn = await self._acquire()
        try:
            start = time.perf_counter()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            duration = (time.perf_counter() - start) * 1000
            metrics.inc("sqlserver_query_ms", duration)
            metrics.inc("sqlserver_query_count")
            latencies.record("sqlserver_query", duration)
            record_query(query, duration)
            await self.circuit_breaker.record_success()
            self.query_cache.set(cache_key, rows)
            return rows
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            await self.circuit_breaker.record_success()
            self.query_cache.set(cache_key, rows)
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
            metrics.inc("sqlserver_query_count")
            latencies.record("sqlserver_query", duration)
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
        self,
        query: str,
        params_seq: Iterable[Iterable[Any]],
        *,
        concurrency: int = 1,
    ) -> int:
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        params_list = list(params_seq)
        if concurrency > 1 and len(params_list) > 0:
            chunks = [
                params_list[i : i + len(params_list) // concurrency + 1]
                for i in range(0, len(params_list), len(params_list) // concurrency + 1)
            ]

            async def run_chunk(chunk: list[Iterable[Any]]) -> int:
                conn = await self._acquire()
                try:
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    if query in self._prepared:
                        cursor.executemany(None, chunk)
                    else:
                        cursor.executemany(query, chunk)
                    conn.commit()
                    return cursor.rowcount
                finally:
                    await self._release(conn)

            results = await asyncio.gather(*[run_chunk(c) for c in chunks])
            return sum(results)

        conn = await self._acquire()
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
            metrics.inc("sqlserver_query_count")
            latencies.record("sqlserver_query", duration)
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

    async def bulk_insert_tvp(
        self,
        table: str,
        columns: Iterable[str],
        rows: Iterable[Iterable[Any]],
    ) -> int:
        """Bulk insert using a table-valued parameter."""
        if not await self.circuit_breaker.allow():
            raise CircuitBreakerOpenError("SQLServer circuit open")
        rows_list = list(rows)
        if not rows_list:
            return 0
        conn = await self._acquire()
        placeholders = ", ".join(["?"] * len(columns))
        tvp_insert = f"INSERT INTO {table} ({', '.join(columns)}) SELECT * FROM ?"
        fallback_insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        try:
            cursor = conn.cursor()
            try:
                if hasattr(cursor, "setinputsizes") and hasattr(pyodbc, "SQL_STRUCTURED"):
                    tvp_name = f"{table}_type"
                    cursor.setinputsizes([(pyodbc.SQL_STRUCTURED, tvp_name)])
                    cursor.fast_executemany = True
                    cursor.execute(tvp_insert, (rows_list,))
                else:
                    raise AttributeError("TVP not supported")
            except Exception:
                cursor.fast_executemany = True
                cursor.executemany(fallback_insert, rows_list)
            conn.commit()
            await self.circuit_breaker.record_success()
            return len(rows_list)
        except pyodbc.Error:
            conn.close()
            conn = self._create_connection()
            cursor = conn.cursor()
            try:
                if hasattr(cursor, "setinputsizes") and hasattr(pyodbc, "SQL_STRUCTURED"):
                    tvp_name = f"{table}_type"
                    cursor.setinputsizes([(pyodbc.SQL_STRUCTURED, tvp_name)])
                    cursor.fast_executemany = True
                    cursor.execute(tvp_insert, (rows_list,))
                else:
                    raise AttributeError("TVP not supported")
            except Exception:
                cursor.fast_executemany = True
                cursor.executemany(fallback_insert, rows_list)
            conn.commit()
            await self.circuit_breaker.record_success()
            return len(rows_list)
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

    async def adjust_pool_size(self) -> None:
        """Dynamically adjust pool size based on system load."""
        try:
            load = os.getloadavg()[0]
        except Exception:
            load = 0
        desired = self.cfg.pool_size
        if load > 4:
            desired = max(self.min_pool, len(self.pool) - 1)
        elif load < 1 and len(self.pool) < self.max_pool:
            desired = len(self.pool) + 1
        diff = desired - len(self.pool)
        if diff > 0:
            for _ in range(diff):
                try:
                    conn = self._create_connection()
                    self.pool.append(conn)
                except Exception:
                    break
        elif diff < 0:
            for _ in range(-diff):
                if self.pool:
                    conn = self.pool.pop()
                    conn.close()

    async def close(self) -> None:
        async with self._lock:
            while self.pool:
                conn = self.pool.pop()
                conn.close()
