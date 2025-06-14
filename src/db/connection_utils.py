from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

from ..monitoring.metrics import metrics
from ..utils.circuit_breaker import CircuitBreaker
from ..utils.errors import CircuitBreakerOpenError, DatabaseConnectionError

T = TypeVar("T")

async def connect_with_retry(
    cb: CircuitBreaker,
    open_error: CircuitBreakerOpenError,
    connect_fn: Callable[[], Awaitable[T]],
    retries: int = 3,
    delay: float = 0.5,
) -> T:
    """Attempt a connection with retries and circuit breaker support."""
    if not await cb.allow():
        raise open_error
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            result = await connect_fn()
            await cb.record_success()
            return result
        except Exception as exc:  # noqa: BLE001 - propagate actual error
            last_exc = exc
            await cb.record_failure()
            if attempt < retries - 1:
                await asyncio.sleep(delay)
    raise DatabaseConnectionError(str(last_exc)) from last_exc

def report_pool_metrics(
    prefix: str,
    *,
    size: int,
    min_size: int = 0,
    max_size: int = 0,
    connections_created: int | None = None,
    prepared_statements: int | None = None,
    cache_memory: int = 0,
) -> None:
    """Report common pool metrics."""
    metrics.set(f"{prefix}_pool_size", float(size))
    metrics.set(f"{prefix}_pool_max_size", float(max_size))
    metrics.set(f"{prefix}_pool_min_size", float(min_size))
    if connections_created is not None:
        metrics.set(f"{prefix}_connections_created", float(connections_created))
    if prepared_statements is not None:
        metrics.set(f"{prefix}_prepared_statements", float(prepared_statements))
    metrics.set(f"{prefix}_cache_memory_mb", cache_memory / 1024 / 1024)
    if max_size:
        metrics.set(f"{prefix}_pool_utilization", (max_size - size) / max_size)
