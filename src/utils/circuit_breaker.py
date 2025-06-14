import asyncio
import time
from typing import Awaitable, Callable, TypeVar

from ..monitoring.metrics import metrics


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open."""


T = TypeVar("T")


class CircuitBreaker:
    """Simple async circuit breaker with metrics support."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_time: float = 30.0,
        *,
        name: str | None = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_count = 0
        self.open_until: float | None = None
        self._lock = asyncio.Lock()
        self.name = name
        if self.name:
            metrics.set(f"{self.name}_circuit_open", 0.0)

    async def allow(self) -> bool:
        async with self._lock:
            if self.open_until is None:
                return True
            if time.monotonic() >= self.open_until:
                self.failure_count = 0
                self.open_until = None
                self._update_metrics()
                return True
            return False

    async def record_success(self) -> None:
        async with self._lock:
            self.failure_count = 0
            self.open_until = None
            self._update_metrics()

    async def record_failure(self) -> None:
        async with self._lock:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.open_until = time.monotonic() + self.recovery_time
            self._update_metrics()

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        if not await self.allow():
            raise CircuitBreakerOpenError("Circuit breaker open")
        try:
            result = await func(*args, **kwargs)
        except Exception:
            await self.record_failure()
            raise
        await self.record_success()
        return result

    def _update_metrics(self) -> None:
        if self.name:
            metrics.set(
                f"{self.name}_circuit_open",
                1.0 if self.is_open else 0.0,
            )

    @property
    def is_open(self) -> bool:
        return self.open_until is not None and time.monotonic() < self.open_until
