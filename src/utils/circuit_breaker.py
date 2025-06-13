import asyncio
import time

class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is open."""


class CircuitBreaker:
    """Simple async circuit breaker."""

    def __init__(self, failure_threshold: int = 3, recovery_time: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_count = 0
        self.open_until: float | None = None
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            if self.open_until is None:
                return True
            if time.monotonic() >= self.open_until:
                self.failure_count = 0
                self.open_until = None
                return True
            return False

    async def record_success(self) -> None:
        async with self._lock:
            self.failure_count = 0
            self.open_until = None

    async def record_failure(self) -> None:
        async with self._lock:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.open_until = time.monotonic() + self.recovery_time
