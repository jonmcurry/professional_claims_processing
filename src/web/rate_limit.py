import asyncio
import time


class RateLimiter:
    def __init__(self, rate: int = 100, per: float = 1.0):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            current = time.monotonic()
            elapsed = current - self.last_check
            self.last_check = current
            self.allowance += elapsed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate
            if self.allowance < 1.0:
                return False
            self.allowance -= 1.0
            return True
