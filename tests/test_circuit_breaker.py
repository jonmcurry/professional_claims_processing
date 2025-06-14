import asyncio

from src.monitoring.metrics import metrics
from src.utils.circuit_breaker import CircuitBreaker


def test_circuit_breaker_blocks_after_failures():
    async def run():
        cb = CircuitBreaker(failure_threshold=2, recovery_time=0.1, name="test")
        assert metrics.get("test_circuit_open") == 0
        assert await cb.allow()
        await cb.record_failure()
        assert metrics.get("test_circuit_open") == 0
        assert await cb.allow()
        await cb.record_failure()
        assert not await cb.allow()
        assert metrics.get("test_circuit_open") == 1
        await asyncio.sleep(0.11)
        assert await cb.allow()
        assert metrics.get("test_circuit_open") == 0

    asyncio.run(run())
