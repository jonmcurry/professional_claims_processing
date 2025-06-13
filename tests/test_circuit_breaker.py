import asyncio
from src.utils.circuit_breaker import CircuitBreaker


def test_circuit_breaker_blocks_after_failures():
    async def run():
        cb = CircuitBreaker(failure_threshold=2, recovery_time=0.1)
        assert await cb.allow()
        await cb.record_failure()
        assert await cb.allow()
        await cb.record_failure()
        assert not await cb.allow()
        await asyncio.sleep(0.11)
        assert await cb.allow()
    asyncio.run(run())
