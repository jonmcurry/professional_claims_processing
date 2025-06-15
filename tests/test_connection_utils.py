import asyncio
import random

import pytest

from src.db.connection_utils import connect_with_retry
from src.utils.errors import CircuitBreakerOpenError, DatabaseConnectionError


class DummyCB:
    async def allow(self):
        return True

    async def record_success(self):
        pass

    async def record_failure(self):
        pass


def test_connect_with_retry_jitter_and_max_delay(monkeypatch):
    calls = 0
    delays = []

    async def failing():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("fail")
        return calls

    async def fake_sleep(delay: float):
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    random.seed(1)
    cb = DummyCB()
    result = asyncio.run(
        connect_with_retry(
            cb,
            CircuitBreakerOpenError("open"),
            failing,
            retries=3,
            delay=1.0,
            max_delay=2.5,
            jitter=0.1,
        )
    )

    assert result == 3
    assert abs(delays[0] - 0.9268728488224803) < 1e-6
    assert abs(delays[1] - 2.138973494774893) < 1e-6


def test_connect_with_retry_respects_max_delay(monkeypatch):
    delays = []

    async def always_fail():
        raise RuntimeError

    async def fake_sleep(delay: float):
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    cb = DummyCB()
    with pytest.raises(DatabaseConnectionError):
        asyncio.run(
            connect_with_retry(
                cb,
                CircuitBreakerOpenError("open"),
                always_fail,
                retries=3,
                delay=1.0,
                max_delay=2.0,
            )
        )

    assert delays == [1.0, 2.0]
