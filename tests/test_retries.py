import asyncio
import random

import pytest

from src.utils.retries import retry_async


def test_retry_async_jitter_and_max_delay(monkeypatch):
    calls = 0
    delays: list[float] = []

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
    wrapped = retry_async(retries=3, backoff=1.0, max_delay=2.5, jitter=0.1)(failing)

    result = asyncio.run(wrapped())

    assert result == 3
    assert abs(delays[0] - 0.9268728488224803) < 1e-6
    assert abs(delays[1] - 2.138973494774893) < 1e-6


def test_retry_async_respects_max_delay(monkeypatch):
    delays: list[float] = []

    async def always_fail():
        raise RuntimeError

    async def fake_sleep(delay: float):
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with pytest.raises(RuntimeError):
        wrapped = retry_async(retries=3, backoff=1.0, max_delay=2.0)(always_fail)
        asyncio.run(wrapped())

    assert delays == [1.0, 2.0]
