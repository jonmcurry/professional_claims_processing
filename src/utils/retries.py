import asyncio
import random
from functools import wraps
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


def retry_async(
    retries: int = 3,
    backoff: float = 0.5,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    *,
    max_delay: float | None = None,
    jitter: float = 0.0,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Retry an async function with exponential backoff.

    Parameters
    ----------
    retries:
        Number of attempts before giving up.
    backoff:
        Initial delay between retries.
    exceptions:
        Exception types to catch and retry.
    max_delay:
        Maximum delay between attempts. ``None`` for unlimited.
    jitter:
        Apply random jitter in the range ``± jitter * delay``.
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            delay = backoff
            for attempt in range(retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions:
                    if attempt >= retries - 1:
                        raise
                    sleep_time = delay
                    if jitter:
                        jitter_range = jitter * sleep_time
                        sleep_time += random.uniform(-jitter_range, jitter_range)
                        sleep_time = max(0.0, sleep_time)
                    await asyncio.sleep(sleep_time)
                    delay *= 2
                    if max_delay is not None:
                        delay = min(delay, max_delay)
            return await func(*args, **kwargs)

        return wrapper

    return decorator

