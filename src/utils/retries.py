import asyncio
from functools import wraps
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


def retry_async(retries: int = 3, backoff: float = 0.5,
                exceptions: tuple[type[Exception], ...] = (Exception,)) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Retry an async function with exponential backoff."""

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
                    await asyncio.sleep(delay)
                    delay *= 2
            return await func(*args, **kwargs)

        return wrapper

    return decorator

