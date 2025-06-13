from typing import Awaitable, Callable, List


class CompensationTransaction:
    """Async context manager for compensation actions."""

    def __init__(self) -> None:
        self._actions: List[Callable[[], Awaitable[None]]] = []

    def add(self, action: Callable[[], Awaitable[None]]) -> None:
        """Register a compensation action executed on failure."""
        self._actions.append(action)

    async def __aenter__(self) -> "CompensationTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc:
            for action in reversed(self._actions):
                try:
                    await action()
                except Exception:
                    continue
