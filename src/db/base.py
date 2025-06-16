from typing import Any, Iterable


class BaseDatabase:
    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        raise NotImplementedError

    async def execute(self, query: str, *params: Any) -> int:
        raise NotImplementedError

    async def execute_many(
        self, query: str, params_seq: Iterable[Iterable[Any]]
    ) -> int:
        """Execute a statement against many parameter sets."""
        raise NotImplementedError

    async def close(self) -> None:
        """Close any underlying connection pools."""
        raise NotImplementedError
