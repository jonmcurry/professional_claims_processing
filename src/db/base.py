from typing import Any, Iterable


class BaseDatabase:
    async def fetch(self, query: str, *params: Any) -> Iterable[dict]:
        raise NotImplementedError

    async def execute(self, query: str, *params: Any) -> int:
        raise NotImplementedError
