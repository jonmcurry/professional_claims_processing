import time
from typing import Any, Dict, Optional


class InMemoryCache:
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self.store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self.store.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > self.ttl:
            del self.store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self.store[key] = (time.time(), value)
