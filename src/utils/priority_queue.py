import heapq
from typing import Any, Dict, List, Tuple


class PriorityClaimQueue:
    """Simple in-memory priority queue for claims."""

    def __init__(self) -> None:
        self._queue: List[Tuple[int, Dict[str, Any]]] = []

    def push(self, claim: Dict[str, Any], priority: int) -> None:
        """Add a claim to the queue with the given priority."""
        heapq.heappush(self._queue, (-priority, claim))

    def pop(self) -> Dict[str, Any] | None:
        """Remove and return the highest priority claim."""
        if not self._queue:
            return None
        _, claim = heapq.heappop(self._queue)
        return claim

    def __len__(self) -> int:
        return len(self._queue)
