import heapq
from typing import Any, Dict, List, Tuple


class PriorityClaimQueue:
    """Simple in-memory priority queue for claims."""

    def __init__(self) -> None:
        self._queue: List[Tuple[int, int, Dict[str, Any]]] = []
        self._counter = 0  # Tie-breaker to maintain insertion order

    def push(self, claim: Dict[str, Any], priority: int) -> None:
        """Add a claim to the queue with the given priority."""
        # Use counter as tie-breaker to avoid comparing dicts
        heapq.heappush(self._queue, (-priority, self._counter, claim))
        self._counter += 1

    def pop(self) -> Dict[str, Any] | None:
        """Remove and return the highest priority claim."""
        if not self._queue:
            return None
        _, _, claim = heapq.heappop(self._queue)
        return claim

    def __len__(self) -> int:
        return len(self._queue)
