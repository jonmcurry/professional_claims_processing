from collections import defaultdict
from threading import Lock
from typing import Dict


class MetricsRegistry:
    """Simple in-memory metrics collector."""

    def __init__(self) -> None:
        self._metrics: Dict[str, float] = defaultdict(float)
        self._lock = Lock()

    def inc(self, name: str, amount: float = 1.0) -> None:
        with self._lock:
            self._metrics[name] += amount

    def set(self, name: str, value: float) -> None:
        with self._lock:
            self._metrics[name] = value

    def get(self, name: str) -> float:
        with self._lock:
            return self._metrics.get(name, 0.0)

    def as_dict(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._metrics)


metrics = MetricsRegistry()
