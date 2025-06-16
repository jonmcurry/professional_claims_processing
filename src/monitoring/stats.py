from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict

from .metrics import metrics


class LatencyTracker:
    """Track latency samples and expose percentile calculations."""

    def __init__(self, window: int = 1000) -> None:
        self.window = window
        self._data: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )

    def record(self, name: str, value: float) -> None:
        series = self._data[name]
        series.append(value)
        metrics.set(f"{name}_p99_ms", self.p99(name))

    def p99(self, name: str) -> float:
        series = self._data.get(name)
        if not series:
            return 0.0
        ordered = sorted(series)
        idx = int(len(ordered) * 0.99)
        if idx >= len(ordered):
            idx = len(ordered) - 1
        return ordered[idx]


latencies = LatencyTracker()
