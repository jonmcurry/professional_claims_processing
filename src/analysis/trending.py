from __future__ import annotations

from collections import deque
from typing import Deque, Dict


class TrendingTracker:
    """Track metric values and provide simple trending analysis."""

    def __init__(self, window: int = 10) -> None:
        self.window = window
        self._data: Dict[str, Deque[float]] = {}

    def record(self, name: str, value: float) -> None:
        series = self._data.setdefault(name, deque(maxlen=self.window))
        series.append(value)

    def moving_average(self, name: str) -> float:
        series = self._data.get(name)
        if not series:
            return 0.0
        return sum(series) / len(series)

    def trend(self, name: str) -> float:
        series = self._data.get(name)
        if not series or len(series) < 2:
            return 0.0
        return series[-1] - series[0]
