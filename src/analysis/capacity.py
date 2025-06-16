from __future__ import annotations

from typing import List


def _forecast(history: List[float], window: int) -> float:
    """Return a simple forecast using moving average and trend."""
    if not history:
        return 0.0
    recent = history[-window:]
    avg = sum(recent) / len(recent)
    trend = (recent[-1] - recent[0]) / max(len(recent) - 1, 1)
    return avg + trend


def predict_throughput(history: List[float], window: int = 10) -> float:
    """Predict next throughput value."""
    return _forecast(history, window)


def predict_resource_usage(history: List[float], window: int = 10) -> float:
    """Predict next resource usage value (e.g., CPU or memory)."""
    return _forecast(history, window)
