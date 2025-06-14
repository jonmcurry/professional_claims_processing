from __future__ import annotations
from typing import Any
from ..monitoring.metrics import metrics


class ModelMonitor:
    """Record basic model performance metrics."""

    def __init__(self, version: str):
        self.version = version

    def record_prediction(self, label: int) -> None:
        metrics.inc(f"model_{self.version}_pred_{label}")
        metrics.inc(f"model_{self.version}_total")

    def record_result(self, predicted: int, actual: int) -> None:
        """Record a prediction with ground truth for accuracy tracking."""
        metrics.inc(f"model_{self.version}_pred_{predicted}")
        metrics.inc(f"model_{self.version}_total")
        if predicted == actual:
            metrics.inc(f"model_{self.version}_correct")
        total = metrics.get(f"model_{self.version}_total")
        correct = metrics.get(f"model_{self.version}_correct")
        if total:
            metrics.set(f"model_{self.version}_accuracy", correct / total)
