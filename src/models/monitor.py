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
