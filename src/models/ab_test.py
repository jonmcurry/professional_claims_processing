from __future__ import annotations
import random
from typing import Any, Dict, Protocol


class _Predictor(Protocol):
    def predict(self, claim: Dict[str, Any]) -> int:
        ...


class ABTestModel:
    """Route predictions between two models based on a ratio."""

    def __init__(self, model_a: _Predictor, model_b: _Predictor, ratio: float = 0.5):
        if not 0.0 < ratio < 1.0:
            raise ValueError("ratio must be between 0 and 1")
        self.model_a = model_a
        self.model_b = model_b
        self.ratio = ratio

    def predict(self, claim: Dict[str, Any]) -> int:
        if random.random() < self.ratio:
            return self.model_a.predict(claim)
        return self.model_b.predict(claim)
