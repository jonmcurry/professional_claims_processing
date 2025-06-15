from __future__ import annotations

import random
from typing import Any, Dict, List, Protocol

import numpy as np

from ..monitoring.metrics import metrics


class _Predictor(Protocol):
    def predict(self, claim: Dict[str, Any]) -> int:
        ...

    def predict_batch(self, claims: List[Dict[str, Any]]) -> List[int]:
        ...


class ABTestModel:
    """Route predictions between two models based on a ratio with optimized batch processing."""

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

    def predict_batch(self, claims: List[Dict[str, Any]]) -> List[int]:
        """Optimized batch prediction with vectorized model routing."""
        if not claims:
            return []

        # Generate routing decisions for entire batch
        batch_size = len(claims)
        try:
            # Use numpy for efficient random number generation if available
            routing_decisions = np.random.random(batch_size) < self.ratio
            model_a_indices = np.where(routing_decisions)[0]
            model_b_indices = np.where(~routing_decisions)[0]
        except (ImportError, Exception):
            # Fallback to standard library
            routing_decisions = [
                random.random() < self.ratio for _ in range(batch_size)
            ]
            model_a_indices = [i for i, use_a in enumerate(routing_decisions) if use_a]
            model_b_indices = [
                i for i, use_a in enumerate(routing_decisions) if not use_a
            ]

        # Split claims into batches for each model
        model_a_claims = [claims[i] for i in model_a_indices]
        model_b_claims = [claims[i] for i in model_b_indices]

        # Get predictions from each model in batches
        results = [0] * batch_size

        if model_a_claims and hasattr(self.model_a, "predict_batch"):
            model_a_predictions = self.model_a.predict_batch(model_a_claims)
            for idx, pred in zip(model_a_indices, model_a_predictions):
                results[idx] = pred
        elif model_a_claims:
            # Fallback to individual predictions
            for idx in model_a_indices:
                results[idx] = self.model_a.predict(claims[idx])

        if model_b_claims and hasattr(self.model_b, "predict_batch"):
            model_b_predictions = self.model_b.predict_batch(model_b_claims)
            for idx, pred in zip(model_b_indices, model_b_predictions):
                results[idx] = pred
        elif model_b_claims:
            # Fallback to individual predictions
            for idx in model_b_indices:
                results[idx] = self.model_b.predict(claims[idx])

        return results


class ABTestManager:
    """Wrapper that tracks variant exposure metrics with batch processing support."""

    def __init__(self, model_a: _Predictor, model_b: _Predictor, ratio: float = 0.5):
        self.ab_model = ABTestModel(model_a, model_b, ratio)
        self.version_a = getattr(model_a, "version", "a")
        self.version_b = getattr(model_b, "version", "b")

    def predict(self, claim: Dict[str, Any]) -> int:
        use_a = random.random() < self.ab_model.ratio
        variant = self.version_a if use_a else self.version_b
        metrics.inc(f"ab_exposure_{variant}")
        model = self.ab_model.model_a if use_a else self.ab_model.model_b
        return model.predict(claim)

    def predict_batch(self, claims: List[Dict[str, Any]]) -> List[int]:
        """Batch prediction with exposure tracking."""
        if not claims:
            return []

        batch_size = len(claims)

        # Generate routing decisions
        try:
            routing_decisions = np.random.random(batch_size) < self.ab_model.ratio
            a_count = int(np.sum(routing_decisions))
            b_count = batch_size - a_count
        except (ImportError, Exception):
            routing_decisions = [
                random.random() < self.ab_model.ratio for _ in range(batch_size)
            ]
            a_count = sum(routing_decisions)
            b_count = batch_size - a_count

        # Update exposure metrics
        metrics.inc(f"ab_exposure_{self.version_a}", a_count)
        metrics.inc(f"ab_exposure_{self.version_b}", b_count)

        # Delegate to AB model for actual predictions
        return self.ab_model.predict_batch(claims)

    def get_exposure_stats(self) -> Dict[str, float]:
        """Get current exposure statistics."""
        total_a = metrics.get(f"ab_exposure_{self.version_a}")
        total_b = metrics.get(f"ab_exposure_{self.version_b}")
        total = total_a + total_b

        if total > 0:
            return {
                f"exposure_rate_{self.version_a}": total_a / total,
                f"exposure_rate_{self.version_b}": total_b / total,
                "total_predictions": total,
            }
        return {
            f"exposure_rate_{self.version_a}": 0.0,
            f"exposure_rate_{self.version_b}": 0.0,
            "total_predictions": 0,
        }
