from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List


def extract_features(claim: Dict[str, Any]) -> Dict[str, float]:
    """Simple feature engineering for claim prediction."""
    features: Dict[str, float] = {}
    proc_code = claim.get("procedure_code") or ""
    features["procedure_code_len"] = float(len(str(proc_code)))
    financial_class = claim.get("financial_class") or ""
    features["financial_class_id"] = float(abs(hash(financial_class)) % 1000)
    features["has_diagnosis"] = 1.0 if claim.get("primary_diagnosis") else 0.0
    return features


class FeaturePipeline:
    """Composable feature engineering pipeline."""

    def __init__(self, steps: List[Callable[[Dict[str, Any]], Dict[str, float]]]):
        self.steps = steps

    def run(self, claim: Dict[str, Any]) -> Dict[str, float]:
        features: Dict[str, float] = {}
        for step in self.steps:
            features.update(step(claim))
        return features

    def run_batch(
        self, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Run the pipeline for a batch of claims in parallel."""
        with ThreadPoolExecutor(max_workers=max_workers) as exc:
            return list(exc.map(self.run, claims))


default_feature_pipeline = FeaturePipeline([extract_features])
