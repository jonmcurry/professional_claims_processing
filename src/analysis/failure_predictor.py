from __future__ import annotations

from typing import Any, Dict, List

from ..models.features import extract_features

try:  # pragma: no cover - allow missing dependency in some environments
    from sklearn.linear_model import LogisticRegression
except Exception:  # pragma: no cover - allow missing dependency
    LogisticRegression = None


class FailurePredictor:
    """Predict the probability of a claim failing."""

    def __init__(self) -> None:
        if LogisticRegression:
            self.model: LogisticRegression | None = LogisticRegression()
            self.stats: Dict[str, tuple[int, int]] | None = None
        else:  # pragma: no cover - used when scikit-learn is unavailable
            self.model = None
            self.stats = {}

    def train(self, claims: List[Dict[str, Any]], labels: List[int]) -> None:
        """Train the predictor using historical claim outcomes."""
        if self.model:
            vectors = []
            for claim in claims:
                feats = extract_features(claim)
                vectors.append([feats[k] for k in sorted(feats)])
            self.model.fit(vectors, labels)
        else:  # pragma: no cover - simple frequency-based training
            for claim, label in zip(claims, labels):
                key = claim.get("procedure_code") or "unknown"
                total, pos = self.stats.get(key, (0, 0))  # type: ignore[assignment]
                self.stats[key] = (total + 1, pos + label)

    def predict_proba(self, claim: Dict[str, Any]) -> float:
        """Return the probability that the claim will fail."""
        if self.model:
            feats = extract_features(claim)
            vector = [feats[k] for k in sorted(feats)]
            proba = self.model.predict_proba([vector])[0][1]
            return float(proba)
        series = self.stats.get(claim.get("procedure_code") or "unknown")  # type: ignore[assignment]
        if not series:
            return 0.0
        total, pos = series
        return pos / total if total else 0.0
