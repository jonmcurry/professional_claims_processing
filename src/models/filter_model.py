from typing import Any, Dict, List

try:
    import joblib
except Exception:  # pragma: no cover - allow missing dependency in tests
    joblib = None
from .features import FeaturePipeline, default_feature_pipeline


class FilterModel:
    def __init__(
        self,
        path: str,
        version: str = "1",
        feature_pipeline: FeaturePipeline | None = None,
    ):
        self.path = path
        self.version = version
        self.feature_pipeline = feature_pipeline or default_feature_pipeline
        if not joblib:
            raise ImportError("joblib is required to load models")
        self.model = joblib.load(path)

    def predict(self, claim: Dict[str, Any]) -> int:
        features = self.feature_pipeline.run(claim)
        vector = [features[k] for k in sorted(features)]
        return int(self.model.predict([vector])[0])

    def predict_batch(self, claims: List[Dict[str, Any]]) -> List[int]:
        """Predict for a batch of claims using vectorized inference."""
        feature_list = self.feature_pipeline.run_batch(claims)
        if not feature_list:
            return []
        keys = sorted({k for feats in feature_list for k in feats})
        vectors = [[feats.get(k, 0.0) for k in keys] for feats in feature_list]
        preds = self.model.predict(vectors)
        return [int(p) for p in preds]
