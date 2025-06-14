from typing import Any, Dict
try:
    import joblib
except Exception:  # pragma: no cover - allow missing dependency in tests
    joblib = None
from .features import FeaturePipeline, default_feature_pipeline, extract_features


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
