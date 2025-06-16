from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import numpy as np

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
        self._feature_keys_cache = None

    def predict(self, claim: Dict[str, Any]) -> int:
        features = self.feature_pipeline.run(claim)
        vector = [features[k] for k in sorted(features)]
        return int(self.model.predict([vector])[0])

    def predict_batch(self, claims: List[Dict[str, Any]]) -> List[int]:
        """Optimized batch prediction using vectorized operations."""
        if not claims:
            return []

        # For very large batches, use chunked processing to manage memory
        if len(claims) > 10000:
            return self._predict_batch_chunked(claims, chunk_size=5000)

        # Extract features for all claims in parallel
        feature_list = self.feature_pipeline.run_batch(claims, max_workers=4)
        if not feature_list:
            return []

        # Get consistent feature keys (cache for performance)
        if self._feature_keys_cache is None:
            self._feature_keys_cache = sorted(
                {k for feats in feature_list for k in feats}
            )

        feature_keys = self._feature_keys_cache

        # Vectorized feature matrix creation
        try:
            # Use numpy for faster vector operations if available
            vectors = np.array(
                [[feats.get(k, 0.0) for k in feature_keys] for feats in feature_list],
                dtype=np.float32,
            )
        except (ImportError, Exception):
            # Fallback to list comprehension
            vectors = [
                [feats.get(k, 0.0) for k in feature_keys] for feats in feature_list
            ]

        # Batch prediction
        preds = self.model.predict(vectors)
        return [int(p) for p in preds]

    def _predict_batch_chunked(
        self, claims: List[Dict[str, Any]], chunk_size: int = 5000
    ) -> List[int]:
        """Process large batches in chunks to manage memory usage."""
        all_predictions = []

        for i in range(0, len(claims), chunk_size):
            chunk = claims[i : i + chunk_size]
            chunk_predictions = self.predict_batch(chunk)
            all_predictions.extend(chunk_predictions)

        return all_predictions

    def predict_batch_parallel(
        self, claims: List[Dict[str, Any]], max_workers: int = 2
    ) -> List[int]:
        """Parallel batch prediction for CPU-intensive models."""
        if not claims or len(claims) < 1000:
            return self.predict_batch(claims)

        # Split claims into chunks for parallel processing
        chunk_size = max(len(claims) // max_workers, 500)
        claim_chunks = [
            claims[i : i + chunk_size] for i in range(0, len(claims), chunk_size)
        ]

        def predict_chunk(chunk: List[Dict[str, Any]]) -> List[int]:
            return self.predict_batch(chunk)

        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chunk_results = list(executor.map(predict_chunk, claim_chunks))

        # Flatten results
        all_predictions = []
        for chunk_result in chunk_results:
            all_predictions.extend(chunk_result)

        return all_predictions

    def warm_up(self, sample_claims: List[Dict[str, Any]]) -> None:
        """Warm up the model with sample predictions to optimize performance."""
        if sample_claims:
            # Cache feature keys
            sample_features = self.feature_pipeline.run_batch(sample_claims[:10])
            if sample_features:
                self._feature_keys_cache = sorted(
                    {k for feats in sample_features for k in feats}
                )

            # Run a small batch prediction to warm up the model
            self.predict_batch(sample_claims[:10])

    def get_model_info(self) -> Dict[str, Any]:
        """Get model information for monitoring."""
        return {
            "version": self.version,
            "path": self.path,
            "feature_keys_cached": self._feature_keys_cache is not None,
            "feature_count": len(self._feature_keys_cache)
            if self._feature_keys_cache
            else None,
        }
