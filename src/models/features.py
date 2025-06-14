from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List
import asyncio


def extract_features(claim: Dict[str, Any]) -> Dict[str, float]:
    """Simple feature engineering for claim prediction."""
    features: Dict[str, float] = {}
    proc_code = claim.get("procedure_code") or ""
    features["procedure_code_len"] = float(len(str(proc_code)))
    financial_class = claim.get("financial_class") or ""
    features["financial_class_id"] = float(abs(hash(financial_class)) % 1000)
    features["has_diagnosis"] = 1.0 if claim.get("primary_diagnosis") else 0.0
    return features


def extract_features_vectorized(claims: List[Dict[str, Any]]) -> List[Dict[str, float]]:
    """Vectorized feature extraction for batch processing."""
    features_list = []
    
    # Pre-extract common fields for vectorized operations
    proc_codes = [claim.get("procedure_code") or "" for claim in claims]
    financial_classes = [claim.get("financial_class") or "" for claim in claims]
    diagnoses = [claim.get("primary_diagnosis") for claim in claims]
    
    # Vectorized computations
    proc_code_lens = [float(len(str(code))) for code in proc_codes]
    financial_class_ids = [float(abs(hash(fc)) % 1000) for fc in financial_classes]
    has_diagnoses = [1.0 if diag else 0.0 for diag in diagnoses]
    
    # Build feature dictionaries
    for i in range(len(claims)):
        features = {
            "procedure_code_len": proc_code_lens[i],
            "financial_class_id": financial_class_ids[i],
            "has_diagnosis": has_diagnoses[i]
        }
        features_list.append(features)
    
    return features_list


class FeaturePipeline:
    """Composable feature engineering pipeline with vectorized processing support."""

    def __init__(self, steps: List[Callable[[Dict[str, Any]], Dict[str, float]]]):
        self.steps = steps
        self._vectorized_steps = []
        
        # Check if steps support vectorized processing
        for step in steps:
            if hasattr(step, '__name__') and step.__name__ == 'extract_features':
                # Use vectorized version for the base feature extractor
                self._vectorized_steps.append(extract_features_vectorized)
            else:
                self._vectorized_steps.append(None)

    def run(self, claim: Dict[str, Any]) -> Dict[str, float]:
        features: Dict[str, float] = {}
        for step in self.steps:
            features.update(step(claim))
        return features

    def run_batch(
        self, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Run the pipeline for a batch of claims with vectorized optimizations."""
        if not claims:
            return []
        
        # Use vectorized processing for large batches
        if len(claims) > 500:
            return self._run_batch_vectorized(claims, max_workers)
        
        # Use parallel processing for medium batches
        if len(claims) > 50:
            return self._run_batch_parallel(claims, max_workers)
        
        # Sequential processing for small batches
        return [self.run(claim) for claim in claims]

    def _run_batch_vectorized(
        self, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Vectorized batch processing for maximum performance."""
        if len(self.steps) == 1 and self._vectorized_steps[0] is not None:
            # Use fully vectorized processing
            return self._vectorized_steps[0](claims)
        
        # Mixed vectorized and parallel processing
        results = []
        
        # Initialize feature dictionaries
        for _ in claims:
            results.append({})
        
        # Process each step
        for i, (step, vectorized_step) in enumerate(zip(self.steps, self._vectorized_steps)):
            if vectorized_step is not None:
                # Use vectorized step
                step_results = vectorized_step(claims)
                for j, step_features in enumerate(step_results):
                    results[j].update(step_features)
            else:
                # Use parallel processing for this step
                step_results = self._run_step_parallel(step, claims, max_workers)
                for j, step_features in enumerate(step_results):
                    results[j].update(step_features)
        
        return results

    def _run_batch_parallel(
        self, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Parallel batch processing using thread pool."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(self.run, claims))

    def _run_step_parallel(
        self, step: Callable, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Run a single step in parallel across all claims."""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(step, claims))

    async def run_batch_async(
        self, claims: List[Dict[str, Any]], max_workers: int | None = None
    ) -> List[Dict[str, float]]:
        """Async batch processing for non-blocking operation."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.run_batch, claims, max_workers)

    def add_step(self, step: Callable[[Dict[str, Any]], Dict[str, float]]) -> None:
        """Add a new feature extraction step."""
        self.steps.append(step)
        self._vectorized_steps.append(None)  # No vectorized version by default

    def get_pipeline_info(self) -> Dict[str, Any]:
        """Get information about the pipeline configuration."""
        return {
            "step_count": len(self.steps),
            "vectorized_steps": sum(1 for vs in self._vectorized_steps if vs is not None),
            "step_names": [getattr(step, '__name__', 'unknown') for step in self.steps]
        }


# Enhanced default pipeline with vectorized support
default_feature_pipeline = FeaturePipeline([extract_features])