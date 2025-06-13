from __future__ import annotations
from typing import Any, Dict


def extract_features(claim: Dict[str, Any]) -> Dict[str, float]:
    """Simple feature engineering for claim prediction."""
    features: Dict[str, float] = {}
    proc_code = claim.get("procedure_code") or ""
    features["procedure_code_len"] = float(len(str(proc_code)))
    financial_class = claim.get("financial_class") or ""
    features["financial_class_id"] = float(abs(hash(financial_class)) % 1000)
    features["has_diagnosis"] = 1.0 if claim.get("primary_diagnosis") else 0.0
    return features
