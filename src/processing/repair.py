import os
from typing import List, Optional

try:  # pragma: no cover - optional dependency
    import joblib
except Exception:  # pragma: no cover - allow tests to run without joblib
    joblib = None


class MLRepairAdvisor:
    """Suggest repairs using an ML model."""

    def __init__(self, model_path: str = "src/models/repair_advisor.joblib") -> None:
        self.model_path = model_path
        self.model = None
        if joblib and os.path.exists(model_path):
            try:
                self.model = joblib.load(model_path)
            except Exception:
                self.model = None

    def predict_suggestions(self, errors: List[str], claim: dict) -> List[str]:
        """Return ML-based repair suggestions."""
        if not self.model:
            return []

        payload = {"errors": errors, **claim}
        try:
            result = self.model.predict(payload)
        except Exception:
            try:
                result = self.model.predict(errors, claim)
            except Exception:
                return []

        if isinstance(result, list):
            return [str(r) for r in result]
        if result is None:
            return []
        return [str(result)]


class ClaimRepairSuggester:
    """Generate repair suggestions for failed claims."""

    def __init__(self, advisor: Optional[MLRepairAdvisor] = None) -> None:
        self.advisor = advisor

    def suggest(self, errors: List[str], claim: Optional[dict] = None) -> str:
        suggestions = []
        if "invalid_facility" in errors:
            suggestions.append("Verify facility_id")
        if "invalid_financial_class" in errors:
            suggestions.append("Check financial_class")
        if "invalid_dob" in errors:
            suggestions.append("Check date_of_birth")
        if "invalid_service_dates" in errors:
            suggestions.append("Correct service dates")
        if self.advisor and claim is not None:
            suggestions.extend(self.advisor.predict_suggestions(errors, claim))
        return "; ".join(suggestions)

    def auto_repair(self, claim: dict, errors: List[str]) -> dict:
        """Attempt simple automatic repairs based on validation errors."""
        fixed = dict(claim)
        if "invalid_facility" in errors:
            fixed["facility_id"] = fixed.get("facility_id") or "UNKNOWN"
        if "invalid_financial_class" in errors:
            fixed["financial_class"] = fixed.get("financial_class") or "UNKNOWN"
        if "invalid_dob" in errors:
            start = fixed.get("service_from_date") or fixed.get("service_to_date")
            if start:
                fixed["date_of_birth"] = start
        if "invalid_service_dates" in errors:
            start = fixed.get("service_from_date")
            end = fixed.get("service_to_date")
            if start and end and start > end:
                fixed["service_to_date"] = start
        return fixed
