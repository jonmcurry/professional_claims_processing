from typing import List


class ClaimRepairSuggester:
    """Generate simple repair suggestions for failed claims."""

    def suggest(self, errors: List[str]) -> str:
        suggestions = []
        if "invalid_facility" in errors:
            suggestions.append("Verify facility_id")
        if "invalid_financial_class" in errors:
            suggestions.append("Check financial_class")
        if "invalid_dob" in errors:
            suggestions.append("Check date_of_birth")
        if "invalid_service_dates" in errors:
            suggestions.append("Correct service dates")
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
