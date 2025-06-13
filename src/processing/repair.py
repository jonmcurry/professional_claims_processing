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

