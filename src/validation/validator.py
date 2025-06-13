from typing import Dict, Any, List


class ClaimValidator:
    def __init__(self, valid_facilities: set[str], valid_financial_classes: set[str]):
        self.valid_facilities = valid_facilities
        self.valid_financial_classes = valid_financial_classes

    def validate(self, claim: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        if claim.get("facility_id") not in self.valid_facilities:
            errors.append("invalid_facility")
        if claim.get("financial_class") not in self.valid_financial_classes:
            errors.append("invalid_financial_class")
        dob = claim.get("date_of_birth")
        if dob and dob > claim.get("service_from_date"):
            errors.append("invalid_dob")
        start = claim.get("service_from_date")
        end = claim.get("service_to_date")
        if start and end and start > end:
            errors.append("invalid_service_dates")
        return errors
