from typing import Dict, Any, List

from .validators import (
    validate_facility,
    validate_financial_class,
    validate_dob,
    validate_service_dates,
)


class ClaimValidator:
    def __init__(self, valid_facilities: set[str], valid_financial_classes: set[str]):
        self.valid_facilities = valid_facilities
        self.valid_financial_classes = valid_financial_classes

    def validate(self, claim: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        errors += validate_facility(claim, self.valid_facilities)
        errors += validate_financial_class(claim, self.valid_financial_classes)
        errors += validate_dob(claim)
        errors += validate_service_dates(claim)
        return errors
