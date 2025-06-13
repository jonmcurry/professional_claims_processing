from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from .validators import (validate_dob, validate_facility,
                         validate_financial_class, validate_line_item_dates,
                         validate_service_dates)


class ClaimValidator:
    def __init__(
        self, valid_facilities: set[str], valid_financial_classes: set[str]
    ):
        self.valid_facilities = valid_facilities
        self.valid_financial_classes = valid_financial_classes

    def validate(self, claim: Dict[str, Any]) -> List[str]:
        """Validate a claim using multiple rules in parallel."""
        errors: List[str] = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    validate_facility, claim, self.valid_facilities
                ),
                executor.submit(
                    validate_financial_class,
                    claim,
                    self.valid_financial_classes,
                ),
                executor.submit(validate_dob, claim),
                executor.submit(validate_service_dates, claim),
                executor.submit(validate_line_item_dates, claim),
            ]
            for f in futures:
                errors += f.result()
        return errors
