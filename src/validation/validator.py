import asyncio
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

    async def validate(self, claim: Dict[str, Any]) -> List[str]:
        """Validate a claim using multiple rules concurrently."""
        results = await asyncio.gather(
            validate_facility(claim, self.valid_facilities),
            validate_financial_class(claim, self.valid_financial_classes),
            validate_dob(claim),
            validate_service_dates(claim),
            validate_line_item_dates(claim),
        )
        errors: List[str] = []
        for r in results:
            errors += r
        return errors
