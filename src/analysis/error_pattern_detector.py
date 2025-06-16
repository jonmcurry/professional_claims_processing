from __future__ import annotations

import logging
import time
from typing import Dict

from ..db.sql_server import SQLServerDatabase
from .failure_patterns import failure_reason_counts


class ErrorPatternDetector:
    """Monitor failed claims for recurring error patterns."""

    def __init__(
        self,
        db: SQLServerDatabase,
        threshold: int = 20,
        check_interval: int = 300,
    ) -> None:
        self.db = db
        self.threshold = threshold
        self.check_interval = check_interval
        self.last_check = 0.0
        self.logger = logging.getLogger("claims_processor")
        self.remediations: Dict[str, str] = {}

    async def maybe_check(self) -> None:
        """Query failure counts and emit warnings if thresholds are exceeded."""
        if time.time() - self.last_check < self.check_interval:
            return
        self.last_check = time.time()
        counts = await failure_reason_counts(self.db)
        for reason, count in counts.items():
            if count >= self.threshold:
                remediation = self._remediation_steps(reason)
                self.remediations[reason] = remediation
                self.logger.warning(
                    "Failure pattern detected",
                    extra={
                        "event": "failure_pattern_detected",
                        "reason": reason,
                        "count": count,
                        "remediation": remediation,
                    },
                )

    def _remediation_steps(self, reason: str) -> str:
        """Return basic remediation guidance for a failure reason."""
        table = {
            "invalid_facility": "Verify facility_id mappings in source data",
            "invalid_financial_class": "Check financial_class values",
            "invalid_dob": "Ensure patient date_of_birth is valid",
        }
        return table.get(reason, "Investigate data quality issues")
