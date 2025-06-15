import asyncio
import logging

import pytest

from src.analysis.error_pattern_detector import ErrorPatternDetector


class DummySQL:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, query: str, *params):
        return self.rows


@pytest.mark.asyncio
async def test_detector_emits_warning(caplog):
    rows = [
        {"failure_reason": "invalid_facility"},
        {"failure_reason": "invalid_facility"},
    ]
    db = DummySQL(rows)
    detector = ErrorPatternDetector(db, threshold=2, check_interval=0)
    with caplog.at_level(logging.WARNING):
        await detector.maybe_check()
    assert "invalid_facility" in detector.remediations
    assert any(r.levelno == logging.WARNING for r in caplog.records)


@pytest.mark.asyncio
async def test_detector_no_warning(caplog):
    db = DummySQL([{"failure_reason": "x"}])
    detector = ErrorPatternDetector(db, threshold=2, check_interval=0)
    with caplog.at_level(logging.WARNING):
        await detector.maybe_check()
    assert not caplog.records
