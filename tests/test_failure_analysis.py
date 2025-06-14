import pytest

from src.analysis.revenue import calculate_potential_revenue_loss, revenue_loss_by_category
from src.analysis.failure_patterns import failure_reason_counts, top_failure_reasons
from src.analysis.failure_predictor import FailurePredictor
from src.analysis.trending import TrendingTracker


class DummySQL:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, query: str, *params):
        return self.rows


@pytest.mark.asyncio
async def test_revenue_loss():
    db = DummySQL([{"total": 100.0}])
    total = await calculate_potential_revenue_loss(db)
    assert total == 100.0


@pytest.mark.asyncio
async def test_revenue_loss_by_category():
    db = DummySQL([
        {"failure_category": "A", "total": 50},
        {"failure_category": "B", "total": 30},
    ])
    result = await revenue_loss_by_category(db)
    assert result["A"] == 50
    assert result["B"] == 30


@pytest.mark.asyncio
async def test_failure_pattern_counts():
    rows = [
        {"failure_reason": "x"},
        {"failure_reason": "y"},
        {"failure_reason": "x"},
    ]
    db = DummySQL(rows)
    counts = await failure_reason_counts(db)
    assert counts["x"] == 2
    assert counts["y"] == 1
    top = await top_failure_reasons(db, limit=1)
    assert top[0] == ("x", 2)


def test_failure_predictor():
    claims = [{"procedure_code": "A"}, {"procedure_code": "B"}]
    labels = [1, 0]
    predictor = FailurePredictor()
    predictor.train(claims, labels)
    prob = predictor.predict_proba({"procedure_code": "A"})
    assert 0.0 <= prob <= 1.0


def test_trending_tracker():
    tracker = TrendingTracker(window=3)
    tracker.record("x", 1)
    tracker.record("x", 2)
    tracker.record("x", 3)
    assert tracker.moving_average("x") == 2
    assert tracker.trend("x") == 2
