import pytest

from src.monitoring.metrics import metrics
from src.services.claim_service import ClaimService


class DummySQL:
    def __init__(self):
        self.queries = []

    async def execute(self, query: str, *params):
        self.queries.append((query, params))
        return 1

    async def fetch(self, query: str, *params):
        return []


class DummyPG:
    async def execute(self, query: str, *params):
        return 1

    async def fetch(self, query: str, *params):
        return []


async def noop(*args, **kwargs):
    pass


@pytest.mark.asyncio
async def test_retry_queue_priority(monkeypatch):
    service = ClaimService(DummyPG(), DummySQL())
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    claim_low = {
        "claim_id": "1",
        "patient_account_number": "1",
        "facility_id": "F1",
        "priority": 1,
    }
    claim_hi = {
        "claim_id": "2",
        "patient_account_number": "2",
        "facility_id": "F2",
        "priority": 5,
    }
    await service.record_failed_claim(claim_low, "bad", "fix")
    await service.record_failed_claim(claim_hi, "bad", "fix")
    popped = service.retry_queue.pop()
    assert popped["claim_id"] == "2"


@pytest.mark.asyncio
async def test_manual_resolution(monkeypatch):
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    metrics.reset("failed_claims_manual")
    await service.assign_failed_claim("x", "user1")
    await service.resolve_failed_claim("x", "retry", "ok")
    assert any("UPDATE failed_claims SET assigned_to" in q[0] for q in sql.queries)
    assert metrics.get("failed_claims_manual") == 1
    metrics.reset("failed_claims_manual")
