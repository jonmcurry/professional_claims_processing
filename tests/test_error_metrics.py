import pytest

from src.monitoring.metrics import metrics
from src.services.claim_service import ClaimService


class DummySQL:
    async def execute(self, query: str, *params):
        return 1


class DummyPG:
    async def execute(self, query: str, *params):
        return 1


async def noop(*args, **kwargs):
    pass


@pytest.mark.asyncio
async def test_error_metric_increment(monkeypatch):
    service = ClaimService(DummyPG(), DummySQL())
    monkeypatch.setattr("src.utils.audit.record_audit_event", noop)
    metrics.set("errors_validation", 0)
    await service.record_failed_claim(
        {"claim_id": "1"}, "bad", "fix", category="validation"
    )
    assert metrics.get("errors_validation") == 1
