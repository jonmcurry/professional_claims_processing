import pytest

from src.monitoring.metrics import metrics
from src.services.claim_service import ClaimService


class DummyPG:
    pass


class DummySQL:
    def __init__(self):
        self.args = None
        self.fail = False
        self.executed = []

    async def execute_many_optimized(
        self, query, params_seq, *, concurrency=1, batch_size=1
    ):
        if self.fail:
            raise Exception("boom")
        self.args = (query, list(params_seq))
        return len(self.args[1])

    async def execute(self, query: str, *params):
        self.executed.append((query, params))
        return 1


@pytest.mark.asyncio
async def test_process_audit_entries_bulk_success():
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)

    entries = [
        ("claims", "1", "insert", None, None, {"a": 1}, None),
        ("claims", "2", "update", None, None, {"b": 2}, None),
    ]

    await service._process_audit_entries(entries)

    assert len(sql.args[1]) == len(entries)


@pytest.mark.asyncio
async def test_process_audit_entries_bulk_error(monkeypatch):
    sql = DummySQL()
    sql.fail = True
    service = ClaimService(DummyPG(), sql)

    entries = [
        ("claims", "1", "insert", "u", None, {"a": 1}, "r"),
        ("claims", "2", "update", None, None, {"b": 2}, None),
    ]

    recorded = []

    async def fake_record(db, table_name, record_id, operation, **kwargs):
        recorded.append(
            (
                table_name,
                record_id,
                operation,
                kwargs.get("user_id"),
                kwargs.get("old_values"),
                kwargs.get("new_values"),
                kwargs.get("reason"),
            )
        )

    monkeypatch.setattr("src.utils.audit.record_audit_event", fake_record)

    await service._process_audit_entries(entries)

    assert recorded == entries


@pytest.mark.asyncio
async def test_failed_claim_assignment_audited():
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)

    recorded = []

    def fake_audit(table, record_id, operation, new_values=None, reason=None):
        recorded.append((table, record_id, operation, new_values, reason))

    service.audit_event = fake_audit

    await service.assign_failed_claim("c1", "user1")

    assert recorded == [("failed_claims", "c1", "assign", {"user": "user1"}, None)]


@pytest.mark.asyncio
async def test_failed_claim_resolution_audited():
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)

    recorded = []

    def fake_audit(table, record_id, operation, new_values=None, reason=None):
        recorded.append((table, record_id, operation, new_values, reason))

    service.audit_event = fake_audit

    metrics.reset("failed_claims_manual")
    await service.resolve_failed_claim("c2", "retry", "ok")
    metrics.reset("failed_claims_manual")

    assert recorded == [
        ("failed_claims", "c2", "resolve", {"action": "retry", "notes": "ok"}, None)
    ]


@pytest.mark.asyncio
async def test_delete_claim_audited():
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)

    recorded = []

    def fake_audit(table, record_id, operation, new_values=None, reason=None):
        recorded.append((table, record_id, operation, new_values, reason))

    service.audit_event = fake_audit

    await service.delete_claim("acc", "F1")

    assert recorded == [("claims", "acc", "delete", None, "compensation")]
