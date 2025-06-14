import pytest

from src.services.claim_service import ClaimService


class DummyPG:
    pass


class DummySQL:
    def __init__(self):
        self.args = None
        self.fail = False

    async def execute_many_optimized(self, query, params_seq, *, concurrency=1, batch_size=1):
        if self.fail:
            raise Exception("boom")
        self.args = (query, list(params_seq))
        return len(self.args[1])


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
