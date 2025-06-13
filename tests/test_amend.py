import types
import pytest

from src.services.claim_service import ClaimService

class DummySQL:
    def __init__(self):
        self.queries = []

    async def execute(self, query: str, *params):
        self.queries.append((query, params))
        return 1

class DummyPG:
    async def execute(self, query: str, *params):
        return 1

async def noop(*args, **kwargs):
    pass

@pytest.mark.asyncio
async def test_amend_claim(monkeypatch):
    service = ClaimService(DummyPG(), DummySQL())
    monkeypatch.setattr('src.utils.audit.record_audit_event', noop)
    await service.amend_claim('1', {'facility_id': 'F2'})
    assert service.sql.queries[0][0].startswith('UPDATE claims')

