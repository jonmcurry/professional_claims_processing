import pytest

from src.services.claim_service import ClaimService


class DummyPG:
    def __init__(self):
        self.args = None

    async def copy_records(self, table, columns, records):
        self.args = (table, list(columns), list(records))
        return len(self.args[2])


class DummySQL:
    pass


@pytest.mark.asyncio
async def test_bulk_insert_postgres_copy():
    pg = DummyPG()
    service = ClaimService(pg, DummySQL())
    claims = [
        {"patient_account_number": "1", "facility_id": "F1", "procedure_code": "A"},
        {"patient_account_number": "2", "facility_id": "F2", "procedure_code": "B"},
    ]
    inserted = await service.bulk_insert_postgres_copy(claims)
    assert inserted == 2
    assert pg.args[0] == "claims"
    assert pg.args[1] == ["patient_account_number", "facility_id", "procedure_code"]
    assert pg.args[2] == [("1", "F1", "A"), ("2", "F2", "B")]
