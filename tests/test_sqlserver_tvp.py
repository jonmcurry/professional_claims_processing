import pytest

from src.services.claim_service import ClaimService


class DummyPG:
    pass


class DummySQL:
    def __init__(self):
        self.args = None

    async def bulk_insert_tvp(self, table, columns, rows):
        self.args = (table, list(columns), list(rows))
        return len(self.args[2])


@pytest.mark.asyncio
async def test_bulk_insert_sqlserver_tvp():
    sql = DummySQL()
    service = ClaimService(DummyPG(), sql)
    claims = [
        {"patient_account_number": "1", "facility_id": "F1", "procedure_code": "A"},
        {"patient_account_number": "2", "facility_id": "F2", "procedure_code": "B"},
    ]
    inserted = await service.bulk_insert_sqlserver_tvp(claims)
    assert inserted == 2
    assert sql.args[0] == "claims"
    assert sql.args[1] == ["patient_account_number", "facility_id", "procedure_code"]
    assert sql.args[2] == [("1", "F1", "A"), ("2", "F2", "B")]
