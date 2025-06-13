import os
import pytest
from types import SimpleNamespace

from src.maintenance.archive_old_claims import archive_old_claims


class DummyDB:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.deleted = False
        self.connected = False

    async def connect(self):
        self.connected = True

    async def close(self):
        pass

    async def fetch(self, query, cutoff):
        return self.rows

    async def execute(self, query, cutoff):
        self.deleted = True
        return 0


@pytest.mark.asyncio
async def test_archive_no_rows(monkeypatch, tmp_path):
    db = DummyDB([])
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.PostgresDatabase", lambda cfg: db
    )
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.load_config",
        lambda: SimpleNamespace(postgres=None),
    )
    monkeypatch.setenv("ARCHIVE_PATH", str(tmp_path))
    await archive_old_claims(days=1)
    assert db.connected
    assert not db.deleted
    assert not os.listdir(tmp_path)


@pytest.mark.asyncio
async def test_archive_with_rows(monkeypatch, tmp_path):
    rows = [{"claim_id": 1, "service_to_date": "2020-01-01"}]
    db = DummyDB(rows)
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.PostgresDatabase", lambda cfg: db
    )
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.load_config",
        lambda: SimpleNamespace(postgres=None),
    )
    monkeypatch.setenv("ARCHIVE_PATH", str(tmp_path))
    await archive_old_claims(days=1)
    assert db.deleted
    files = list(tmp_path.iterdir())
    assert len(files) == 1
    assert "claim_id" in files[0].read_text()
