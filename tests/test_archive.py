import json
import os
from types import SimpleNamespace

import pytest

from src.maintenance.archive_old_claims import archive_old_claims
from src.security.compliance import decrypt_text


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
    key = "abcd" * 8
    rows = [
        {
            "claim_id": 1,
            "patient_account_number": "123",
            "service_to_date": "2020-01-01",
        }
    ]
    db = DummyDB(rows)
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.PostgresDatabase", lambda cfg: db
    )
    monkeypatch.setattr(
        "src.maintenance.archive_old_claims.load_config",
        lambda: SimpleNamespace(
            postgres=None, security=SimpleNamespace(encryption_key=key)
        ),
    )
    monkeypatch.setenv("ARCHIVE_PATH", str(tmp_path))
    await archive_old_claims(days=1)
    assert db.deleted
    files = list(tmp_path.iterdir())
    assert len(files) == 1
    line = files[0].read_text().splitlines()[0]
    assert "claim_id" not in line
    decrypted = decrypt_text(line, key)
    data = json.loads(decrypted)
    assert data["patient_account_number"] != "123"
    assert decrypt_text(data["patient_account_number"], key) == "123"
