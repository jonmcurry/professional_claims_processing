import subprocess
from types import SimpleNamespace

import pytest

from src.maintenance import backup_restore


@pytest.mark.asyncio
async def test_backup_restore_encrypted(tmp_path, monkeypatch):
    data_file = tmp_path / "data.sql"
    data_file.write_text("SELECT 1;\n")

    cfg = SimpleNamespace(
        postgres=SimpleNamespace(host="localhost", port=5432, user="u", database="db"),
        security=SimpleNamespace(encryption_key="testkey"),
    )
    monkeypatch.setattr(backup_restore, "load_config", lambda: cfg)

    real_popen = subprocess.Popen

    def fake_popen(cmd, *args, **kwargs):
        if cmd and cmd[0] == "pg_dump":
            return real_popen(["cat", str(data_file)], stdout=subprocess.PIPE)
        return real_popen(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    captured = tmp_path / "restored.sql"
    real_run = subprocess.run

    def fake_run(cmd, *args, **kwargs):
        if cmd and cmd[0] == "psql":
            stdin = kwargs.get("stdin")
            if stdin:
                with open(captured, "wb") as f:
                    f.write(stdin.read())
                return subprocess.CompletedProcess(cmd, 0)
        return real_run(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", fake_run)

    backup_file = await backup_restore.backup()
    assert backup_file.exists()

    await backup_restore.restore(backup_file)

    assert captured.read_text() == "SELECT 1;\n"
