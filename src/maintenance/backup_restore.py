import asyncio
import subprocess
from pathlib import Path
from typing import Optional

from ..config.config import load_config


async def backup() -> Path:
    cfg = load_config()
    outfile = Path(f"backup_{cfg.postgres.database}.sql")
    subprocess.run(
        [
            "pg_dump",
            f"--host={cfg.postgres.host}",
            f"--port={cfg.postgres.port}",
            f"--username={cfg.postgres.user}",
            "-F",
            "plain",
            "-f",
            str(outfile),
            cfg.postgres.database,
        ],
        check=True,
    )
    return outfile


async def restore(path: Path) -> None:
    cfg = load_config()
    subprocess.run(
        [
            "psql",
            f"--host={cfg.postgres.host}",
            f"--port={cfg.postgres.port}",
            f"--username={cfg.postgres.user}",
            cfg.postgres.database,
            "-f",
            str(path),
        ],
        check=True,
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: backup_restore.py [backup|restore <file>]")
        raise SystemExit(1)
    cmd = sys.argv[1]
    if cmd == "backup":
        asyncio.run(backup())
    elif cmd == "restore" and len(sys.argv) >= 3:
        asyncio.run(restore(Path(sys.argv[2])))
    else:
        print("Invalid arguments")
        raise SystemExit(1)
