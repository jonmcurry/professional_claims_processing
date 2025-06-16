import asyncio
import subprocess
from pathlib import Path

from ..config.config import load_config


async def backup() -> Path:
    cfg = load_config()
    key = cfg.security.encryption_key
    outfile = Path(f"backup_{cfg.postgres.database}.sql.gpg")

    pg_cmd = [
        "pg_dump",
        f"--host={cfg.postgres.host}",
        f"--port={cfg.postgres.port}",
        f"--username={cfg.postgres.user}",
        cfg.postgres.database,
    ]

    gpg_cmd = [
        "gpg",
        "--batch",
        "--yes",
        "--passphrase",
        key,
        "--symmetric",
        "--cipher-algo",
        "AES256",
        "-o",
        str(outfile),
    ]

    dump_proc = subprocess.Popen(pg_cmd, stdout=subprocess.PIPE)
    try:
        subprocess.run(gpg_cmd, stdin=dump_proc.stdout, check=True)
    finally:
        if dump_proc.stdout:
            dump_proc.stdout.close()
        dump_proc.wait()

    return outfile


async def restore(path: Path) -> None:
    cfg = load_config()
    key = cfg.security.encryption_key

    gpg_cmd = [
        "gpg",
        "--batch",
        "--yes",
        "--passphrase",
        key,
        "-d",
        str(path),
    ]

    psql_cmd = [
        "psql",
        f"--host={cfg.postgres.host}",
        f"--port={cfg.postgres.port}",
        f"--username={cfg.postgres.user}",
        cfg.postgres.database,
    ]

    decrypt_proc = subprocess.Popen(gpg_cmd, stdout=subprocess.PIPE)
    try:
        subprocess.run(psql_cmd, stdin=decrypt_proc.stdout, check=True)
    finally:
        if decrypt_proc.stdout:
            decrypt_proc.stdout.close()
        decrypt_proc.wait()


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
