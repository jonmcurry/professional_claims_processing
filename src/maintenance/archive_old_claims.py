import asyncio
import json
import os
from datetime import datetime, timedelta

from ..config.config import load_config
from ..db.postgres import PostgresDatabase
from ..security.compliance import encrypt_claim_fields, encrypt_text

ARCHIVE_PATH = os.getenv("ARCHIVE_PATH", "archive")


async def archive_old_claims(days: int = 1095) -> None:
    """Archive claims older than ``days`` days into a JSONL file."""
    cfg = load_config()
    db = PostgresDatabase(cfg.postgres)
    await db.connect()
    cutoff = datetime.utcnow() - timedelta(days=days)
    rows = await db.fetch("SELECT * FROM claims WHERE service_to_date < $1", cutoff)
    if rows:
        os.makedirs(ARCHIVE_PATH, exist_ok=True)
        fname = os.path.join(ARCHIVE_PATH, f"claims_{cutoff:%Y_%m_%d}.jsonl")
        with open(fname, "w", encoding="utf-8") as f:
            key = cfg.security.encryption_key
            for row in rows:
                record = dict(row)
                if key:
                    record = encrypt_claim_fields(record, key)
                    line = encrypt_text(json.dumps(record, default=str), key)
                else:
                    line = json.dumps(record, default=str)
                f.write(line + "\n")
        await db.execute("DELETE FROM claims WHERE service_to_date < $1", cutoff)
    await db.close()


if __name__ == "__main__":
    asyncio.run(archive_old_claims())
