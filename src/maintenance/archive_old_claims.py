import asyncio
import json
import os
from datetime import datetime, timedelta

from ..config.config import load_config
from ..db.postgres import PostgresDatabase

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
            for row in rows:
                f.write(json.dumps(row, default=str) + "\n")
        await db.execute("DELETE FROM claims WHERE service_to_date < $1", cutoff)
    await db.close()


if __name__ == "__main__":
    asyncio.run(archive_old_claims())
