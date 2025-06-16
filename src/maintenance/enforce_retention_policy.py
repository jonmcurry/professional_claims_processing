import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path

from .archive_old_claims import archive_old_claims

RETENTION_YEARS = int(os.getenv("RETENTION_YEARS", "7"))
ARCHIVE_PATH = Path(os.getenv("ARCHIVE_PATH", "archive"))


async def purge_old_archives() -> None:
    """Delete archive files older than the retention window."""
    cutoff = datetime.utcnow() - timedelta(days=RETENTION_YEARS * 365)
    if not ARCHIVE_PATH.exists():
        return
    for path in ARCHIVE_PATH.glob("claims_*.jsonl"):
        try:
            timestamp = datetime.strptime(path.stem.split("_")[1], "%Y_%m_%d")
        except Exception:
            continue
        if timestamp < cutoff:
            path.unlink(missing_ok=True)


async def enforce() -> None:
    await archive_old_claims()
    await purge_old_archives()


if __name__ == "__main__":
    asyncio.run(enforce())
