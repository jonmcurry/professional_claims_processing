import asyncio
from datetime import datetime

from ..config.config import load_config
from ..db.postgres import PostgresDatabase


async def manage_claims_partitions(retention_years: int = 5) -> None:
    """Create missing yearly partitions and drop old ones."""
    cfg = load_config()
    db = PostgresDatabase(cfg.postgres)
    await db.connect()
    current = datetime.utcnow().year
    keep_start = current - retention_years
    # create partitions for recent years
    for yr in range(keep_start, current + 1):
        start = f"{yr}-01-01"
        end = f"{yr + 1}-01-01"
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS claims_{yr} PARTITION OF claims
            FOR VALUES FROM ($1) TO ($2)
            """,
            start,
            end,
        )
    # drop older partitions
    for yr in range(2000, keep_start):
        await db.execute(f"DROP TABLE IF EXISTS claims_{yr}")
    await db.close()


if __name__ == "__main__":
    asyncio.run(manage_claims_partitions())
