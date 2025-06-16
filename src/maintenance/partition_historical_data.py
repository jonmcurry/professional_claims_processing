import asyncio
from datetime import datetime

from ..config.config import load_config
from ..db.postgres import PostgresDatabase


async def create_claims_partitions(years: int = 5) -> None:
    """Create yearly partitions for the claims table."""
    cfg = load_config()
    db = PostgresDatabase(cfg.postgres)
    await db.connect()
    current = datetime.utcnow().year
    for yr in range(current - years, current + 1):
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
    await db.close()


if __name__ == "__main__":
    asyncio.run(create_claims_partitions())
