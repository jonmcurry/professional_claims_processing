import asyncio

from ..analysis.index_recommender import recommend_indexes
from ..config.config import load_config
from ..db.postgres import PostgresDatabase


async def auto_index_and_update_stats() -> None:
    cfg = load_config()
    db = PostgresDatabase(cfg.postgres)
    await db.connect()
    suggestions = await recommend_indexes(db)
    for stmt in suggestions:
        try:
            await db.execute(stmt)
        except Exception:
            continue
    await db.execute("ANALYZE claims")
    await db.execute("ANALYZE failed_claims")
    await db.close()


if __name__ == "__main__":
    asyncio.run(auto_index_and_update_stats())
