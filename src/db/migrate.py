"""Simple wrapper around Alembic commands."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

from ..config.config import PostgresConfig


async def apply_migrations(cfg: PostgresConfig) -> None:
    """Run ``alembic upgrade head`` using the given configuration."""
    ini_path = Path(__file__).resolve().parent.parent.parent / "alembic.ini"
    env = dict(os.environ)
    env[
        "DATABASE_URL"
    ] = f"postgresql://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.database}"
    subprocess.run(
        ["alembic", "-c", str(ini_path), "upgrade", "head"], check=True, env=env
    )


if __name__ == "__main__":
    import os

    from ..config.config import load_config

    config_path = os.getenv("APP_CONFIG", "config.yaml")
    cfg = load_config(config_path)
    asyncio.run(apply_migrations(cfg.postgres))
