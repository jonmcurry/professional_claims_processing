import asyncio

from ..config.config import load_config
from .pipeline import ClaimsPipeline


def main() -> None:
    """Entry point for the claims processing worker."""
    cfg = load_config()
    pipeline = ClaimsPipeline(cfg)
    asyncio.run(pipeline.startup())
    asyncio.run(pipeline.process_stream())


if __name__ == "__main__":
    main()
