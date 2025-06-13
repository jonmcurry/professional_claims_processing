import asyncio
from ..config.config import load_config
from .pipeline import ClaimsPipeline


def main() -> None:
    cfg = load_config()
    pipeline = ClaimsPipeline(cfg)
    asyncio.run(pipeline.startup())
    asyncio.run(pipeline.process_stream())


if __name__ == "__main__":
    main()
