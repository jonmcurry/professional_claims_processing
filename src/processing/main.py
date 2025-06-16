import asyncio

from ..config.config import load_config
from .pipeline import ClaimsPipeline


async def _run_pipeline() -> None:
    """Run the pipeline startup and streaming loop in a single event loop."""
    cfg = load_config()
    pipeline = ClaimsPipeline(cfg)
    await pipeline.startup()
    try:
        await pipeline.process_stream()
    finally:
        if hasattr(pipeline, "shutdown"):
            await pipeline.shutdown()


def main() -> None:
    """Entry point for the claims processing worker."""
    asyncio.run(_run_pipeline())


if __name__ == "__main__":
    main()
