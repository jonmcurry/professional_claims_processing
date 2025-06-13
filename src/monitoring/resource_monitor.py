from __future__ import annotations
import asyncio
from typing import Optional

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psutil = None

from .metrics import metrics

_task: Optional[asyncio.Task] = None


async def _collect(interval: float) -> None:
    while True:
        if psutil:
            metrics.set("cpu_usage_percent", float(psutil.cpu_percent()))
            mem = psutil.virtual_memory().used / (1024 * 1024)
            metrics.set("memory_usage_mb", float(mem))
        await asyncio.sleep(interval)


def start(interval: float = 1.0) -> None:
    """Start background resource monitoring."""
    global _task
    if _task:
        return
    loop = asyncio.get_event_loop()
    _task = loop.create_task(_collect(interval))


def stop() -> None:
    """Stop background resource monitoring."""
    global _task
    if _task:
        _task.cancel()
        _task = None
