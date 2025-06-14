from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psutil = None

from collections import deque
from .metrics import metrics
from ..analysis.trending import TrendingTracker
from ..analysis.capacity import predict_resource_usage, predict_throughput
from ..alerting import AlertManager

_task: Optional[asyncio.Task] = None
_alert_manager: Optional[AlertManager] = None
_trending = TrendingTracker(window=60)
_cpu_history: deque[float] = deque(maxlen=60)
_mem_history: deque[float] = deque(maxlen=60)
_throughput_history: deque[float] = deque(maxlen=60)


async def _collect(interval: float, log_interval: float) -> None:
    logger = logging.getLogger("claims_processor")
    last_log = time.time()
    while True:
        if psutil:
            cpu = float(psutil.cpu_percent())
            metrics.set("cpu_usage_percent", cpu)
            mem_info = psutil.virtual_memory()
            mem = mem_info.used / (1024 * 1024)
            metrics.set("memory_usage_mb", float(mem))
            metrics.set("memory_usage_percent", float(mem_info.percent))
            _cpu_history.append(cpu)
            _mem_history.append(mem)
            metrics.set(
                "cpu_capacity_forecast",
                predict_resource_usage(list(_cpu_history)),
            )
            metrics.set(
                "memory_capacity_forecast",
                predict_resource_usage(list(_mem_history)),
            )
            _trending.record("cpu", cpu)
            _trending.record("mem", mem)
            metrics.set("cpu_usage_avg", _trending.moving_average("cpu"))
            metrics.set("memory_usage_avg", _trending.moving_average("mem"))
            metrics.set("cpu_usage_trend", _trending.trend("cpu"))
            metrics.set("memory_usage_trend", _trending.trend("mem"))
            now = time.time()
            if now - last_log >= log_interval:
                logger.info(
                    "CPU usage: %.2f%%, Memory usage: %.2f MB", cpu, mem
                )
                last_log = now
        tp = metrics.get("batch_processing_rate_per_sec")
        if tp:
            _throughput_history.append(tp)
            metrics.set(
                "throughput_forecast",
                predict_throughput(list(_throughput_history)),
            )
        if _alert_manager:
            _alert_manager.evaluate()
        await asyncio.sleep(interval)


def start(
    interval: float = 1.0,
    log_interval: float = 60.0,
    alert_manager: Optional[AlertManager] = None,
) -> None:
    """Start background resource monitoring."""
    global _task, _alert_manager
    if _task:
        return
    _alert_manager = alert_manager
    loop = asyncio.get_event_loop()
    _task = loop.create_task(_collect(interval, log_interval))


def stop() -> None:
    """Stop background resource monitoring."""
    global _task, _alert_manager
    if _task:
        _task.cancel()
        _task = None
    _alert_manager = None
