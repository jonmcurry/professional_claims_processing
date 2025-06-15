import time
from collections import defaultdict, deque
from threading import Lock
from typing import Deque, Dict, Tuple


class MetricsRegistry:
    """Simple in-memory metrics collector."""

    def __init__(self) -> None:
        self._metrics: Dict[str, float] = defaultdict(float)
        self._hourly: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self._lock = Lock()

    def inc(self, name: str, amount: float = 1.0) -> None:
        with self._lock:
            self._metrics[name] += amount

    def set(self, name: str, value: float) -> None:
        with self._lock:
            self._metrics[name] = value

    def record_hourly(self, name: str, amount: float) -> None:
        """Record a value for hourly rate tracking."""
        now = time.time()
        with self._lock:
            series = self._hourly[name]
            series.append((now, amount))
            cutoff = now - 3600
            while series and series[0][0] < cutoff:
                series.popleft()
            total = sum(a for _, a in series)
            self._metrics[f"{name}_per_hour"] = total

    def reset(self, name: str) -> None:
        """Reset metric and associated hourly data."""
        with self._lock:
            self._metrics[name] = 0.0
            if name in self._hourly:
                self._hourly[name].clear()
                self._metrics[f"{name}_per_hour"] = 0.0

    def get(self, name: str) -> float:
        with self._lock:
            return self._metrics.get(name, 0.0)

    def as_dict(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._metrics)


metrics = MetricsRegistry()


class SLAMonitor:
    """Monitor processing time against an SLA threshold."""

    def __init__(self, threshold: float = 2.0) -> None:
        self.threshold = threshold

    def record_batch(self, total_time: float, num_claims: int) -> None:
        if num_claims <= 0:
            return
        avg = total_time / num_claims
        if avg > self.threshold:
            metrics.inc("sla_violations", num_claims)


sla_monitor = SLAMonitor()
