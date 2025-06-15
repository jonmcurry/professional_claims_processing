from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, List

from ..config.config import AppConfig
from ..monitoring.metrics import metrics
from .notifier import EmailNotifier


@dataclass
class AlertRule:
    metric: str
    threshold: float
    comparator: Callable[[float, float], bool]
    message: str


class AlertManager:
    """Evaluate alert rules and send notifications."""

    def __init__(self, cfg: AppConfig, notifier: EmailNotifier) -> None:
        self.cfg = cfg
        self.notifier = notifier
        self.recipients = cfg.monitoring.alerts.email_recipients
        self.logger = logging.getLogger("claims_processor")
        self._rules = self._build_rules(cfg)

    def _build_rules(self, cfg: AppConfig) -> List[AlertRule]:
        alerts = cfg.monitoring.alerts
        return [
            AlertRule(
                "cpu_usage_percent",
                alerts.high_cpu_threshold,
                lambda v, t: v > t,
                "CPU usage high",
            ),
            AlertRule(
                "memory_usage_percent",
                alerts.high_memory_threshold,
                lambda v, t: v > t,
                "Memory usage high",
            ),
            AlertRule(
                "batch_processing_rate_per_sec",
                alerts.low_throughput_threshold,
                lambda v, t: v < t,
                "Throughput below threshold",
            ),
        ]

    def evaluate(self) -> None:
        triggered: List[str] = []
        for rule in self._rules:
            value = metrics.get(rule.metric)
            if value and rule.comparator(value, rule.threshold):
                triggered.append(f"{rule.message}: {value:.2f}")

        # Error rate uses processed/failed metrics
        processed = metrics.get("claims_processed")
        failed = metrics.get("claims_failed")
        if processed > 0:
            rate = failed / processed
            if rate > self.cfg.monitoring.alerts.high_error_rate_threshold:
                triggered.append(f"High error rate: {rate:.2%}")

        if triggered and self.recipients:
            body = "\n".join(triggered)
            self.notifier.send(self.recipients, "Claims Processor Alert", body)
            self.logger.warning("Alerts triggered: %s", body)
