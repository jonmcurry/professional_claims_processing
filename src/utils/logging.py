import json
import logging
import logging.handlers
import time
from pathlib import Path
from typing import Dict, Optional

try:  # pragma: no cover - optional dependency
    from sentry_sdk import init as sentry_init
    from sentry_sdk.integrations.logging import LoggingIntegration
except Exception:  # pragma: no cover - optional dependency
    sentry_init = None
    LoggingIntegration = None

from ..config.config import LoggingConfig
from ..monitoring.metrics import metrics
from ..security.compliance import mask_claim_data
from .tracing import correlation_id_var, span_id_var, trace_id_var


class JsonFormatter(logging.Formatter):
    """Format log records as JSON for analytics."""

    def format(
        self, record: logging.LogRecord
    ) -> str:  # pragma: no cover - formatting only
        standard = logging.makeLogRecord({}).__dict__.keys()
        data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", ""),
            "span_id": getattr(record, "span_id", ""),
            "correlation_id": getattr(record, "correlation_id", ""),
        }
        for key, value in record.__dict__.items():
            if key not in standard and key not in data:
                data[key] = value
        return json.dumps(data)


class SensitiveDataFilter(logging.Filter):
    """Mask sensitive claim data from logs."""

    def filter(
        self, record: logging.LogRecord
    ) -> bool:  # pragma: no cover - simple masking
        claim = getattr(record, "claim", None)
        if isinstance(claim, dict):
            record.claim = mask_claim_data(claim)
        return True


class MetricsHandler(logging.Handler):
    """Wrapper handler that records logging overhead."""

    def __init__(self, handler: logging.Handler) -> None:
        super().__init__(handler.level)
        self.handler = handler
        self.setFormatter(handler.formatter)

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - timing only
        start = time.perf_counter()
        self.handler.emit(record)
        metrics.inc("logging_overhead_ms", (time.perf_counter() - start) * 1000)


def setup_logging(cfg: Optional[LoggingConfig] = None) -> logging.Logger:
    logger = logging.getLogger("claims_processor")
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    if logger.handlers:
        return logger

    if cfg and cfg.sentry_dsn and sentry_init and LoggingIntegration:
        sentry_logging = LoggingIntegration(
            level=logging.INFO, event_level=logging.ERROR
        )
        sentry_init(dsn=cfg.sentry_dsn, integrations=[sentry_logging])

    stream_handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(request_id)s %(span_id)s %(message)s"
    )
    stream_handler.setFormatter(fmt)
    logger.addHandler(MetricsHandler(stream_handler))

    file_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "audit.log",
        maxBytes=(cfg.rotate_mb * 1024 * 1024 if cfg else 10 * 1024 * 1024),
        backupCount=(cfg.backup_count if cfg else 5),
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(MetricsHandler(file_handler))

    analytics_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "analytics.log",
        maxBytes=(cfg.rotate_mb * 1024 * 1024 if cfg else 10 * 1024 * 1024),
        backupCount=(cfg.backup_count if cfg else 5),
    )
    analytics_handler.setFormatter(JsonFormatter())
    logger.addHandler(MetricsHandler(analytics_handler))

    if cfg and cfg.aggregator_host and cfg.aggregator_port:
        socket_handler = logging.handlers.SocketHandler(
            cfg.aggregator_host, cfg.aggregator_port
        )
        socket_handler.setFormatter(JsonFormatter())
        logger.addHandler(MetricsHandler(socket_handler))

    logger.setLevel(getattr(logging, (cfg.level if cfg else "INFO")))
    if cfg and cfg.component_levels:
        for name, level in cfg.component_levels.items():
            logging.getLogger(name).setLevel(getattr(logging, level))

    logger.addFilter(RequestContextFilter())
    logger.addFilter(SensitiveDataFilter())
    return logger


class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = trace_id_var.get("")
        record.span_id = span_id_var.get("")
        record.correlation_id = correlation_id_var.get("")
        return True
