import logging
import json
import logging.handlers
from typing import Optional, Dict

from .tracing import trace_id_var, span_id_var
from ..security.compliance import mask_claim_data
from ..config.config import LoggingConfig


class JsonFormatter(logging.Formatter):
    """Format log records as JSON for analytics."""

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting only
        standard = logging.makeLogRecord({}).__dict__.keys()
        data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", ""),
            "span_id": getattr(record, "span_id", ""),
        }
        for key, value in record.__dict__.items():
            if key not in standard and key not in data:
                data[key] = value
        return json.dumps(data)


class SensitiveDataFilter(logging.Filter):
    """Mask sensitive claim data from logs."""

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - simple masking
        claim = getattr(record, "claim", None)
        if isinstance(claim, dict):
            record.claim = mask_claim_data(claim)
        return True


def setup_logging(cfg: Optional[LoggingConfig] = None) -> logging.Logger:
    logger = logging.getLogger("claims_processor")
    if logger.handlers:
        return logger

    stream_handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(request_id)s %(span_id)s %(message)s"
    )
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler("audit.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    analytics_handler = logging.FileHandler("analytics.log")
    analytics_handler.setFormatter(JsonFormatter())
    logger.addHandler(analytics_handler)

    if cfg and cfg.aggregator_host and cfg.aggregator_port:
        socket_handler = logging.handlers.SocketHandler(
            cfg.aggregator_host, cfg.aggregator_port
        )
        socket_handler.setFormatter(JsonFormatter())
        logger.addHandler(socket_handler)

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
        return True

