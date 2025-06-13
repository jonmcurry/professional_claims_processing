import logging
import json
import uuid


class JsonFormatter(logging.Formatter):
    """Format log records as JSON for analytics."""

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - formatting only
        standard = logging.makeLogRecord({}).__dict__.keys()
        data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", ""),
        }
        for key, value in record.__dict__.items():
            if key not in standard and key not in data:
                data[key] = value
        return json.dumps(data)


def setup_logging(request_id: str | None = None) -> logging.Logger:
    logger = logging.getLogger("claims_processor")
    if logger.handlers:
        return logger

    stream_handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(request_id)s %(message)s"
    )
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler("audit.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    analytics_handler = logging.FileHandler("analytics.log")
    analytics_handler.setFormatter(JsonFormatter())
    logger.addHandler(analytics_handler)

    logger.setLevel(logging.INFO)
    logger.addFilter(RequestContextFilter(request_id))
    return logger


class RequestContextFilter(logging.Filter):
    def __init__(self, request_id: str | None = None):
        super().__init__()
        self.request_id = request_id or str(uuid.uuid4())

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = self.request_id
        return True

