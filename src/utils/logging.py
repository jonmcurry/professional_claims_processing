import logging
import uuid


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("claims_processor")
    handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(request_id)s %(message)s"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


class RequestContextFilter(logging.Filter):
    def __init__(self, request_id: str | None = None):
        super().__init__()
        self.request_id = request_id or str(uuid.uuid4())

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = self.request_id
        return True
