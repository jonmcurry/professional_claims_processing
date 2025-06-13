import logging
import uuid


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("claims_processor")
    stream_handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(request_id)s %(message)s"
    )
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler("audit.log")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger


class RequestContextFilter(logging.Filter):
    def __init__(self, request_id: str | None = None):
        super().__init__()
        self.request_id = request_id or str(uuid.uuid4())

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = self.request_id
        return True

