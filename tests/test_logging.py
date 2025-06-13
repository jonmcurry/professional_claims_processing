import logging
from src.utils.logging import setup_logging
from src.config.config import LoggingConfig


def test_sensitive_data_filter(caplog):
    cfg = LoggingConfig()
    logger = setup_logging(cfg)
    with caplog.at_level(logging.INFO):
        logger.info("test", extra={"claim": {"patient_name": "Jane"}})
    assert caplog.records
    record = caplog.records[0]
    assert record.claim["patient_name"] == "***"
