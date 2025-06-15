import logging

from src.config.config import LoggingConfig
from src.utils.logging import setup_logging


def test_sensitive_data_filter(caplog):
    cfg = LoggingConfig()
    logger = setup_logging(cfg)
    with caplog.at_level(logging.INFO):
        logger.info("test", extra={"claim": {"patient_name": "Jane"}})
    assert caplog.records
    record = caplog.records[0]
    assert record.claim["patient_name"] == "***"


def test_logging_overhead_metric(caplog):
    cfg = LoggingConfig()
    logger = setup_logging(cfg)
    from src.monitoring.metrics import metrics

    before = metrics.get("logging_overhead_ms")
    with caplog.at_level(logging.INFO):
        logger.info("metric")
    assert metrics.get("logging_overhead_ms") > before
