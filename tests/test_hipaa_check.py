import pytest

from src.config.config import create_default_config
from src.security.hipaa_check import check_hipaa_compliance


def test_compliance_passes():
    cfg = create_default_config()
    check_hipaa_compliance(cfg)


def test_compliance_requires_encryption():
    cfg = create_default_config()
    cfg.security.encryption_key = ""
    with pytest.raises(ValueError):
        check_hipaa_compliance(cfg)


def test_compliance_requires_audit_logging():
    cfg = create_default_config()
    cfg.logging.rotate_mb = 0
    with pytest.raises(ValueError):
        check_hipaa_compliance(cfg)
