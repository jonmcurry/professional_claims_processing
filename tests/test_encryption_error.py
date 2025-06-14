import importlib
import sys

import pytest


def test_missing_cryptography_raises(monkeypatch):
    monkeypatch.dict(sys.modules, {"cryptography": None})
    sys.modules.pop("src.security.compliance", None)
    with pytest.raises(ImportError):
        importlib.import_module("src.security.compliance")
    # reload module for other tests
    importlib.import_module("src.security.compliance")
