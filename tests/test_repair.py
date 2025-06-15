import sys
import types

sys.modules.setdefault("joblib", types.ModuleType("joblib"))
durable_module = types.ModuleType("durable")
durable_lang = types.ModuleType("lang")
durable_lang.ruleset = lambda name: (lambda func: func)
durable_lang.when_all = lambda cond: (lambda func: func)
durable_lang.m = object()
durable_lang.post = lambda name, data: None
setattr(durable_module, "lang", durable_lang)
sys.modules.setdefault("durable", durable_module)
sys.modules.setdefault("durable.lang", durable_lang)

from src.processing.repair import ClaimRepairSuggester, MLRepairAdvisor


def test_repair_suggestions():
    suggester = ClaimRepairSuggester()
    result = suggester.suggest(
        [
            "invalid_facility",
            "invalid_service_dates",
        ],
        {"claim_id": "1"},
    )
    assert "Verify facility_id" in result
    assert "Correct service dates" in result


def test_repair_suggestions_with_ml(monkeypatch):
    class DummyModel:
        def predict(self, payload):
            return ["ML fix"]

    import joblib

    monkeypatch.setattr(joblib, "load", lambda path: DummyModel(), raising=False)
    import os

    monkeypatch.setattr(os.path, "exists", lambda p: True)
    advisor = MLRepairAdvisor("dummy.joblib")
    suggester = ClaimRepairSuggester(advisor)
    res = suggester.suggest(["invalid_dob"], {"claim_id": "1"})
    assert "Check date_of_birth" in res
    assert "ML fix" in res
