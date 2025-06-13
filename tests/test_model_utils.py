from src.models.features import extract_features
from src.models.ab_test import ABTestModel
from src.models.monitor import ModelMonitor
from src.monitoring.metrics import metrics

class DummyModel:
    def __init__(self, value: int):
        self.value = value

    def predict(self, claim):
        return self.value

def test_extract_features():
    claim = {"procedure_code": "ABC", "financial_class": "X", "primary_diagnosis": "D"}
    feats = extract_features(claim)
    assert feats["procedure_code_len"] == 3
    assert feats["has_diagnosis"] == 1.0


def test_ab_test_model(monkeypatch):
    model_a = DummyModel(1)
    model_b = DummyModel(2)
    ab = ABTestModel(model_a, model_b, ratio=0.5)
    monkeypatch.setattr('random.random', lambda: 0.4)
    assert ab.predict({}) == 1
    monkeypatch.setattr('random.random', lambda: 0.6)
    assert ab.predict({}) == 2


def test_model_monitor():
    monitor = ModelMonitor("v1")
    metrics.set("model_v1_pred_1", 0)
    metrics.set("model_v1_total", 0)
    monitor.record_prediction(1)
    assert metrics.get("model_v1_pred_1") == 1
    assert metrics.get("model_v1_total") == 1

