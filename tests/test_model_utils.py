from src.models.ab_test import ABTestModel
from src.models.features import FeaturePipeline, extract_features
from src.models.filter_model import FilterModel
from src.models.monitor import ModelMonitor
from src.monitoring.metrics import metrics


class DummyModel:
    def __init__(self, value: int):
        self.value = value

    def predict(self, claim):
        return self.value

    def predict_batch(self, claims):
        return [self.value for _ in claims]


def test_extract_features():
    claim = {"procedure_code": "ABC", "financial_class": "X", "primary_diagnosis": "D"}
    feats = extract_features(claim)
    assert feats["procedure_code_len"] == 3
    assert feats["has_diagnosis"] == 1.0


def test_ab_test_model(monkeypatch):
    model_a = DummyModel(1)
    model_b = DummyModel(2)
    ab = ABTestModel(model_a, model_b, ratio=0.5)
    monkeypatch.setattr("random.random", lambda: 0.4)
    assert ab.predict({}) == 1
    monkeypatch.setattr("random.random", lambda: 0.6)
    assert ab.predict({}) == 2


def test_model_monitor():
    monitor = ModelMonitor("v1")
    metrics.set("model_v1_pred_1", 0)
    metrics.set("model_v1_total", 0)
    monitor.record_prediction(1)
    assert metrics.get("model_v1_pred_1") == 1
    assert metrics.get("model_v1_total") == 1


def test_feature_pipeline_batch():
    pipeline = FeaturePipeline([extract_features])
    claims = [
        {"procedure_code": "A"},
        {"procedure_code": "AB"},
    ]
    res = pipeline.run_batch(claims, max_workers=2)
    assert [f["procedure_code_len"] for f in res] == [1.0, 2.0]


def test_filter_model_predict_batch(monkeypatch):
    pipeline = FeaturePipeline([extract_features])

    class DummyModelImpl:
        def predict(self, vecs):
            return [int(v[2]) for v in vecs]

    import joblib

    monkeypatch.setattr(joblib, "load", lambda path: DummyModelImpl(), raising=False)
    model = FilterModel("dummy.joblib", feature_pipeline=pipeline)
    claims = [
        {"procedure_code": "A"},
        {"procedure_code": "AB"},
    ]
    preds = model.predict_batch(claims)
    assert preds == [1, 2]
