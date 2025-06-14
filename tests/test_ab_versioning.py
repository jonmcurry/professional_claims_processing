from src.models.registry import ModelRegistry
from src.models.features import FeaturePipeline, default_feature_pipeline
from src.models.monitor import ModelMonitor
from src.models.ab_test import ABTestManager
from src.monitoring.metrics import metrics

class DummyModel:
    def __init__(self, value, version):
        self.value = value
        self.version = version
    def predict(self, claim):
        return self.value

def test_model_registry_rollback():
    reg = ModelRegistry()
    reg.register("v1", "path1", active=True)
    reg.register("v2", "path2", active=False)
    reg.activate("v2")
    assert reg.current_version() == "v2"
    reg.rollback()
    assert reg.current_version() == "v1"

def test_feature_pipeline():
    def extra_step(claim):
        return {"x": 1.0}
    pipe = FeaturePipeline([default_feature_pipeline.run, extra_step])
    feats = pipe.run({"procedure_code": "A"})
    assert "x" in feats and "procedure_code_len" in feats

def test_model_performance_monitor():
    monitor = ModelMonitor("v1")
    metrics.set("model_v1_total", 0)
    metrics.set("model_v1_correct", 0)
    monitor.record_result(1, 1)
    assert metrics.get("model_v1_accuracy") == 1.0

def test_ab_test_manager(monkeypatch):
    a = DummyModel(1, "a")
    b = DummyModel(2, "b")
    manager = ABTestManager(a, b, ratio=0.5)
    metrics.set("ab_exposure_a", 0)
    metrics.set("ab_exposure_b", 0)
    monkeypatch.setattr('random.random', lambda: 0.3)
    assert manager.predict({}) == 1
    assert metrics.get("ab_exposure_a") == 1
    monkeypatch.setattr('random.random', lambda: 0.8)
    assert manager.predict({}) == 2
    assert metrics.get("ab_exposure_b") == 1
