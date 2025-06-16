"""Model utilities and wrappers."""

__all__ = [
    "FilterModel",
    "extract_features",
    "FeaturePipeline",
    "default_feature_pipeline",
    "ABTestModel",
    "ABTestManager",
    "ModelMonitor",
    "ModelRegistry",
]

from importlib import import_module as _import_module

_MODULE_MAP = {
    "FilterModel": "filter_model",
    "extract_features": "features",
    "FeaturePipeline": "features",
    "default_feature_pipeline": "features",
    "ABTestModel": "ab_test",
    "ABTestManager": "ab_test",
    "ModelMonitor": "monitor",
    "ModelRegistry": "registry",
}


def __getattr__(name):
    module_name = _MODULE_MAP.get(name)
    if module_name:
        module = _import_module(f"{__name__}.{module_name}")
        return getattr(module, name)
    raise AttributeError(name)
