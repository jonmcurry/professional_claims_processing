import sys
import types

# Stub durable.lang so tests run without the dependency
sys.modules.setdefault("asyncpg", types.ModuleType("asyncpg"))
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

from src.processing import main


class DummyPipeline:
    def __init__(self, cfg):
        self.started = False
        self.processed = False

    async def startup(self):
        self.started = True

    async def process_stream(self):
        self.processed = True


def test_main_invokes_pipeline(monkeypatch):
    called = DummyPipeline(None)
    monkeypatch.setattr(main, "ClaimsPipeline", lambda cfg: called)
    monkeypatch.setattr(main, "load_config", lambda: None)
    main.main()
    assert called.started
    assert called.processed
