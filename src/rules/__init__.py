from .engine import Rule

try:  # pragma: no cover - durable_rules optional
    from .durable_engine import DurableRulesEngine
except Exception:  # pragma: no cover - fallback simple engine
    DurableRulesEngine = None  # type: ignore

