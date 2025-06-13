import uuid
from contextvars import ContextVar
from contextlib import contextmanager

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")
span_id_var: ContextVar[str] = ContextVar("span_id", default="")


def start_trace(trace_id: str | None = None) -> str:
    """Start a new trace and set the trace_id context variable."""
    trace = trace_id or str(uuid.uuid4())
    trace_id_var.set(trace)
    return trace


@contextmanager
def start_span(span_id: str | None = None):
    """Context manager to set a span_id for the duration of the block."""
    token = span_id_var.set(span_id or str(uuid.uuid4()))
    try:
        yield
    finally:
        span_id_var.reset(token)
