import uuid
from contextlib import contextmanager
from contextvars import ContextVar

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")
span_id_var: ContextVar[str] = ContextVar("span_id", default="")
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def start_trace(trace_id: str | None = None) -> str:
    """Start a new trace and set the trace_id context variable."""
    trace = trace_id or str(uuid.uuid4())
    trace_id_var.set(trace)
    return trace


def start_trace_from_traceparent(header: str | None) -> str:
    """Parse a W3C traceparent header and start a new trace."""
    if header:
        try:
            parts = header.split("-")
            if len(parts) >= 3:
                trace_id = parts[1]
                span_id = parts[2]
                trace_id_var.set(trace_id)
                span_id_var.set(span_id)
                return trace_id
        except Exception:
            pass
    return start_trace()


@contextmanager
def start_span(span_id: str | None = None):
    """Context manager to set a span_id for the duration of the block."""
    token = span_id_var.set(span_id or str(uuid.uuid4()))
    try:
        yield
    finally:
        span_id_var.reset(token)


def get_traceparent() -> str:
    """Return the current W3C traceparent header value."""
    trace_id = trace_id_var.get("")
    span_id = span_id_var.get("")
    if trace_id and span_id:
        return f"00-{trace_id}-{span_id}-01"
    return ""
