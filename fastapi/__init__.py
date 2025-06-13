from .responses import HTMLResponse
import asyncio


class Request:
    """Minimal request object used for tests."""
    def __init__(self, headers=None):
        self.headers = headers or {}


class HTTPException(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail


def Header(default: str | None = None):
    return default

class FastAPI:
    def __init__(self):
        self.routes = {}
        self.startup_handlers = []
        self.middleware_handlers = []

    def on_event(self, name):
        def decorator(func):
            if name == "startup":
                self.startup_handlers.append(func)
            return func
        return decorator

    def get(self, path, response_class=None):
        def decorator(func):
            self.routes[("GET", path)] = func
            return func
        return decorator

    def middleware(self, _):
        def decorator(func):
            self.middleware_handlers.append(func)
            return func
        return decorator

__all__ = ["FastAPI", "HTMLResponse", "HTTPException", "Header", "Request"]

