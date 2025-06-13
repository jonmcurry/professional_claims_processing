import asyncio
from . import HTMLResponse

class Response:
    def __init__(self, status_code=200, json=None, content=None):
        self.status_code = status_code
        self._json = json
        self.text = content or ""

    def json(self):
        return self._json

class TestClient:
    def __init__(self, app):
        self.app = app
        self.loop = asyncio.get_event_loop()

    def __enter__(self):
        for func in self.app.startup_handlers:
            if asyncio.iscoroutinefunction(func):
                self.loop.run_until_complete(func())
            else:
                func()
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, path):
        func = self.app.routes.get(("GET", path))
        if not func:
            return Response(status_code=404, content="Not Found")
        if asyncio.iscoroutinefunction(func):
            result = self.loop.run_until_complete(func())
        else:
            result = func()
        if isinstance(result, HTMLResponse):
            return Response(content=str(result))
        return Response(json=result)

