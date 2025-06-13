import asyncio
from . import HTMLResponse, Request

class Response:
    def __init__(self, status_code=200, json=None, content=None, headers=None):
        self.status_code = status_code
        self._json = json
        self.text = content or ""
        self.headers = headers or {}

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

    def get(self, path, headers=None):
        func = self.app.routes.get(("GET", path))
        if not func:
            return Response(status_code=404, content="Not Found")
        kwargs = {}
        if headers and "X-API-Key" in headers:
            kwargs["x_api_key"] = headers["X-API-Key"]
        request = Request(headers)
        if asyncio.iscoroutinefunction(func):
            result = self.loop.run_until_complete(func(**kwargs))
        else:
            result = func(**kwargs)
        if isinstance(result, HTMLResponse):
            response = Response(content=str(result), headers={})
        elif isinstance(result, Response):
            response = result
        else:
            response = Response(json=result, headers={})

        async def call_next(_):
            return response

        for mw in reversed(self.app.middleware_handlers):
            response = self.loop.run_until_complete(mw(request, call_next))
            async def call_next(_):
                return response

        return response

