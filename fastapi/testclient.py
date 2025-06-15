import asyncio

from . import HTMLResponse, HTTPException, Request


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
        if headers and "X-User-Role" in headers:
            kwargs["x_user_role"] = headers["X-User-Role"]
        request = Request(headers)
        request.url = type("URL", (), {"path": path})()
        request.method = "GET"
        if "request" in func.__code__.co_varnames:
            kwargs["request"] = request
        try:
            if asyncio.iscoroutinefunction(func):
                result = self.loop.run_until_complete(func(**kwargs))
            else:
                result = func(**kwargs)
        except HTTPException as exc:
            result = Response(status_code=exc.status_code, json={"detail": exc.detail})
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

    def post(self, path, json=None, headers=None):
        func = self.app.routes.get(("POST", path))
        if not func:
            return Response(status_code=404, content="Not Found")
        kwargs = json or {}
        if headers and "X-API-Key" in headers:
            kwargs["x_api_key"] = headers["X-API-Key"]
        if headers and "X-User-Role" in headers:
            kwargs["x_user_role"] = headers["X-User-Role"]
        request = Request(headers)
        request.url = type("URL", (), {"path": path})()
        request.method = "POST"
        if "request" in func.__code__.co_varnames:
            kwargs["request"] = request
        try:
            if asyncio.iscoroutinefunction(func):
                result = self.loop.run_until_complete(func(**kwargs))
            else:
                result = func(**kwargs)
        except HTTPException as exc:
            result = Response(status_code=exc.status_code, json={"detail": exc.detail})
        if isinstance(result, Response):
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
