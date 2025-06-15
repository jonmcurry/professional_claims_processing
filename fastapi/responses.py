class HTMLResponse(str):
    pass


class JSONResponse(dict):
    def __init__(
        self, content: dict, status_code: int = 200, headers: dict | None = None
    ) -> None:
        super().__init__(content)
        self.status_code = status_code
        self.headers = headers or {}
