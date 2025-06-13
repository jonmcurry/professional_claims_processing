from __future__ import annotations
from typing import Any, Dict, Optional


class ModelRegistry:
    """In-memory model registry with simple version resolution."""

    def __init__(self) -> None:
        self._models: Dict[str, str] = {}
        self._active: Optional[str] = None

    def register(self, version: str, path: str, active: bool = False) -> None:
        self._models[version] = path
        if active:
            self._active = version

    def get_path(self, version: Optional[str] = None) -> str:
        ver = version or self._active
        if not ver or ver not in self._models:
            raise ValueError("model version not found")
        return self._models[ver]
