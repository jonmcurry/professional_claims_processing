from __future__ import annotations

from typing import Any, Dict, Optional


class ModelRegistry:
    """In-memory model registry with version activation and rollback."""

    def __init__(self) -> None:
        self._models: Dict[str, str] = {}
        self._history: list[str] = []

    def register(self, version: str, path: str, active: bool = False) -> None:
        """Register a model version. Optionally activate it."""
        self._models[version] = path
        if active:
            self.activate(version)

    def activate(self, version: str) -> None:
        """Activate a registered model version."""
        if version not in self._models:
            raise ValueError("model version not found")
        if self._history and self._history[-1] == version:
            return
        self._history.append(version)

    def current_version(self) -> Optional[str]:
        return self._history[-1] if self._history else None

    def get_path(self, version: Optional[str] = None) -> str:
        ver = version or self.current_version()
        if not ver or ver not in self._models:
            raise ValueError("model version not found")
        return self._models[ver]

    def rollback(self) -> str:
        """Rollback to the previous model version and return it."""
        if len(self._history) < 2:
            raise ValueError("no previous version to rollback to")
        self._history.pop()
        return self._history[-1]
