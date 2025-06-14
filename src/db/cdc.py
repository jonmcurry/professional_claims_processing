from __future__ import annotations

import asyncio
import re
import time
from typing import Any, AsyncIterator

from .postgres import PostgresDatabase
from ..web.status import sync_status


class ChangeDataCapture:
    """Simple CDC poller using an incrementing ID column."""

    _VALID_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, db: PostgresDatabase, table: str, *, id_column: str = "id") -> None:
        self.db = db
        self.table = self._validate_name(table, "table")
        self.id_column = self._validate_name(id_column, "id column")
        self._last_id = 0
        self._poll_interval = 1.0

    @classmethod
    def _validate_name(cls, name: str, label: str) -> str:
        if not cls._VALID_NAME.fullmatch(name):
            raise ValueError(f"Invalid {label}: {name}")
        return name

    async def poll(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            rows = await self.db.fetch(
                f"SELECT * FROM {self.table} WHERE {self.id_column} > $1 ORDER BY {self.id_column}",
                self._last_id,
            )
            sync_status["last_polled_at"] = time.time()
            for row in rows:
                self._last_id = max(self._last_id, row[self.id_column])
                sync_status["last_id"] = self._last_id
                yield row
            await asyncio.sleep(self._poll_interval)

