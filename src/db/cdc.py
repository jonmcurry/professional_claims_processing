from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from .postgres import PostgresDatabase


class ChangeDataCapture:
    """Simple CDC poller using an incrementing ID column."""

    def __init__(self, db: PostgresDatabase, table: str, *, id_column: str = "id") -> None:
        self.db = db
        self.table = table
        self.id_column = id_column
        self._last_id = 0
        self._poll_interval = 1.0

    async def poll(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            rows = await self.db.fetch(
                f"SELECT * FROM {self.table} WHERE {self.id_column} > $1 ORDER BY {self.id_column}",
                self._last_id,
            )
            for row in rows:
                self._last_id = max(self._last_id, row[self.id_column])
                yield row
            await asyncio.sleep(self._poll_interval)

