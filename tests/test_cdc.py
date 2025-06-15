import pytest

from src.db.cdc import ChangeDataCapture


class DummyDB:
    async def fetch(self, *args, **kwargs):
        return []


def test_invalid_table_name():
    with pytest.raises(ValueError):
        ChangeDataCapture(DummyDB(), "claims;")


def test_invalid_id_column():
    with pytest.raises(ValueError):
        ChangeDataCapture(DummyDB(), "claims", id_column="id;")
