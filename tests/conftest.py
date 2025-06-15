import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Provide lightweight stubs for optional third-party modules
if "numpy" not in sys.modules:
    sys.modules["numpy"] = types.ModuleType("numpy")

# Stub pyodbc so tests can import database modules without the actual driver
if "pyodbc" not in sys.modules:
    pyodbc_mod = types.SimpleNamespace(connect=lambda *a, **k: None, Connection=object)
    sys.modules["pyodbc"] = pyodbc_mod

if "yaml" not in sys.modules:
    yaml_mod = types.ModuleType("yaml")
    yaml_mod.safe_load = lambda *a, **k: {}
    yaml_mod.dump = lambda *a, **k: ""
    sys.modules["yaml"] = yaml_mod

if "psutil" not in sys.modules:
    psutil_mod = types.ModuleType("psutil")
    psutil_mod.cpu_percent = lambda *a, **k: 0.0
    psutil_mod.cpu_count = lambda *a, **k: 1

    class _Mem:
        def __init__(self):
            self.percent = 0.0
            self.available = 1024 * 1024 * 1024

    psutil_mod.virtual_memory = lambda: _Mem()

    class _Proc:
        def memory_info(self):
            return types.SimpleNamespace(rss=0)

    psutil_mod.Process = lambda *a, **k: _Proc()
    sys.modules["psutil"] = psutil_mod

# Minimal cryptography stub so tests run without the dependency
if "cryptography" not in sys.modules:
    crypto_mod = types.ModuleType("cryptography")
    fernet_mod = types.ModuleType("fernet")

    import base64

    class _Fernet:
        def __init__(self, key: bytes):
            self._key = key

        def encrypt(self, data: bytes) -> bytes:
            return base64.b64encode(self._key + data)

        def decrypt(self, token: bytes) -> bytes:
            decoded = base64.b64decode(token)
            return decoded[len(self._key) :]

    fernet_mod.Fernet = _Fernet
    crypto_mod.fernet = fernet_mod
    sys.modules["cryptography"] = crypto_mod
    sys.modules["cryptography.fernet"] = fernet_mod
import importlib

if "src.utils.memory" not in sys.modules:
    mp = importlib.import_module("src.utils.memory_pool")
    mem_mod = types.ModuleType("src.utils.memory")
    mem_mod.memory_pool = mp.memory_pool
    sys.modules["src.utils.memory"] = mem_mod
if "src.db.memory_pool" not in sys.modules:
    mp = importlib.import_module("src.utils.memory_pool")
    db_mem_mod = types.ModuleType("src.db.memory_pool")
    db_mem_mod.memory_pool = mp.memory_pool
    sys.modules["src.db.memory_pool"] = db_mem_mod
if "src.memory.memory_pool" not in sys.modules:
    mp = importlib.import_module("src.utils.memory_pool")
    mem_pkg = types.ModuleType("src.memory.memory_pool")
    mem_pkg.sql_memory_pool = mp.memory_pool
    sys.modules["src.memory.memory_pool"] = mem_pkg

# Provide compatibility helper for older monkeypatch API used in tests
import pytest

if not hasattr(pytest.MonkeyPatch, "dict"):

    def _dict(self, mapping, values):
        if mapping is sys.modules and values.get("cryptography") is None:
            mapping.pop("cryptography.fernet", None)
        for k, v in values.items():
            self.setitem(mapping, k, v)

    pytest.MonkeyPatch.dict = _dict
