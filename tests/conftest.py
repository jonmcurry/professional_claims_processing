import sys, types, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Provide lightweight stubs for optional third-party modules
if 'numpy' not in sys.modules:
    sys.modules['numpy'] = types.ModuleType('numpy')

if 'yaml' not in sys.modules:
    yaml_mod = types.ModuleType('yaml')
    yaml_mod.safe_load = lambda *a, **k: {}
    yaml_mod.dump = lambda *a, **k: ''
    sys.modules['yaml'] = yaml_mod

if 'psutil' not in sys.modules:
    psutil_mod = types.ModuleType('psutil')
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
    sys.modules['psutil'] = psutil_mod
import importlib
if 'src.utils.memory' not in sys.modules:
    mp = importlib.import_module('src.utils.memory_pool')
    mem_mod = types.ModuleType('src.utils.memory')
    mem_mod.memory_pool = mp.memory_pool
    sys.modules['src.utils.memory'] = mem_mod
if 'src.db.memory_pool' not in sys.modules:
    mp = importlib.import_module('src.utils.memory_pool')
    db_mem_mod = types.ModuleType('src.db.memory_pool')
    db_mem_mod.memory_pool = mp.memory_pool
    sys.modules['src.db.memory_pool'] = db_mem_mod
if 'src.memory.memory_pool' not in sys.modules:
    mp = importlib.import_module('src.utils.memory_pool')
    mem_pkg = types.ModuleType('src.memory.memory_pool')
    mem_pkg.sql_memory_pool = mp.memory_pool
    sys.modules['src.memory.memory_pool'] = mem_pkg
