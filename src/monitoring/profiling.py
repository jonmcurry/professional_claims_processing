import cProfile
import pstats
from io import StringIO
from typing import Optional

_profiler: Optional[cProfile.Profile] = None


def start_profiling() -> None:
    """Enable Python profiling globally."""
    global _profiler
    if _profiler is None:
        _profiler = cProfile.Profile()
        _profiler.enable()


def stop_profiling() -> str:
    """Disable profiling and return stats as text."""
    global _profiler
    if not _profiler:
        return ""
    _profiler.disable()
    s = StringIO()
    pstats.Stats(_profiler, stream=s).sort_stats("cumulative").print_stats()
    _profiler = None
    return s.getvalue()
