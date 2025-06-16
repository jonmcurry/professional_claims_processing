"""
Memory Management Improvements for Claims Processing System

Key fixes implemented:
1. Memory pooling for large batches
2. Memory leak prevention in long-running processes
3. Garbage collection optimization
4. Resource cleanup and lifecycle management
"""

import asyncio
# src/utils/memory_pool.py
import gc
import time
import weakref
from collections import deque
from typing import Any, Dict, List, Optional, Set

import psutil


class MemoryPool:
    """Memory pool for reusing objects and preventing fragmentation."""

    def __init__(self, initial_size: int = 1000, max_size: int = 10000):
        self.initial_size = initial_size
        self.max_size = max_size
        self.pools: Dict[str, deque] = {}
        self.stats: Dict[str, Dict[str, int]] = {}
        self._cleanup_refs: Set[weakref.ref] = set()

    def get_pool(self, pool_name: str, factory_func=None) -> deque:
        """Get or create a pool for specific object types."""
        if pool_name not in self.pools:
            self.pools[pool_name] = deque(maxlen=self.max_size)
            self.stats[pool_name] = {"created": 0, "reused": 0, "discarded": 0}

            # Pre-populate pool if factory function provided
            if factory_func and pool_name not in self.pools:
                for _ in range(min(self.initial_size, 100)):  # Limit initial allocation
                    try:
                        obj = factory_func()
                        self.pools[pool_name].append(obj)
                        self.stats[pool_name]["created"] += 1
                    except Exception:
                        break

        return self.pools[pool_name]

    def acquire(self, pool_name: str, factory_func=None) -> Any:
        """Acquire an object from the pool."""
        pool = self.get_pool(pool_name, factory_func)

        if pool:
            obj = pool.popleft()
            self.stats[pool_name]["reused"] += 1
            return obj
        elif factory_func:
            obj = factory_func()
            self.stats[pool_name]["created"] += 1
            return obj
        else:
            return None

    def release(self, pool_name: str, obj: Any) -> None:
        """Return an object to the pool."""
        if pool_name in self.pools:
            pool = self.pools[pool_name]
            if len(pool) < self.max_size:
                # Clean object before returning to pool
                if hasattr(obj, "clear"):
                    obj.clear()
                elif hasattr(obj, "__dict__"):
                    # Reset object state
                    for attr in list(obj.__dict__.keys()):
                        if not attr.startswith("_"):
                            try:
                                delattr(obj, attr)
                            except AttributeError:
                                pass

                pool.append(obj)
            else:
                self.stats[pool_name]["discarded"] += 1

    def register_cleanup(self, obj: Any) -> None:
        """Register an object for automatic cleanup."""

        def cleanup_callback(ref):
            self._cleanup_refs.discard(ref)

        ref = weakref.ref(obj, cleanup_callback)
        self._cleanup_refs.add(ref)

    def cleanup_pool(self, pool_name: str) -> int:
        """Clean up a specific pool and return number of objects removed."""
        if pool_name in self.pools:
            pool = self.pools[pool_name]
            removed = len(pool)
            pool.clear()
            return removed
        return 0

    def cleanup_all(self) -> Dict[str, int]:
        """Clean up all pools."""
        cleanup_stats = {}
        for pool_name in list(self.pools.keys()):
            cleanup_stats[pool_name] = self.cleanup_pool(pool_name)
        return cleanup_stats

    def get_stats(self) -> Dict[str, Any]:
        """Get memory pool statistics."""
        pool_sizes = {name: len(pool) for name, pool in self.pools.items()}
        return {
            "pool_sizes": pool_sizes,
            "stats": self.stats.copy(),
            "total_pools": len(self.pools),
            "cleanup_refs": len(self._cleanup_refs),
        }


# Global memory pool instance
memory_pool = MemoryPool()
