import asyncio
import gc
import json
import time
import weakref
from collections import Counter, OrderedDict, deque
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import psutil

from ..monitoring.metrics import metrics
from ..utils.memory import memory_pool

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - redis optional
    aioredis = None


class InMemoryCache:
    """Enhanced in-memory cache with memory management."""

    def __init__(self, ttl: int = 300, max_size: int = 10000):
        self.ttl = ttl
        self.max_size = max_size
        self.store: OrderedDict[str, Tuple[float, Any]] = OrderedDict()
        self._memory_pool = memory_pool
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # Clean every minute

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache with TTL check and LRU update."""
        if key not in self.store:
            return None

        ts, value = self.store[key]
        current_time = time.time()

        if current_time - ts > self.ttl:
            del self.store[key]
            return None

        # Move to end (most recently used)
        self.store.move_to_end(key)

        # Periodic cleanup
        if current_time - self._last_cleanup > self._cleanup_interval:
            self._cleanup_expired()

        return value

    def set(self, key: str, value: Any) -> None:
        """Set item in cache with LRU eviction."""
        current_time = time.time()

        # Add/update item
        self.store[key] = (current_time, value)
        self.store.move_to_end(key)

        # Evict oldest if over capacity
        while len(self.store) > self.max_size:
            self.store.popitem(last=False)

        # Periodic cleanup
        if current_time - self._last_cleanup > self._cleanup_interval:
            self._cleanup_expired()

    def delete(self, key: str) -> None:
        """Delete item from cache."""
        self.store.pop(key, None)

    def _cleanup_expired(self) -> None:
        """Clean up expired entries."""
        current_time = time.time()
        expired_keys = []

        for key, (timestamp, _) in self.store.items():
            if current_time - timestamp > self.ttl:
                expired_keys.append(key)
            else:
                break  # OrderedDict, so rest are newer

        for key in expired_keys:
            del self.store[key]

        self._last_cleanup = current_time

    # New public wrapper so external users can trigger cleanup
    def cleanup_expired(self) -> None:
        self._cleanup_expired()

    # Allow use of len(), keys(), and popitem() on the cache
    def __len__(self) -> int:
        return len(self.store)

    def keys(self) -> Iterable[str]:
        return list(self.store.keys())

    def popitem(self, last: bool = True) -> Tuple[str, Tuple[float, Any]]:
        return self.store.popitem(last=last)

    def __contains__(self, key: str) -> bool:
        return key in self.store

    def items(self):
        return self.store.items()

    def clear(self) -> None:
        """Clear all cache entries."""
        self.store.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self.store),
            "max_size": self.max_size,
            "ttl": self.ttl,
            "utilization": (
                len(self.store) / self.max_size if self.max_size > 0 else 0.0
            ),
        }


class DistributedCache:
    """Enhanced Redis-based distributed cache with memory management."""

    def __init__(self, url: str, ttl: int = 3600, max_connections: int = 10) -> None:
        self.url = url
        self.ttl = ttl
        self.max_connections = max_connections
        self._memory_pool = memory_pool
        self._connection_pool = None
        self._last_health_check = 0
        self._health_check_interval = 30

        try:
            if aioredis:
                self.client = aioredis.from_url(
                    url,
                    max_connections=max_connections,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                    socket_keepalive_options={
                        "TCP_KEEPIDLE": 300,
                        "TCP_KEEPINTVL": 30,
                        "TCP_KEEPCNT": 3,
                    },
                )
            else:
                self.client = None
        except Exception:
            self.client = None

    async def get(self, key: str) -> Optional[Any]:
        """Get item from distributed cache with connection health check."""
        if not self.client:
            return None

        # Health check
        if await self._health_check():
            data_container = self._memory_pool.acquire("redis_data", dict)
            try:
                data_container.clear()
                raw = await self.client.get(key)
                if raw is None:
                    return None

                data_container.update(json.loads(raw))
                return dict(data_container)

            except Exception:
                return None
            finally:
                self._memory_pool.release("redis_data", data_container)

        return None

    async def set(self, key: str, value: Any) -> None:
        """Set item in distributed cache with connection health check."""
        if not self.client:
            return

        if await self._health_check():
            data_container = self._memory_pool.acquire("redis_data", dict)
            try:
                data_container.clear()
                data_container.update(
                    value if isinstance(value, dict) else {"data": value}
                )

                await self.client.set(key, json.dumps(data_container), ex=self.ttl)

            except Exception:
                pass
            finally:
                self._memory_pool.release("redis_data", data_container)

    async def delete(self, key: str) -> None:
        """Delete item from distributed cache."""
        if not self.client:
            return

        if await self._health_check():
            try:
                await self.client.delete(key)
            except Exception:
                pass

    async def get_many(self, keys: List[str]) -> Dict[str, Optional[Any]]:
        """Get multiple items from distributed cache efficiently."""
        if not self.client or not keys:
            return {}

        if not await self._health_check():
            return {}

        results = {}
        data_container = self._memory_pool.acquire("redis_data", dict)

        try:
            # Use pipeline for efficiency
            pipe = self.client.pipeline()
            for key in keys:
                pipe.get(key)

            raw_results = await pipe.execute()

            for i, (key, raw) in enumerate(zip(keys, raw_results)):
                if raw is not None:
                    try:
                        data_container.clear()
                        data_container.update(json.loads(raw))
                        results[key] = dict(data_container)
                    except Exception:
                        results[key] = None
                else:
                    results[key] = None

        except Exception:
            # Fallback to individual gets
            for key in keys:
                results[key] = await self.get(key)
        finally:
            self._memory_pool.release("redis_data", data_container)

        return results

    async def set_many(self, items: Dict[str, Any]) -> None:
        """Set multiple items in distributed cache efficiently."""
        if not self.client or not items:
            return

        if not await self._health_check():
            return

        data_container = self._memory_pool.acquire("redis_data", dict)

        try:
            # Use pipeline for efficiency
            pipe = self.client.pipeline()

            for key, value in items.items():
                data_container.clear()
                data_container.update(
                    value if isinstance(value, dict) else {"data": value}
                )
                pipe.set(key, json.dumps(data_container), ex=self.ttl)

            await pipe.execute()

        except Exception:
            # Fallback to individual sets
            for key, value in items.items():
                await self.set(key, value)
        finally:
            self._memory_pool.release("redis_data", data_container)

    async def _health_check(self) -> bool:
        """Check Redis connection health."""
        current_time = time.time()

        if current_time - self._last_health_check < self._health_check_interval:
            return True  # Assume healthy until next check

        try:
            await self.client.ping()
            self._last_health_check = current_time
            return True
        except Exception:
            return False

    async def cleanup_expired(self) -> int:
        """Clean up expired keys (Redis handles this automatically, but we can track it)."""
        # Redis handles TTL automatically, but we can get info about memory usage
        if not self.client:
            return 0

        try:
            info = await self.client.info("memory")
            memory_usage = info.get("used_memory", 0)
            metrics.set("redis_memory_usage", memory_usage)
            return 0
        except Exception:
            return 0

    async def close(self) -> None:
        """Close Redis connection."""
        if self.client:
            try:
                await self.client.close()
            except Exception:
                pass


class RvuCache:
    """Ultra-optimized RVU cache with advanced bulk operations and memory management."""

    def __init__(
        self,
        db: "BaseDatabase",
        ttl: int = 3600,
        distributed: Optional[DistributedCache] = None,
        predictive_ahead: int = 5,
        local_cache_size: int = 10000,
        batch_size: int = 100,
    ) -> None:
        from ..db.base import BaseDatabase

        self.db = db
        self.ttl = ttl
        self.distributed = distributed
        self.predictive_ahead = predictive_ahead
        self.batch_size = batch_size

        # Enhanced local caching using the shared InMemoryCache implementation
        self.local_cache = InMemoryCache(ttl=ttl, max_size=local_cache_size)
        self.local_cache_size = local_cache_size

        # Access pattern tracking for predictive loading
        self.access_history: deque[str] = deque(maxlen=1000)
        self.access_frequency: Counter[str] = Counter()
        self.access_patterns: Dict[str, List[str]] = {}  # code -> subsequent codes

        # Performance metrics
        self.hits = 0
        self.misses = 0
        self.bulk_fetches = 0
        self.predictive_loads = 0

        # Memory management
        self._memory_pool = memory_pool
        self._max_cache_memory = 200 * 1024 * 1024  # 200MB max cache
        self._cleanup_threshold = 0.8  # Clean up at 80% capacity
        self._last_memory_check = time.time()
        self._memory_check_interval = 60  # Check every minute
        self._total_memory_limit = 500 * 1024 * 1024  # 500MB total limit

        # Object lifecycle tracking
        self._active_containers: List[Any] = []
        self._container_cleanup_threshold = 1000

    def _update_local_cache(self, code: str, data: Dict[str, Any]) -> None:
        """Update local LRU cache with TTL and memory management."""
        # Remove expired entries during update
        self._cleanup_expired_entries(time.time())

        # Use memory pool for cache data
        cache_container = self._memory_pool.acquire("rvu_cache_data", dict)
        cache_container.clear()
        cache_container.update(data)

        # Add new entry using shared cache implementation
        self.local_cache.set(code, dict(cache_container))

        # Return container to pool
        self._memory_pool.release("rvu_cache_data", cache_container)

        # Register container for lifecycle tracking
        self._active_containers.append(cache_container)
        if len(self._active_containers) > self._container_cleanup_threshold:
            self._active_containers = self._active_containers[-500:]

    def _cleanup_expired_entries(self, current_time: float) -> None:
        """Remove expired entries from local cache."""
        self.local_cache.cleanup_expired()

    def _get_from_local_cache(self, code: str) -> Optional[Dict[str, Any]]:
        """Get from local cache with TTL check and memory management."""
        return self.local_cache.get(code)

    async def get(
        self, code: str, *, _prefetch: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Get single RVU code with memory management optimizations."""
        if not code:
            return None

        # Memory check for large operations
        if not _prefetch:
            await self._check_cache_memory()

        # Check local cache first
        cached_data = self._get_from_local_cache(code)
        if cached_data:
            self.hits += 1
            metrics.inc("rvu_cache_hits")
            if not _prefetch:
                self._record_access(code)
            self._update_hit_ratio()
            return cached_data

        # Check distributed cache
        if self.distributed:
            distributed_data = await self.distributed.get(f"rvu:{code}")
            if distributed_data:
                self.hits += 1
                metrics.inc("rvu_cache_hits")
                self._update_local_cache(code, distributed_data)
                if not _prefetch:
                    self._record_access(code)
                self._update_hit_ratio()
                return distributed_data

        # Cache miss - fetch from database
        self.misses += 1
        metrics.inc("rvu_cache_misses")

        # Use bulk fetch even for single items for consistency
        bulk_result = await self.get_many([code])
        result = bulk_result.get(code)

        if not _prefetch:
            self._record_access(code)
            # Trigger predictive loading
            await self._predictive_load_async(code)

        self._update_hit_ratio()
        return result

    async def get_many(
        self, codes: Iterable[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Ultra-optimized bulk RVU retrieval with multi-level caching and memory management."""
        unique_codes = [c for c in set(codes) if c]
        if not unique_codes:
            return {}

        # Check memory before processing large requests
        if len(unique_codes) > 1000:
            await self._check_cache_memory()

        # Use memory pool for result containers
        results_container = self._memory_pool.acquire("rvu_results", dict)
        local_hits_container = self._memory_pool.acquire("rvu_hits", dict)
        distributed_hits_container = self._memory_pool.acquire("rvu_hits", dict)
        missing_codes_container = self._memory_pool.acquire("rvu_codes", list)

        try:
            results_container.clear()
            local_hits_container.clear()
            distributed_hits_container.clear()
            missing_codes_container.clear()

            # Phase 1: Check local cache
            for code in unique_codes:
                cached_data = self._get_from_local_cache(code)
                if cached_data:
                    local_hits_container[code] = cached_data
                    results_container[code] = cached_data
                    self.hits += 1
                else:
                    missing_codes_container.append(code)

            # Phase 2: Check distributed cache for missing codes
            if missing_codes_container and self.distributed:
                distributed_keys = [f"rvu:{code}" for code in missing_codes_container]

                # Bulk fetch from distributed cache
                distributed_results = await self.distributed.get_many(distributed_keys)

                # Process distributed results
                codes_still_missing = []
                for i, code in enumerate(missing_codes_container):
                    key = distributed_keys[i]
                    data = distributed_results.get(key)
                    if data:
                        distributed_hits_container[code] = data
                        results_container[code] = data
                        self._update_local_cache(code, data)
                        self.hits += 1
                    else:
                        codes_still_missing.append(code)

                # Update missing codes list
                missing_codes_container.clear()
                missing_codes_container.extend(codes_still_missing)

            # Phase 3: Bulk database fetch for remaining missing codes
            if missing_codes_container:
                self.misses += len(missing_codes_container)
                self.bulk_fetches += 1

                try:
                    # Use optimized bulk fetch if available
                    if hasattr(self.db, "get_rvu_bulk_optimized"):
                        db_results = await self.db.get_rvu_bulk_optimized(
                            missing_codes_container
                        )
                    else:
                        # Fallback to batch query
                        db_results = await self._fetch_batch_from_db(
                            missing_codes_container
                        )

                    # Cache and distribute results using memory pooling
                    cache_updates = {}

                    for code, data in db_results.items():
                        if data:
                            results_container[code] = data
                            self._update_local_cache(code, data)
                            cache_updates[f"rvu:{code}"] = data

                    # Bulk update distributed cache
                    if cache_updates and self.distributed:
                        await self.distributed.set_many(cache_updates)

                    # Set None for codes not found in database
                    for code in missing_codes_container:
                        if code not in results_container:
                            results_container[code] = None

                except Exception as e:
                    print(f"Warning: Bulk RVU fetch failed: {e}")
                    # Set None for all missing codes on error
                    for code in missing_codes_container:
                        results_container[code] = None

            # Update metrics
            metrics.inc(
                "rvu_cache_hits",
                len(local_hits_container) + len(distributed_hits_container),
            )
            metrics.inc("rvu_cache_misses", len(missing_codes_container))
            metrics.inc("rvu_bulk_fetches", 1 if missing_codes_container else 0)

            # Record access patterns for all successfully retrieved codes
            successful_codes = [
                code for code, data in results_container.items() if data
            ]
            for code in successful_codes:
                self._record_access(code)

            # Trigger predictive loading for frequently accessed codes
            if successful_codes:
                await self._bulk_predictive_load(successful_codes)

            self._update_hit_ratio()

            # Return copy of results
            return dict(results_container)

        finally:
            # Return all containers to pool
            self._memory_pool.release("rvu_results", results_container)
            self._memory_pool.release("rvu_hits", local_hits_container)
            self._memory_pool.release("rvu_hits", distributed_hits_container)
            self._memory_pool.release("rvu_codes", missing_codes_container)

    async def _fetch_batch_from_db(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fallback batch fetch from database with memory management."""
        results_container = self._memory_pool.acquire("db_results", dict)

        try:
            results_container.clear()

            # Build batch query
            placeholders = ", ".join(f"${i+1}" for i in range(len(codes)))
            query = f"""
                SELECT procedure_code, description, total_rvu, work_rvu, 
                       practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code IN ({placeholders}) AND status = 'active'
            """

            rows = await self.db.fetch(query, *codes)

            # Use memory pooling for row processing
            row_container = self._memory_pool.acquire("db_row", dict)

            try:
                for row in rows:
                    code = row.get("procedure_code")
                    if code:
                        row_container.clear()
                        row_container.update(row)
                        results_container[code] = dict(row_container)
            finally:
                self._memory_pool.release("db_row", row_container)

            return dict(results_container)

        except Exception as e:
            print(f"Warning: Batch RVU database fetch failed: {e}")
            # Fallback to individual queries
            for code in codes:
                try:
                    rows = await self.db.fetch(
                        "SELECT * FROM rvu_data WHERE procedure_code = $1 AND status = 'active'",
                        code,
                    )
                    if rows:
                        results_container[code] = dict(rows[0])
                except Exception:
                    continue

            return dict(results_container)

        finally:
            self._memory_pool.release("db_results", results_container)

    def _record_access(self, code: str) -> None:
        """Record access pattern for predictive loading with memory management."""
        self.access_history.append(code)
        self.access_frequency[code] += 1

        # Update access patterns (what codes are accessed after this one)
        if len(self.access_history) >= 2:
            prev_code = self.access_history[-2]
            if prev_code not in self.access_patterns:
                self.access_patterns[prev_code] = []
            self.access_patterns[prev_code].append(code)

            # Keep only recent patterns (last 50 for each code)
            if len(self.access_patterns[prev_code]) > 50:
                self.access_patterns[prev_code] = self.access_patterns[prev_code][-50:]

    async def _predictive_load_async(self, code: str) -> None:
        """Asynchronous predictive loading based on access patterns."""
        try:
            await self._predictive_load(code)
        except Exception as e:
            print(f"Warning: Predictive loading failed for {code}: {e}")

    async def _predictive_load(self, code: str) -> None:
        """Load codes likely to be accessed next based on patterns."""
        if not self.predictive_ahead:
            return

        # Use memory pooling for candidate collection
        candidates_container = self._memory_pool.acquire("predictive_candidates", set)

        try:
            candidates_container.clear()

            # Pattern-based prediction
            if code in self.access_patterns:
                recent_patterns = self.access_patterns[code][-10:]  # Last 10 patterns
                pattern_counter = Counter(recent_patterns)
                for next_code, _count in pattern_counter.most_common(
                    self.predictive_ahead
                ):
                    candidates_container.add(next_code)

            # Sequential prediction (try next codes in sequence)
            sequential_candidates = self._get_sequential_codes(
                code, self.predictive_ahead
            )
            candidates_container.update(sequential_candidates)

            # Frequency-based prediction (other frequently accessed codes)
            if len(candidates_container) < self.predictive_ahead:
                frequent_codes = [
                    c for c, _count in self.access_frequency.most_common(20)
                ]
                for freq_code in frequent_codes:
                    if freq_code != code and freq_code not in candidates_container:
                        candidates_container.add(freq_code)
                        if len(candidates_container) >= self.predictive_ahead:
                            break

            # Filter out codes already in cache
            codes_to_fetch = []
            for candidate in candidates_container:
                if not self._get_from_local_cache(candidate):
                    codes_to_fetch.append(candidate)

            # Bulk fetch missing codes
            if codes_to_fetch:
                self.predictive_loads += 1
                metrics.inc("rvu_predictive_loads")
                try:
                    await self.get_many(codes_to_fetch)
                except Exception as e:
                    print(f"Warning: Predictive bulk fetch failed: {e}")

        finally:
            self._memory_pool.release("predictive_candidates", candidates_container)

    async def _bulk_predictive_load(self, accessed_codes: List[str]) -> None:
        """Bulk predictive loading for multiple recently accessed codes."""
        if not self.predictive_ahead or len(accessed_codes) < 2:
            return

        # Use memory pooling for bulk prediction
        candidates_container = self._memory_pool.acquire(
            "bulk_predictive_candidates", set
        )

        try:
            candidates_container.clear()

            # Collect predictive candidates for all accessed codes
            for code in accessed_codes[-5:]:  # Only consider last 5 codes
                # Pattern-based candidates
                if code in self.access_patterns:
                    recent_patterns = self.access_patterns[code][-5:]
                    candidates_container.update(recent_patterns)

                # Sequential candidates
                sequential = self._get_sequential_codes(code, 2)
                candidates_container.update(sequential)

            # Remove codes already accessed and already cached
            candidates_container = {
                c for c in candidates_container if c not in accessed_codes
            }
            codes_to_fetch = [
                c for c in candidates_container if not self._get_from_local_cache(c)
            ]

            # Limit to reasonable batch size
            if codes_to_fetch:
                limited_codes = codes_to_fetch[: self.predictive_ahead * 2]
                if limited_codes:
                    self.predictive_loads += 1
                    metrics.inc("rvu_bulk_predictive_loads")
                    try:
                        await self.get_many(limited_codes)
                    except Exception as e:
                        print(f"Warning: Bulk predictive loading failed: {e}")

        finally:
            self._memory_pool.release(
                "bulk_predictive_candidates", candidates_container
            )

    def _get_sequential_codes(self, code: str, count: int) -> List[str]:
        """Get sequential procedure codes for predictive loading."""
        try:
            # Simple pattern: if code ends with digits, increment them
            if not code:
                return []

            # Find numeric suffix
            numeric_suffix = ""
            alpha_prefix = ""

            for i, char in enumerate(reversed(code)):
                if char.isdigit():
                    numeric_suffix = char + numeric_suffix
                else:
                    alpha_prefix = code[: len(code) - i]
                    break

            if not numeric_suffix:
                return []

            base_num = int(numeric_suffix)
            width = len(numeric_suffix)

            candidates = []
            for i in range(1, count + 1):
                next_num = base_num + i
                next_code = alpha_prefix + str(next_num).zfill(width)
                candidates.append(next_code)

            return candidates

        except Exception:
            return []

    async def warm_cache(self, codes: Iterable[str]) -> None:
        """Warm cache with bulk loading and memory management optimization."""
        code_list = [c for c in set(codes) if c]
        if not code_list:
            return

        # Memory check for large warming operations
        if len(code_list) > 1000:
            await self._check_cache_memory()

        # Process in optimized batches with memory management
        batch_size = min(
            self.batch_size * 2, 200
        )  # Larger batches for warming but memory-safe

        for i in range(0, len(code_list), batch_size):
            batch = code_list[i : i + batch_size]
            try:
                await self.get_many(batch)
            except Exception as e:
                print(f"Warning: Cache warming batch failed: {e}")
                # Continue with next batch
                continue

            # Memory check between batches
            if i > 0 and i % (batch_size * 5) == 0:
                await self._check_cache_memory()

        metrics.inc("rvu_cache_warm_operations")
        metrics.set("rvu_cache_warmed_codes", len(code_list))

    async def invalidate(self, code: str) -> None:
        """Invalidate a specific code from all cache levels."""
        # Remove from local cache
        if code in self.local_cache:
            self.local_cache.delete(code)

        # Remove from distributed cache
        if self.distributed:
            await self.distributed.delete(f"rvu:{code}")

        metrics.inc("rvu_cache_invalidations")

    async def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate codes matching a pattern with memory management."""
        invalidated_count = 0

        # Use memory pooling for invalidation operations
        keys_to_remove_container = self._memory_pool.acquire("invalidation_keys", list)

        try:
            keys_to_remove_container.clear()

            # Invalidate from local cache
            for key in self.local_cache.keys():
                if pattern in key:
                    keys_to_remove_container.append(key)

            for key in keys_to_remove_container:
                self.local_cache.delete(key)
                invalidated_count += 1

            # Note: Distributed cache pattern invalidation would require specific support
            # This is a simplified implementation

        finally:
            self._memory_pool.release("invalidation_keys", keys_to_remove_container)

        metrics.inc("rvu_cache_pattern_invalidations")
        metrics.set("rvu_cache_pattern_invalidated_count", invalidated_count)

    def _update_hit_ratio(self) -> None:
        """Update cache hit ratio metrics."""
        total = self.hits + self.misses
        if total > 0:
            hit_ratio = self.hits / total
            miss_ratio = self.misses / total
            metrics.set("rvu_cache_hit_ratio", hit_ratio)
            metrics.set("rvu_cache_miss_ratio", miss_ratio)

    async def _check_cache_memory(self) -> None:
        """Check and manage cache memory usage."""
        current_time = time.time()
        if current_time - self._last_memory_check < self._memory_check_interval:
            return

        self._last_memory_check = current_time

        try:
            # Get process memory usage
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            # Estimate cache memory usage
            cache_memory = len(self.local_cache) * 2048  # Estimate 2KB per entry

            if (
                cache_memory > self._max_cache_memory * self._cleanup_threshold
                or memory_mb > self._total_memory_limit / (1024 * 1024) * 0.8
            ):
                # Perform cache cleanup
                cleanup_count = await self._perform_cache_cleanup()

                print(f"Cache memory cleanup: removed {cleanup_count} entries")
                metrics.inc("cache_memory_cleanups")

        except Exception as e:
            print(f"Cache memory check failed: {e}")

    async def _perform_cache_cleanup(self) -> int:
        """Perform cache cleanup to free memory with advanced cleanup strategies."""
        initial_size = len(self.local_cache)

        # Phase 1: Remove expired entries
        current_time = time.time()
        self._cleanup_expired_entries(current_time)
        expired_removed = initial_size - len(self.local_cache)

        # Phase 2: LRU cleanup if still over threshold
        target_size = int(self.local_cache_size * 0.6)  # Reduce to 60% capacity

        lru_removed = 0
        while len(self.local_cache) > target_size:
            try:
                self.local_cache.popitem(last=False)  # Remove oldest
                lru_removed += 1
            except KeyError:
                break

        # Phase 3: Clean up access patterns for removed entries
        self._cleanup_access_patterns()

        # Phase 4: Clean up memory pools
        pool_cleanup = self._memory_pool.cleanup_all()

        # Phase 5: Force garbage collection
        collected = gc.collect()

        total_removed = expired_removed + lru_removed

        # Update metrics
        metrics.set("cache_cleanup_expired_removed", expired_removed)
        metrics.set("cache_cleanup_lru_removed", lru_removed)
        metrics.set("cache_cleanup_gc_collected", collected)
        metrics.set("cache_cleanup_pool_stats", sum(pool_cleanup.values()))

        return total_removed

    def _cleanup_access_patterns(self) -> None:
        """Clean up access patterns to prevent memory leaks."""
        # Keep only recent access history
        if len(self.access_history) > 500:
            recent_history = list(self.access_history)[-300:]
            self.access_history.clear()
            self.access_history.extend(recent_history)

        # Clean up frequency counter
        if len(self.access_frequency) > 1000:
            # Keep only top 500 most frequent
            top_frequent = dict(self.access_frequency.most_common(500))
            self.access_frequency.clear()
            self.access_frequency.update(top_frequent)

        # Clean up access patterns
        if len(self.access_patterns) > 500:
            # Keep only patterns for recently accessed codes
            recent_codes = set(list(self.access_history)[-100:])
            patterns_to_keep = {
                code: patterns
                for code, patterns in self.access_patterns.items()
                if code in recent_codes
            }
            self.access_patterns = patterns_to_keep

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache performance statistics including memory management."""
        total_requests = self.hits + self.misses
        hit_ratio = self.hits / total_requests if total_requests > 0 else 0.0

        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
        except Exception:
            memory_mb = 0

        return {
            "performance": {
                "hits": self.hits,
                "misses": self.misses,
                "total_requests": total_requests,
                "hit_ratio": hit_ratio,
                "bulk_fetches": self.bulk_fetches,
                "predictive_loads": self.predictive_loads,
            },
            "cache_sizes": {
                "local_cache_size": len(self.local_cache),
                "local_cache_capacity": self.local_cache_size,
                "access_history_size": len(self.access_history),
                "access_patterns_count": len(self.access_patterns),
                "access_frequency_entries": len(self.access_frequency),
            },
            "memory_management": {
                "current_memory_mb": memory_mb,
                "max_cache_memory_mb": self._max_cache_memory / 1024 / 1024,
                "total_memory_limit_mb": self._total_memory_limit / 1024 / 1024,
                "cache_memory_estimate_mb": len(self.local_cache) * 2048 / 1024 / 1024,
                "active_containers": len(self._active_containers),
                "memory_pools": self._memory_pool.get_stats(),
                "last_memory_check": self._last_memory_check,
                "cleanup_threshold": self._cleanup_threshold,
            },
            "optimization_features": {
                "local_lru_cache": True,
                "distributed_cache": self.distributed is not None,
                "bulk_operations": True,
                "predictive_loading": self.predictive_ahead > 0,
                "pattern_recognition": True,
                "sequential_prediction": True,
                "access_frequency_tracking": True,
                "memory_pooling": True,
                "memory_monitoring": True,
                "adaptive_cleanup": True,
                "lifecycle_management": True,
            },
            "configuration": {
                "ttl_seconds": self.ttl,
                "predictive_ahead": self.predictive_ahead,
                "batch_size": self.batch_size,
                "local_cache_capacity": self.local_cache_size,
                "memory_check_interval": self._memory_check_interval,
            },
        }

    async def optimize_cache(self) -> Dict[str, Any]:
        """Perform comprehensive cache optimization operations."""
        optimization_container = self._memory_pool.acquire("optimization_results", dict)

        try:
            optimization_container.clear()

            # Phase 1: Clean up expired entries
            current_time = time.time()
            initial_size = len(self.local_cache)
            self._cleanup_expired_entries(current_time)
            cleaned_entries = initial_size - len(self.local_cache)
            optimization_container["expired_entries_cleaned"] = cleaned_entries

            # Phase 2: Optimize access patterns
            initial_patterns = len(self.access_patterns)

            # Remove patterns for codes not accessed recently
            recent_codes = set(list(self.access_history)[-100:])  # Last 100 accesses
            patterns_to_remove = []

            for code in self.access_patterns:
                if code not in recent_codes and self.access_frequency[code] < 5:
                    patterns_to_remove.append(code)

            for code in patterns_to_remove:
                del self.access_patterns[code]

            pattern_cleanup = initial_patterns - len(self.access_patterns)
            optimization_container["access_patterns_cleaned"] = pattern_cleanup

            # Phase 3: Memory pool optimization
            pool_cleanup_stats = self._memory_pool.cleanup_all()
            optimization_container["memory_pool_cleanup"] = pool_cleanup_stats

            # Phase 4: Preload frequently accessed codes that aren't cached
            frequent_codes_container = self._memory_pool.acquire("frequent_codes", list)

            try:
                frequent_codes_container.clear()

                for code, count in self.access_frequency.most_common(50):
                    if count >= 10 and not self._get_from_local_cache(code):
                        frequent_codes_container.append(code)

                if frequent_codes_container:
                    try:
                        limited_codes = frequent_codes_container[
                            :20
                        ]  # Limit preload size
                        await self.get_many(limited_codes)
                        optimization_container["frequent_codes_preloaded"] = len(
                            limited_codes
                        )
                    except Exception as e:
                        optimization_container["preload_error"] = str(e)

            finally:
                self._memory_pool.release("frequent_codes", frequent_codes_container)

            # Phase 5: Garbage collection
            collected = gc.collect()
            optimization_container["gc_objects_collected"] = collected

            # Phase 6: Memory usage after optimization
            try:
                process = psutil.Process()
                final_memory_mb = process.memory_info().rss / 1024 / 1024
                optimization_container["final_memory_mb"] = final_memory_mb
            except Exception:
                optimization_container["final_memory_mb"] = 0

            # Update metrics
            metrics.inc("rvu_cache_optimizations")

            return dict(optimization_container)

        finally:
            self._memory_pool.release("optimization_results", optimization_container)

    def reset_stats(self) -> None:
        """Reset performance statistics with memory cleanup."""
        # Reset performance counters
        self.hits = 0
        self.misses = 0
        self.bulk_fetches = 0
        self.predictive_loads = 0

        # Clear access patterns but keep recent history
        recent_codes = list(self.access_history)[-50:]  # Keep last 50
        self.access_history.clear()
        self.access_history.extend(recent_codes)

        # Reset frequency counter but keep top entries
        top_frequent = dict(self.access_frequency.most_common(100))
        self.access_frequency.clear()
        self.access_frequency.update(top_frequent)

        # Clear access patterns for non-recent codes
        recent_codes_set = set(recent_codes)
        patterns_to_keep = {
            code: patterns
            for code, patterns in self.access_patterns.items()
            if code in recent_codes_set
        }
        self.access_patterns = patterns_to_keep

        # Clean up memory pools
        self._memory_pool.cleanup_all()

        # Clear active containers
        self._active_containers.clear()

        # Force garbage collection
        gc.collect()

        metrics.inc("rvu_cache_stats_resets")

    async def get_memory_status(self) -> Dict[str, Any]:
        """Get detailed memory status for the cache."""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            return {
                "current_memory_mb": memory_mb,
                "cache_memory_estimate_mb": len(self.local_cache) * 2048 / 1024 / 1024,
                "max_cache_memory_mb": self._max_cache_memory / 1024 / 1024,
                "total_memory_limit_mb": self._total_memory_limit / 1024 / 1024,
                "memory_utilization": memory_mb
                / (self._total_memory_limit / 1024 / 1024),
                "cache_utilization": len(self.local_cache) / self.local_cache_size,
                "memory_pools": self._memory_pool.get_stats(),
                "active_containers": len(self._active_containers),
                "last_memory_check": self._last_memory_check,
                "cleanup_threshold": self._cleanup_threshold,
                "access_patterns_memory": {
                    "access_history_size": len(self.access_history),
                    "access_frequency_size": len(self.access_frequency),
                    "access_patterns_size": len(self.access_patterns),
                },
            }
        except Exception as e:
            return {"error": str(e)}

    async def emergency_cleanup(self) -> Dict[str, Any]:
        """Emergency memory cleanup for critical situations."""
        cleanup_results = {}

        # Phase 1: Aggressive cache cleanup
        initial_cache_size = len(self.local_cache)
        target_size = max(100, self.local_cache_size // 10)  # Keep only 10%

        while len(self.local_cache) > target_size:
            try:
                self.local_cache.popitem(last=False)
            except KeyError:
                break

        cleanup_results["cache_entries_removed"] = initial_cache_size - len(
            self.local_cache
        )

        # Phase 2: Clear all access patterns
        cleanup_results["access_patterns_cleared"] = len(self.access_patterns)
        self.access_patterns.clear()

        cleanup_results["access_frequency_cleared"] = len(self.access_frequency)
        self.access_frequency.clear()

        # Keep minimal access history
        if len(self.access_history) > 50:
            recent_history = list(self.access_history)[-20:]
            self.access_history.clear()
            self.access_history.extend(recent_history)
            cleanup_results["access_history_trimmed"] = True

        # Phase 3: Aggressive memory pool cleanup
        pool_cleanup = self._memory_pool.cleanup_all()
        cleanup_results["memory_pools_cleaned"] = pool_cleanup

        # Phase 4: Clear active containers
        cleanup_results["active_containers_cleared"] = len(self._active_containers)
        self._active_containers.clear()

        # Phase 5: Multiple garbage collection cycles
        total_collected = 0
        for _ in range(3):  # Multiple GC cycles
            collected = gc.collect()
            total_collected += collected

        cleanup_results["total_gc_collected"] = total_collected

        # Phase 6: Reset statistics
        self.reset_stats()
        cleanup_results["stats_reset"] = True

        # Update metrics
        metrics.inc("rvu_emergency_cleanups")

        return cleanup_results


class OptimizedRvuCache(RvuCache):
    """Specialized optimized RVU cache with additional performance features."""

    def __init__(self, redis_url: str, db: "BaseDatabase", redis_client=None):
        # Initialize distributed cache
        distributed = DistributedCache(redis_url) if redis_url else None
        if redis_client:
            distributed.client = redis_client

        # Initialize with optimized settings
        super().__init__(
            db=db,
            ttl=3600,
            distributed=distributed,
            predictive_ahead=10,  # More aggressive prediction
            local_cache_size=20000,  # Larger local cache
            batch_size=200,  # Larger batch sizes
        )

        # Additional optimizations
        self._prefetch_enabled = True
        self._prefetch_threshold = 100  # Prefetch when cache gets this low
        self._last_prefetch = 0
        self._prefetch_interval = 300  # 5 minutes between prefetches

    async def get_many_optimized(
        self, codes: Iterable[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Ultra-optimized version with additional performance features."""
        # Trigger prefetch if cache is getting low
        if (
            self._prefetch_enabled
            and len(self.local_cache) < self._prefetch_threshold
            and time.time() - self._last_prefetch > self._prefetch_interval
        ):

            asyncio.create_task(self._background_prefetch())

        # Use the parent optimized method
        return await super().get_many(codes)

    async def _background_prefetch(self) -> None:
        """Background prefetching of likely-to-be-accessed codes."""
        try:
            self._last_prefetch = time.time()

            # Get top frequent codes that aren't cached
            prefetch_candidates = []
            for code, _count in self.access_frequency.most_common(100):
                if not self._get_from_local_cache(code):
                    prefetch_candidates.append(code)
                    if len(prefetch_candidates) >= 50:  # Limit prefetch size
                        break

            if prefetch_candidates:
                await self.get_many(prefetch_candidates)
                metrics.inc("rvu_background_prefetches")

        except Exception as e:
            print(f"Background prefetch failed: {e}")


# Export memory-managed cache components
__all__ = [
    "InMemoryCache",
    "DistributedCache",
    "RvuCache",
    "OptimizedRvuCache",
    "MemoryPool",
    "memory_pool",
]
