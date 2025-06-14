import json
import time
from collections import Counter, deque, OrderedDict
from typing import Any, Dict, Iterable, Optional

from ..monitoring.metrics import metrics

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - redis optional
    aioredis = None


class InMemoryCache:
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self.store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self.store.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > self.ttl:
            del self.store[key]
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self.store[key] = (time.time(), value)

    def delete(self, key: str) -> None:
        self.store.pop(key, None)


class DistributedCache:
    """Simple Redis-based distributed cache."""

    def __init__(self, url: str, ttl: int = 3600) -> None:
        self.url = url
        self.ttl = ttl
        try:
            self.client = aioredis.from_url(url) if aioredis else None
        except Exception:
            self.client = None

    async def get(self, key: str) -> Optional[Any]:
        if not self.client:
            return None
        try:
            raw = await self.client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def set(self, key: str, value: Any) -> None:
        if not self.client:
            return
        try:
            await self.client.set(key, json.dumps(value), ex=self.ttl)
        except Exception:
            pass

    async def delete(self, key: str) -> None:
        if not self.client:
            return
        try:
            await self.client.delete(key)
        except Exception:
            pass


class RvuCache:
    """Ultra-optimized RVU cache with advanced bulk operations and predictive loading."""

    def __init__(
        self,
        db: "BaseDatabase",
        ttl: int = 3600,
        distributed: Optional["DistributedCache"] = None,
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
        
        # Enhanced local caching with LRU
        self.local_cache: OrderedDict[str, Tuple[float, Dict[str, Any]]] = OrderedDict()
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
        
        # Batch processing optimization
        self._pending_requests: Dict[str, List] = {}  # code -> list of futures
        self._batch_timer: Optional = None
        self._batch_lock = False

    def _update_local_cache(self, code: str, data: Dict[str, Any]) -> None:
        """Update local LRU cache with TTL."""
        current_time = time.time()
        
        # Remove expired entries during update
        self._cleanup_expired_entries(current_time)
        
        # Add new entry
        self.local_cache[code] = (current_time, data)
        self.local_cache.move_to_end(code)
        
        # Evict oldest if over capacity
        while len(self.local_cache) > self.local_cache_size:
            self.local_cache.popitem(last=False)

    def _cleanup_expired_entries(self, current_time: float) -> None:
        """Remove expired entries from local cache."""
        expired_keys = []
        for key, (timestamp, _data) in self.local_cache.items():
            if current_time - timestamp > self.ttl:
                expired_keys.append(key)
            else:
                break  # OrderedDict, so rest are newer
        
        for key in expired_keys:
            del self.local_cache[key]

    def _get_from_local_cache(self, code: str) -> Optional[Dict[str, Any]]:
        """Get from local cache with TTL check."""
        if code not in self.local_cache:
            return None
        
        timestamp, data = self.local_cache[code]
        current_time = time.time()
        
        if current_time - timestamp > self.ttl:
            del self.local_cache[code]
            return None
        
        # Move to end (most recently used)
        self.local_cache.move_to_end(code)
        return data

    async def get(self, code: str, *, _prefetch: bool = False) -> Optional[Dict[str, Any]]:
        """Get single RVU code with optimizations."""
        if not code:
            return None
        
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

    async def get_many(self, codes: Iterable[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Ultra-optimized bulk RVU retrieval with multi-level caching."""
        unique_codes = [c for c in set(codes) if c]
        if not unique_codes:
            return {}
        
        results: Dict[str, Optional[Dict[str, Any]]] = {}
        local_hits: Dict[str, Dict[str, Any]] = {}
        distributed_hits: Dict[str, Dict[str, Any]] = {}
        missing_codes: List[str] = []
        
        # Phase 1: Check local cache
        for code in unique_codes:
            cached_data = self._get_from_local_cache(code)
            if cached_data:
                local_hits[code] = cached_data
                results[code] = cached_data
                self.hits += 1
            else:
                missing_codes.append(code)
        
        # Phase 2: Check distributed cache for missing codes
        if missing_codes and self.distributed:
            distributed_keys = [f"rvu:{code}" for code in missing_codes]
            
            # Bulk fetch from distributed cache
            if hasattr(self.distributed, 'get_many'):
                distributed_results = await self.distributed.get_many(distributed_keys)
                
                for i, code in enumerate(missing_codes):
                    key = distributed_keys[i]
                    data = distributed_results.get(key)
                    if data:
                        distributed_hits[code] = data
                        results[code] = data
                        self._update_local_cache(code, data)
                        self.hits += 1
            else:
                # Fallback to individual distributed cache lookups
                for code in missing_codes:
                    data = await self.distributed.get(f"rvu:{code}")
                    if data:
                        distributed_hits[code] = data
                        results[code] = data
                        self._update_local_cache(code, data)
                        self.hits += 1
        
        # Update missing codes list
        still_missing = [c for c in missing_codes if c not in distributed_hits]
        
        # Phase 3: Bulk database fetch for remaining missing codes
        if still_missing:
            self.misses += len(still_missing)
            self.bulk_fetches += 1
            
            try:
                # Use optimized bulk fetch if available
                if hasattr(self.db, 'get_rvu_bulk_optimized'):
                    db_results = await self.db.get_rvu_bulk_optimized(still_missing)
                else:
                    # Fallback to batch query
                    db_results = await self._fetch_batch_from_db(still_missing)
                
                # Cache and distribute results
                for code, data in db_results.items():
                    if data:
                        results[code] = data
                        self._update_local_cache(code, data)
                        
                        # Store in distributed cache
                        if self.distributed:
                            await self.distributed.set(f"rvu:{code}", data)
                
                # Set None for codes not found in database
                for code in still_missing:
                    if code not in results:
                        results[code] = None
                
            except Exception as e:
                print(f"Warning: Bulk RVU fetch failed: {e}")
                # Set None for all missing codes on error
                for code in still_missing:
                    results[code] = None
        
        # Update metrics
        metrics.inc("rvu_cache_hits", len(local_hits) + len(distributed_hits))
        metrics.inc("rvu_cache_misses", len(still_missing))
        metrics.inc("rvu_bulk_fetches", 1 if still_missing else 0)
        
        # Record access patterns for all successfully retrieved codes
        successful_codes = [code for code, data in results.items() if data]
        for code in successful_codes:
            self._record_access(code)
        
        # Trigger predictive loading for frequently accessed codes
        if successful_codes:
            await self._bulk_predictive_load(successful_codes)
        
        self._update_hit_ratio()
        return results

    async def _fetch_batch_from_db(self, codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fallback batch fetch from database."""
        results = {}
        
        try:
            # Build batch query
            placeholders = ", ".join(f"${i+1}" for i in range(len(codes)))
            query = f"""
                SELECT procedure_code, description, total_rvu, work_rvu, 
                       practice_expense_rvu, malpractice_rvu, conversion_factor
                FROM rvu_data 
                WHERE procedure_code IN ({placeholders}) AND status = 'active'
            """
            
            rows = await self.db.fetch(query, *codes)
            
            for row in rows:
                code = row.get("procedure_code")
                if code:
                    results[code] = dict(row)
                    
        except Exception as e:
            print(f"Warning: Batch RVU database fetch failed: {e}")
            # Fallback to individual queries
            for code in codes:
                try:
                    rows = await self.db.fetch(
                        "SELECT * FROM rvu_data WHERE procedure_code = $1 AND status = 'active'",
                        code
                    )
                    if rows:
                        results[code] = dict(rows[0])
                except Exception:
                    continue
        
        return results

    def _record_access(self, code: str) -> None:
        """Record access pattern for predictive loading."""
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
        
        candidates = set()
        
        # Pattern-based prediction
        if code in self.access_patterns:
            recent_patterns = self.access_patterns[code][-10:]  # Last 10 patterns
            pattern_counter = Counter(recent_patterns)
            for next_code, _count in pattern_counter.most_common(self.predictive_ahead):
                candidates.add(next_code)
        
        # Sequential prediction (try next codes in sequence)
        sequential_candidates = self._get_sequential_codes(code, self.predictive_ahead)
        candidates.update(sequential_candidates)
        
        # Frequency-based prediction (other frequently accessed codes)
        if len(candidates) < self.predictive_ahead:
            frequent_codes = [c for c, _count in self.access_frequency.most_common(20)]
            for freq_code in frequent_codes:
                if freq_code != code and freq_code not in candidates:
                    candidates.add(freq_code)
                    if len(candidates) >= self.predictive_ahead:
                        break
        
        # Filter out codes already in cache
        codes_to_fetch = []
        for candidate in candidates:
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

    async def _bulk_predictive_load(self, accessed_codes: List[str]) -> None:
        """Bulk predictive loading for multiple recently accessed codes."""
        if not self.predictive_ahead or len(accessed_codes) < 2:
            return
        
        candidates = set()
        
        # Collect predictive candidates for all accessed codes
        for code in accessed_codes[-5:]:  # Only consider last 5 codes
            # Pattern-based candidates
            if code in self.access_patterns:
                recent_patterns = self.access_patterns[code][-5:]
                candidates.update(recent_patterns)
            
            # Sequential candidates
            sequential = self._get_sequential_codes(code, 2)
            candidates.update(sequential)
        
        # Remove codes already accessed and already cached
        candidates = {c for c in candidates if c not in accessed_codes}
        codes_to_fetch = [c for c in candidates if not self._get_from_local_cache(c)]
        
        # Limit to reasonable batch size
        if codes_to_fetch:
            limited_codes = codes_to_fetch[:self.predictive_ahead * 2]
            if limited_codes:
                self.predictive_loads += 1
                metrics.inc("rvu_bulk_predictive_loads")
                try:
                    await self.get_many(limited_codes)
                except Exception as e:
                    print(f"Warning: Bulk predictive loading failed: {e}")

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
                    alpha_prefix = code[:len(code)-i]
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
        """Warm cache with bulk loading and optimization."""
        code_list = [c for c in set(codes) if c]
        if not code_list:
            return
        
        # Process in optimized batches
        batch_size = self.batch_size * 2  # Larger batches for warming
        
        for i in range(0, len(code_list), batch_size):
            batch = code_list[i:i + batch_size]
            try:
                await self.get_many(batch)
            except Exception as e:
                print(f"Warning: Cache warming batch failed: {e}")
                # Continue with next batch
                continue
        
        metrics.inc("rvu_cache_warm_operations")
        metrics.set("rvu_cache_warmed_codes", len(code_list))

    async def invalidate(self, code: str) -> None:
        """Invalidate a specific code from all cache levels."""
        # Remove from local cache
        if code in self.local_cache:
            del self.local_cache[code]
        
        # Remove from distributed cache
        if self.distributed:
            await self.distributed.delete(f"rvu:{code}")
        
        metrics.inc("rvu_cache_invalidations")

    async def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate codes matching a pattern."""
        invalidated_count = 0
        
        # Invalidate from local cache
        keys_to_remove = []
        for key in self.local_cache.keys():
            if pattern in key:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.local_cache[key]
            invalidated_count += 1
        
        # Note: Distributed cache pattern invalidation would require specific support
        # This is a simplified implementation
        
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

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache performance statistics."""
        total_requests = self.hits + self.misses
        hit_ratio = self.hits / total_requests if total_requests > 0 else 0.0
        
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
            "optimization_features": {
                "local_lru_cache": True,
                "distributed_cache": self.distributed is not None,
                "bulk_operations": True,
                "predictive_loading": self.predictive_ahead > 0,
                "pattern_recognition": True,
                "sequential_prediction": True,
                "access_frequency_tracking": True,
            },
            "configuration": {
                "ttl_seconds": self.ttl,
                "predictive_ahead": self.predictive_ahead,
                "batch_size": self.batch_size,
                "local_cache_capacity": self.local_cache_size,
            }
        }

    async def optimize_cache(self) -> Dict[str, Any]:
        """Perform cache optimization operations."""
        optimization_results = {}
        
        # Clean up expired entries
        current_time = time.time()
        initial_size = len(self.local_cache)
        self._cleanup_expired_entries(current_time)
        cleaned_entries = initial_size - len(self.local_cache)
        optimization_results["expired_entries_cleaned"] = cleaned_entries
        
        # Optimize access patterns (keep only frequently accessed patterns)
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
        optimization_results["access_patterns_cleaned"] = pattern_cleanup
        
        # Preload frequently accessed codes that aren't cached
        frequent_codes = [
            code for code, count in self.access_frequency.most_common(50)
            if count >= 10 and not self._get_from_local_cache(code)
        ]
        
        if frequent_codes:
            try:
                await self.get_many(frequent_codes[:20])  # Limit preload size
                optimization_results["frequent_codes_preloaded"] = len(frequent_codes[:20])
            except Exception as e:
                optimization_results["preload_error"] = str(e)
        
        # Update metrics
        metrics.inc("rvu_cache_optimizations")
        
        return optimization_results

    def reset_stats(self) -> None:
        """Reset performance statistics."""
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
        
        metrics.inc("rvu_cache_stats_resets")