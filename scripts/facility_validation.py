#!/usr/bin/env python3
"""
Facility Validation Service
Provides efficient facility validation against SQL Server with caching.
This replaces the need for facilities table in PostgreSQL.
"""

import asyncio
import logging
from typing import Dict, Optional, Set
from datetime import datetime, timedelta

from src.db.sql_server import SQLServerDatabase


class FacilityValidationService:
    """
    Service for validating facility IDs against SQL Server with intelligent caching.
    
    Features:
    - In-memory LRU cache for frequent lookups
    - Batch validation for performance
    - Connection pooling for SQL Server
    - Cache warming for common facilities
    - Automatic cache refresh
    """
    
    def __init__(self, sql_db: SQLServerDatabase, cache_size: int = 1000, cache_ttl_minutes: int = 60):
        self.sql_db = sql_db
        self.logger = logging.getLogger(__name__)
        
        # Cache configuration
        self.cache_size = cache_size
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        
        # Cache storage
        self._facility_cache: Dict[str, Dict] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_access_order: list = []
        
        # Performance metrics
        self.cache_hits = 0
        self.cache_misses = 0
        self.total_validations = 0
        
    async def validate_facility_id(self, facility_id: str) -> bool:
        """
        Validate a single facility ID.
        Returns True if facility exists and is active.
        """
        self.total_validations += 1
        
        # Check cache first
        if self._is_cached_and_fresh(facility_id):
            self.cache_hits += 1
            facility = self._facility_cache[facility_id]
            return facility.get('active', False)
        
        # Cache miss - query SQL Server
        self.cache_misses += 1
        facility = await self._fetch_facility_from_db(facility_id)
        
        if facility:
            self._update_cache(facility_id, facility)
            return facility.get('active', False)
        
        # Cache negative result to avoid repeated queries
        self._update_cache(facility_id, {'active': False, 'exists': False})
        return False
    
    async def validate_facility_ids_batch(self, facility_ids: Set[str]) -> Dict[str, bool]:
        """
        Validate multiple facility IDs efficiently.
        Returns dict mapping facility_id -> is_valid.
        """
        results = {}
        uncached_ids = set()
        
        # Check cache for each facility
        for facility_id in facility_ids:
            if self._is_cached_and_fresh(facility_id):
                self.cache_hits += 1
                facility = self._facility_cache[facility_id]
                results[facility_id] = facility.get('active', False)
            else:
                uncached_ids.add(facility_id)
        
        # Batch fetch uncached facilities
        if uncached_ids:
            self.cache_misses += len(uncached_ids)
            facilities = await self._fetch_facilities_batch_from_db(uncached_ids)
            
            for facility_id in uncached_ids:
                if facility_id in facilities:
                    facility = facilities[facility_id]
                    self._update_cache(facility_id, facility)
                    results[facility_id] = facility.get('active', False)
                else:
                    # Cache negative result
                    self._update_cache(facility_id, {'active': False, 'exists': False})
                    results[facility_id] = False
        
        self.total_validations += len(facility_ids)
        return results
    
    async def get_facility_details(self, facility_id: str) -> Optional[Dict]:
        """
        Get complete facility details for a given facility ID.
        Returns None if facility doesn't exist.
        """
        if self._is_cached_and_fresh(facility_id):
            facility = self._facility_cache[facility_id]
            return facility if facility.get('exists', True) else None
        
        facility = await self._fetch_facility_from_db(facility_id)
        if facility:
            self._update_cache(facility_id, facility)
            return facility
        
        self._update_cache(facility_id, {'active': False, 'exists': False})
        return None
    
    async def warm_cache(self, facility_ids: Optional[Set[str]] = None):
        """
        Pre-load facility cache with commonly used facilities.
        If facility_ids not provided, loads all active facilities.
        """
        self.logger.info("Warming facility validation cache...")
        
        if facility_ids:
            # Warm specific facilities
            await self.validate_facility_ids_batch(facility_ids)
            self.logger.info(f"Warmed cache with {len(facility_ids)} specific facilities")
        else:
            # Warm with all active facilities
            query = """
                SELECT facility_id, facility_name, facility_type, active, 
                       city, state, region_id
                FROM facilities 
                WHERE active = 1
            """
            
            try:
                results = await self.sql_db.fetch(query)
                for row in results:
                    facility_data = dict(row)
                    facility_data['exists'] = True
                    self._update_cache(facility_data['facility_id'], facility_data)
                
                self.logger.info(f"Warmed cache with {len(results)} active facilities")
            except Exception as e:
                self.logger.error(f"Failed to warm facility cache: {e}")
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics."""
        hit_rate = (self.cache_hits / max(self.total_validations, 1)) * 100
        
        return {
            'cache_size': len(self._facility_cache),
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'total_validations': self.total_validations,
            'hit_rate_percent': round(hit_rate, 2),
            'cached_facilities': list(self._facility_cache.keys())
        }
    
    def clear_cache(self):
        """Clear the entire cache."""
        self._facility_cache.clear()
        self._cache_timestamps.clear()
        self._cache_access_order.clear()
        self.logger.info("Facility validation cache cleared")
    
    async def _fetch_facility_from_db(self, facility_id: str) -> Optional[Dict]:
        """Fetch a single facility from SQL Server."""
        query = """
            SELECT facility_id, facility_name, facility_type, active, 
                   city, state, region_id, beds, phone
            FROM facilities 
            WHERE facility_id = ?
        """
        
        try:
            result = await self.sql_db.fetch_one(query, facility_id)
            if result:
                facility_data = dict(result)
                facility_data['exists'] = True
                return facility_data
        except Exception as e:
            self.logger.error(f"Failed to fetch facility {facility_id}: {e}")
        
        return None
    
    async def _fetch_facilities_batch_from_db(self, facility_ids: Set[str]) -> Dict[str, Dict]:
        """Fetch multiple facilities from SQL Server efficiently."""
        if not facility_ids:
            return {}
        
        # Create parameterized query for batch fetch
        placeholders = ','.join(['?' for _ in facility_ids])
        query = f"""
            SELECT facility_id, facility_name, facility_type, active, 
                   city, state, region_id, beds, phone
            FROM facilities 
            WHERE facility_id IN ({placeholders})
        """
        
        try:
            results = await self.sql_db.fetch(query, *facility_ids)
            facilities = {}
            for row in results:
                facility_data = dict(row)
                facility_data['exists'] = True
                facilities[facility_data['facility_id']] = facility_data
            
            return facilities
        except Exception as e:
            self.logger.error(f"Failed to batch fetch facilities: {e}")
            return {}
    
    def _is_cached_and_fresh(self, facility_id: str) -> bool:
        """Check if facility is cached and not expired."""
        if facility_id not in self._facility_cache:
            return False
        
        timestamp = self._cache_timestamps.get(facility_id)
        if not timestamp:
            return False
        
        return datetime.now() - timestamp < self.cache_ttl
    
    def _update_cache(self, facility_id: str, facility_data: Dict):
        """Update cache with LRU eviction."""
        # Remove if already exists (for LRU reordering)
        if facility_id in self._facility_cache:
            self._cache_access_order.remove(facility_id)
        
        # Add to cache
        self._facility_cache[facility_id] = facility_data
        self._cache_timestamps[facility_id] = datetime.now()
        self._cache_access_order.append(facility_id)
        
        # Evict oldest if cache is full
        while len(self._facility_cache) > self.cache_size:
            oldest_id = self._cache_access_order.pop(0)
            self._facility_cache.pop(oldest_id, None)
            self._cache_timestamps.pop(oldest_id, None)


# Global service instance (initialized in main application)
facility_service: Optional[FacilityValidationService] = None


async def initialize_facility_service(sql_db: SQLServerDatabase) -> FacilityValidationService:
    """Initialize the global facility validation service."""
    global facility_service
    facility_service = FacilityValidationService(sql_db)
    
    # Warm cache with active facilities
    await facility_service.warm_cache()
    
    return facility_service


def get_facility_service() -> FacilityValidationService:
    """Get the global facility validation service."""
    if facility_service is None:
        raise RuntimeError("Facility service not initialized. Call initialize_facility_service() first.")
    return facility_service


# Example usage functions
async def validate_claim_facility(facility_id: str) -> bool:
    """Convenience function for claim validation."""
    service = get_facility_service()
    return await service.validate_facility_id(facility_id)


async def validate_batch_facilities(facility_ids: Set[str]) -> Dict[str, bool]:
    """Convenience function for batch claim validation."""
    service = get_facility_service()
    return await service.validate_facility_ids_batch(facility_ids)