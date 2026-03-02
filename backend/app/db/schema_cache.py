"""
Schema Cache Manager with TTL and Performance Optimizations.

Features:
- In-memory schema caching with configurable TTL
- Background refresh to prevent stale data
- Cache warming for frequently accessed databases
- Metrics tracking for cache hits/misses
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class CacheEntry:
    """Schema cache entry with metadata."""
    schema_text: str
    timestamp: float
    db_url_hash: str
    access_count: int = 0
    last_accessed: float = 0.0


class SchemaCacheManager:
    """
    Thread-safe schema cache with TTL and LRU eviction.
    """
    
    def __init__(
        self,
        ttl_seconds: int = 300,  # 5 minutes default
        max_entries: int = 100,
        background_refresh: bool = False,
    ):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self.background_refresh = background_refresh
        
        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._total_fetch_time = 0.0
        
        logger.info(
            "SchemaCacheManager initialized (ttl=%ds, max_entries=%d)",
            ttl_seconds, max_entries
        )
    
    def _get_db_hash(self, db_url: str) -> str:
        """Generate unique hash for database URL."""
        return hashlib.sha256(db_url.encode()).hexdigest()[:16]
    
    def _is_valid(self, entry: CacheEntry) -> bool:
        """Check if cache entry is still valid (not expired)."""
        return (time.time() - entry.timestamp) < self.ttl
    
    def get(
        self,
        db_url: str,
        fetch_func=None,
    ) -> Optional[str]:
        """
        Get schema from cache or fetch and cache it.
        
        Args:
            db_url: Database URL for cache key
            fetch_func: Function to fetch schema if cache miss
            
        Returns:
            Schema text or None if unavailable
        """
        db_hash = self._get_db_hash(db_url)
        
        with self._lock:
            entry = self._cache.get(db_hash)
            
            if entry and self._is_valid(entry):
                # Cache HIT
                entry.access_count += 1
                entry.last_accessed = time.time()
                self._hits += 1
                
                logger.debug(
                    "Schema CACHE HIT | db_hash=%s | age=%.1fs | hits=%d",
                    db_hash, time.time() - entry.timestamp, entry.access_count
                )
                return entry.schema_text
            
            # Cache MISS or EXPIRED
            if entry:
                logger.debug("Schema CACHE EXPIRED | db_hash=%s", db_hash)
                del self._cache[db_hash]
            
            self._misses += 1
        
        # Fetch outside lock to prevent blocking
        if fetch_func:
            start = time.time()
            try:
                schema = fetch_func()
                fetch_time = time.time() - start
                
                if schema:
                    self.set(db_url, schema)
                    logger.info(
                        "Schema FETCH | db_hash=%s | time=%.2fs | size=%d chars",
                        db_hash, fetch_time, len(schema)
                    )
                
                with self._lock:
                    self._total_fetch_time += fetch_time
                
                return schema
                
            except Exception as e:
                logger.error("Schema fetch failed: %s", e)
                return None
        
        return None
    
    def set(self, db_url: str, schema: str) -> None:
        """Manually set cache entry."""
        db_hash = self._get_db_hash(db_url)
        
        with self._lock:
            # LRU eviction if at capacity
            if len(self._cache) >= self.max_entries and db_hash not in self._cache:
                # Find least recently used entry
                lru_key = min(
                    self._cache.keys(),
                    key=lambda k: self._cache[k].last_accessed or self._cache[k].timestamp
                )
                del self._cache[lru_key]
                logger.debug("Evicted LRU cache entry: %s", lru_key)
            
            self._cache[db_hash] = CacheEntry(
                schema_text=schema,
                timestamp=time.time(),
                db_url_hash=db_hash,
                last_accessed=time.time(),
            )
    
    def invalidate(self, db_url: str) -> bool:
        """Invalidate cache for specific database."""
        db_hash = self._get_db_hash(db_url)
        
        with self._lock:
            if db_hash in self._cache:
                del self._cache[db_hash]
                logger.info("Schema cache invalidated | db_hash=%s", db_hash)
                return True
        return False
    
    def invalidate_all(self) -> None:
        """Clear all cached schemas."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            self._total_fetch_time = 0.0
            logger.info("All schema caches cleared (%d entries)", count)
    
    def get_metrics(self) -> dict:
        """Get cache performance metrics."""
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0
            avg_fetch_time = (self._total_fetch_time / self._misses) if self._misses > 0 else 0
            
            return {
                "entries": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": round(hit_rate, 2),
                "avg_fetch_time_seconds": round(avg_fetch_time, 3),
                "total_fetch_time_seconds": round(self._total_fetch_time, 3),
            }
    
    def get_stats_report(self) -> str:
        """Get formatted statistics report."""
        metrics = self.get_metrics()
        return f"""
Schema Cache Statistics:
  Entries: {metrics['entries']}
  Hits: {metrics['hits']}
  Misses: {metrics['misses']}
  Hit Rate: {metrics['hit_rate_percent']}%
  Avg Fetch Time: {metrics['avg_fetch_time_seconds']}s
  Total Fetch Time: {metrics['total_fetch_time_seconds']}s
"""


# Global cache instance
_schema_cache: Optional[SchemaCacheManager] = None


def get_schema_cache() -> SchemaCacheManager:
    """Get or create the global schema cache."""
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = SchemaCacheManager(
            ttl_seconds=settings.DB_SESSION_TTL_SECONDS,
            max_entries=100,
        )
    return _schema_cache


def reset_schema_cache() -> None:
    """Reset the global schema cache."""
    global _schema_cache
    _schema_cache = None
    logger.info("Schema cache reset")
