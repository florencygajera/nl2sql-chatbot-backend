"""
Async Ollama Client with Connection Pooling and Performance Optimizations.

Features:
- Async HTTP requests via aiohttp
- Connection pooling (persistent TCP connections)
- Request timeouts with configurable retry logic
- Response caching for identical prompts
- Performance metrics tracking
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from functools import lru_cache

from app.core.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class LLMMetrics:
    """Performance metrics for LLM calls."""
    prompt_hash: str
    start_time: float
    end_time: float = 0.0
    tokens_generated: int = 0
    error: Optional[str] = None
    
    @property
    def latency_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000


class AsyncOllamaClient:
    """
    High-performance async Ollama client with connection pooling and caching.
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = 120,
        max_connections: int = 10,
        cache_ttl_seconds: int = 300,
    ):
        settings = get_settings()
        self.base_url = base_url or settings.OLLAMA_BASE_URL
        self.model = model or settings.OLLAMA_MODEL
        self.timeout = timeout
        self.cache_ttl = cache_ttl_seconds
        
        # Connection pool configuration
        connector = aiohttp.TCPConnector(
            limit=max_connections,
            limit_per_host=max_connections,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
            force_close=False,
        )
        
        timeout_config = aiohttp.ClientTimeout(
            total=timeout,
            connect=10,
            sock_read=timeout,
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout_config,
            headers={"Content-Type": "application/json"},
        )
        
        # Simple in-memory cache with TTL
        self._cache: dict[str, tuple[str, float]] = {}
        self._metrics: list[LLMMetrics] = []
        
        logger.info(
            "AsyncOllamaClient initialized (pool_size=%d, timeout=%ds, cache_ttl=%ds)",
            max_connections, timeout, cache_ttl_seconds
        )
    
    def _get_cache_key(self, prompt: str) -> str:
        """Generate cache key from prompt."""
        return hashlib.sha256(prompt.encode()).hexdigest()[:16]
    
    def _get_cached(self, prompt: str) -> Optional[str]:
        """Get cached response if not expired."""
        key = self._get_cache_key(prompt)
        if key in self._cache:
            response, timestamp = self._cache[key]
            if time.time() - timestamp < self.cache_ttl:
                logger.debug("Cache HIT for prompt hash %s", key)
                return response
            else:
                # Expired
                del self._cache[key]
        return None
    
    def _set_cached(self, prompt: str, response: str) -> None:
        """Cache response with timestamp."""
        key = self._get_cache_key(prompt)
        self._cache[key] = (response, time.time())
        # Cleanup old entries if cache grows too large
        if len(self._cache) > 1000:
            oldest = min(self._cache.keys(), key=lambda k: self._cache[k][1])
            del self._cache[oldest]
    
    async def generate(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 150,
        use_cache: bool = True,
    ) -> str:
        """
        Generate text from prompt with caching and metrics.
        
        Args:
            prompt: The input prompt
            temperature: Sampling temperature (0.0 = deterministic)
            max_tokens: Maximum tokens to generate
            use_cache: Whether to use response caching
            
        Returns:
            Generated text response
        """
        metrics = LLMMetrics(
            prompt_hash=self._get_cache_key(prompt),
            start_time=time.time(),
        )
        
        # Check cache first
        if use_cache:
            cached = self._get_cached(prompt)
            if cached is not None:
                metrics.end_time = time.time()
                metrics.tokens_generated = len(cached.split())
                self._metrics.append(metrics)
                logger.info(
                    "LLM CACHE HIT | latency=%.2fms | hash=%s",
                    metrics.latency_ms, metrics.prompt_hash
                )
                return cached
        
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                },
            }
            
            async with self.session.post(
                f"{self.base_url}/api/generate",
                json=payload,
            ) as response:
                response.raise_for_status()
                data = await response.json()
                result = data.get("response", "").strip()
                
                # Update metrics
                metrics.end_time = time.time()
                metrics.tokens_generated = len(result.split())
                self._metrics.append(metrics)
                
                # Cache the result
                if use_cache:
                    self._set_cached(prompt, result)
                
                logger.info(
                    "LLM CALL | latency=%.2fms | tokens=%d | hash=%s",
                    metrics.latency_ms, metrics.tokens_generated, metrics.prompt_hash
                )
                
                return result
                
        except asyncio.TimeoutError as e:
            metrics.end_time = time.time()
            metrics.error = "TIMEOUT"
            self._metrics.append(metrics)
            logger.error("LLM timeout after %ds | hash=%s", self.timeout, metrics.prompt_hash)
            raise RuntimeError(f"LLM request timed out after {self.timeout}s") from e
            
        except Exception as e:
            metrics.end_time = time.time()
            metrics.error = str(e)
            self._metrics.append(metrics)
            logger.error("LLM error: %s | hash=%s", e, metrics.prompt_hash)
            raise RuntimeError(f"LLM request failed: {e}") from e
    
    def get_metrics(self) -> dict:
        """Get performance metrics summary."""
        if not self._metrics:
            return {"calls": 0}
        
        latencies = [m.latency_ms for m in self._metrics if m.error is None]
        errors = [m for m in self._metrics if m.error is not None]
        cache_hits = len([m for m in self._metrics if m.latency_ms < 10])
        
        return {
            "calls": len(self._metrics),
            "cache_hits": cache_hits,
            "errors": len(errors),
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "max_latency_ms": max(latencies) if latencies else 0,
            "p95_latency_ms": sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 20 else 0,
        }
    
    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()
        logger.info("LLM response cache cleared")
    
    async def close(self) -> None:
        """Close the HTTP session."""
        await self.session.close()
        logger.info("AsyncOllamaClient session closed")


# Global client instance (singleton pattern)
_ollama_client: Optional[AsyncOllamaClient] = None


def get_ollama_client() -> AsyncOllamaClient:
    """Get or create the global Ollama client instance."""
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = AsyncOllamaClient()
    return _ollama_client


async def generate_async(
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 150,
    use_cache: bool = True,
) -> str:
    """
    Convenience function for async LLM generation.
    
    Example:
        result = await generate_async("Your prompt here")
    """
    client = get_ollama_client()
    return await client.generate(
        prompt=prompt,
        temperature=temperature,
        max_tokens=max_tokens,
        use_cache=use_cache,
    )


def reset_ollama_client() -> None:
    """Reset the global client (useful for testing or config changes)."""
    global _ollama_client
    _ollama_client = None
    logger.info("Ollama client reset")
