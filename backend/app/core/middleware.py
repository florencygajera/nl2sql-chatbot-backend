"""
Performance Monitoring Middleware.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional, List

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class RequestMetrics:
    method: str
    path: str
    status_code: int
    duration_ms: float
    timestamp: float


class PerformanceMonitor:
    _instance: Optional["PerformanceMonitor"] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_history: int = 10000):
        if self._initialized:
            return
        self.max_history = max_history
        self._requests: deque = deque(maxlen=max_history)
        self._start_time = time.time()
        self._initialized = True

    def record_request(self, metrics: RequestMetrics) -> None:
        self._requests.append(metrics)
        if metrics.duration_ms > 5000:
            logger.warning(
                "SLOW REQUEST | %s %s | %.2fms | status=%d",
                metrics.method, metrics.path, metrics.duration_ms, metrics.status_code
            )

    def get_summary(self, window_seconds: int = 300) -> dict:
        cutoff = time.time() - window_seconds
        recent = [r for r in self._requests if r.timestamp >= cutoff]
        if not recent:
            return {"requests": 0, "message": "No requests in time window"}
        durations = [r.duration_ms for r in recent]
        errors = [r for r in recent if r.status_code >= 400]
        sorted_durations = sorted(durations)
        p50 = sorted_durations[int(len(sorted_durations) * 0.5)]
        p95 = sorted_durations[int(len(sorted_durations) * 0.95)]
        return {
            "window_seconds": window_seconds,
            "requests": len(recent),
            "requests_per_second": round(len(recent) / window_seconds, 2),
            "errors": len(errors),
            "error_rate_percent": round(len(errors) / len(recent) * 100, 2) if recent else 0,
            "latency_ms": {
                "min": round(min(durations), 2),
                "avg": round(sum(durations) / len(durations), 2),
                "max": round(max(durations), 2),
                "p50": round(p50, 2),
                "p95": round(p95, 2),
            },
            "uptime_seconds": round(time.time() - self._start_time, 0),
        }


class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, exclude_paths: Optional[List[str]] = None):
        super().__init__(app)
        self.exclude_paths = exclude_paths or ["/health", "/metrics", "/favicon.ico"]
        self.monitor = PerformanceMonitor()

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path
        if any(path.startswith(ep) for ep in self.exclude_paths):
            return await call_next(request)
        start_time = time.time()
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            status_code = 500
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            metrics = RequestMetrics(
                method=request.method,
                path=path,
                status_code=status_code,
                duration_ms=duration_ms,
                timestamp=time.time(),
            )
            self.monitor.record_request(metrics)
            if settings.DEBUG:
                response.headers["X-Response-Time-Ms"] = str(round(duration_ms, 2))
        return response


def get_monitor() -> PerformanceMonitor:
    return PerformanceMonitor()
