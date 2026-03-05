"""
Application entry point.

Run with:
    uvicorn app.main:app --reload
"""
from __future__ import annotations

import logging
import logging.config

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import router
from app.core.config import get_settings
from app.core.middleware import PerformanceMonitoringMiddleware

settings = get_settings()
import aiohttp
import asyncio
from app.core.config import validate_settings
from app.db.connection_manager import cleanup_idle_connections

_cleanup_task: asyncio.Task | None = None

@app.on_event("startup")    
async def _startup():
    validate_settings(settings)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ── App factory ───────────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    application = FastAPI(
        title=settings.APP_NAME,
        description=(
            "A production-ready Natural Language to SQL chatbot backend.\n\n"
            "Users type plain-English questions about an employee database and "
            "receive SQL queries and/or structured results.\n\n"
            "## Performance Optimizations\n"
            "- Async LLM calls with connection pooling\n"
            "- Schema caching with TTL\n"
            "- Request performance monitoring\n"
            "- Response caching for identical prompts"
        ),
        version="1.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # ── Performance Monitoring ────────────────────────────────────────────────
    if settings.ENABLE_PERFORMANCE_MONITORING:
        application.add_middleware(PerformanceMonitoringMiddleware)
        logger.info("Performance monitoring middleware enabled")

    # ── CORS ──────────────────────────────────────────────────────────────────
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routes ────────────────────────────────────────────────────────────────
    application.include_router(router, prefix="/api/v1")

    # ── Root redirect ─────────────────────────────────────────────────────────
    @application.get("/", include_in_schema=False)
    def root() -> JSONResponse:
        return JSONResponse(
            {
                "service": settings.APP_NAME,
                "docs": "/docs",
                "health": "/api/v1/health",
                "chat": "/api/v1/chat",
            }
        )

    logger.info("✅ %s started (debug=%s)", settings.APP_NAME, settings.DEBUG)
    return application


app = create_app()
