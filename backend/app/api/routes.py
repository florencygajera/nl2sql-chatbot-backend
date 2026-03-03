"""
API Routes — /health and /chat endpoints.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status
from app.api.schemas import (
    ChatRequest,
    ChatResponse,
    ClarifyResponse,
    DBResponse,
    HealthResponse,
)
from app.core.config import get_settings
from app.core.middleware import get_monitor
from app.db.schema_cache import get_schema_cache
from app.db.session import ping_database, SessionLocal, set_database_url, reset_database_url, set_database_source, active_db_info
from app.services.chat_service import handle_chat
from app.services.db_session import get_session, cleanup_expired
from app.api.upload_sql import router as upload_sql_router
from app.api.db_routes import router as db_router
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter()
router.include_router(db_router)
router.include_router(upload_sql_router)

_APP_VERSION = "1.0.0"


# ───────────────────────────────────────
# /health
# ───────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    tags=["Monitoring"],
)
def health_check() -> HealthResponse:
    """
    Returns the operational status of the API and its database connection.
    """
    db_ok = ping_database()
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        database="connected" if db_ok else "unreachable",
        version=_APP_VERSION,
    )


# ── /chat ─────────────────────────────────────────────────────────────────────

@router.post(
    "/chat",
    summary="Natural Language to SQL Chat",
    tags=["Chat"],
    responses={
        200: {
            "description": "Successful response — DB query result, chat reply, or clarification.",
            "content": {
                "application/json": {
                    "examples": {
                        "db_response": {
                            "summary": "DB query executed",
                            "value": {
                                "type": "DB",
                                "mode": "QUERY_AND_ANSWER",
                                "sql": "SELECT SUM(salary) AS total FROM employees e",
                                "params": {},
                                "explanation": "Sums all employee salaries.",
                                "result": {"columns": ["total"], "rows": [[4250000]], "row_count": 1},
                                "answer_text": "Sums all employee salaries. Result: total = 4250000",
                            },
                        },
                        "chat_response": {
                            "summary": "General chat",
                            "value": {"type": "CHAT", "answer": "Hi! How can I help you?"},
                        },
                    }
                }
            },
        },
        422: {"description": "Validation error"},
        500: {"description": "Internal server error"},
    },
)
async def chat(
    request: ChatRequest,
) -> dict:
    """
    Main endpoint: converts a natural-language question into SQL,
    executes it, and returns the result according to the detected mode.

    Dynamic DB behavior:
    - If db_session_id is provided, it attaches that DB only for this request
      and auto-detaches in finally.
    """
    try:
        # Optional: cleanup idle DB sessions (in-memory)
        cleanup_expired(settings.DB_SESSION_TTL_SECONDS)

        if request.db_session_id:
            sess = get_session(request.db_session_id)
            if not sess:
                raise HTTPException(status_code=400, detail="DB session expired or invalid. Please upload/connect again.")

            # Attach DB only for this request
            await run_in_threadpool(set_database_url, sess.db_url)
            await run_in_threadpool(
                set_database_source,
                attach_mode="SESSION",
                db_type=sess.source.get("db_type", "unknown"),
                details=sess.source.get("details", {}),
            )

            db = SessionLocal()
            try:
                response = await handle_chat(message=request.message, db=db)
                return response
            finally:
                await run_in_threadpool(db.close)
                # Always detach back to env default
                await run_in_threadpool(reset_database_url)
        else:
            # Fallback: use current default DB configured in environment
            db = SessionLocal()
            try:
                response = await handle_chat(message=request.message, db=db)
                return response
            finally:
                await run_in_threadpool(db.close)

    except Exception as exc:
        import traceback
        logger.exception("Unhandled error in /chat: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {traceback.format_exc()}",
        ) from exc


# ── Performance Monitoring Endpoints ─────────────────────────────────────────

@router.get("/metrics", tags=["Monitoring"])
def get_metrics(window_seconds: int = 300) -> dict:
    """Get application performance metrics."""
    if not settings.ENABLE_PERFORMANCE_MONITORING:
        return {"error": "Performance monitoring is disabled"}
    
    monitor = get_monitor()
    return monitor.get_summary(window_seconds=window_seconds)


@router.get("/metrics/schema-cache", tags=["Monitoring"])
def get_schema_cache_metrics() -> dict:
    """Get schema cache statistics."""
    if not settings.ENABLE_SCHEMA_CACHE:
        return {"error": "Schema caching is disabled"}
    
    cache = get_schema_cache()
    return cache.get_metrics()


@router.post("/metrics/clear-cache", tags=["Monitoring"])
def clear_caches() -> dict:
    """Clear all caches (schema and LLM)."""
    cache = get_schema_cache()
    cache.invalidate_all()
    
    from app.llm.async_ollama_client import get_ollama_client
    client = get_ollama_client()
    if client:
        client.clear_cache()
    
    return {"message": "All caches cleared"}