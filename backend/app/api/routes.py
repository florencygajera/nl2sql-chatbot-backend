"""
API Routes — /health and /chat endpoints.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.schemas import (
    ChatRequest,
    ChatResponse,
    ClarifyResponse,
    DBResponse,
    HealthResponse,
)
from app.core.config import get_settings
from app.db.session import get_db, ping_database
from app.services.chat_service import handle_chat
from app.api.upload_sql import router as upload_sql_router
from app.api.db_routes import router as db_router

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter()
router.include_router(db_router)
router.include_router(upload_sql_router)

_APP_VERSION = "1.0.0"


# ─────────────────────────────────────── /health ───────────────────────────────────────────────────────────────────

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
    db: Session = Depends(get_db),
) -> dict:
    """
    Main endpoint: converts a natural-language question into SQL,
    executes it, and returns the result according to the detected mode.

    **Mode detection:**
    - Contains "sql" / "query" / "command only" → `QUERY_ONLY`
    - Contains "only answer" → `ANSWER_ONLY`
    - Otherwise → `QUERY_AND_ANSWER`
    """
    try:
        response = await handle_chat(message=request.message, db=db)
        return response
    except Exception as exc:
        logger.exception("Unhandled error in /chat: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {exc}",
        ) from exc
