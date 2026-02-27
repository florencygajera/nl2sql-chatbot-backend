"""
Pydantic schemas for API request/response validation.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


# ── Request ───────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str = Field(
        ...,
        min_length=1,
        max_length=2000,
        description="Natural language question about the database.",
        examples=["Total salary paid to all employees"],
    )


# ── DB result sub-schema ──────────────────────────────────────────────────────

class QueryResultSchema(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


# ── Response union ────────────────────────────────────────────────────────────

class DBResponse(BaseModel):
    type: Literal["DB"] = "DB"
    mode: Literal["QUERY_ONLY", "ANSWER_ONLY", "QUERY_AND_ANSWER"]
    sql: str | None = None
    params: dict[str, Any] | None = None
    explanation: str = ""
    result: QueryResultSchema | None = None
    answer_text: str | None = None
    error: str | None = None


class ChatResponse(BaseModel):
    type: Literal["CHAT"] = "CHAT"
    answer: str


class ClarifyResponse(BaseModel):
    type: Literal["CLARIFY"] = "CLARIFY"
    question: str
    missing: list[str] = []


# ── Health check ──────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    database: str
    version: str
