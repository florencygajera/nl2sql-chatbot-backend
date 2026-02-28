"""
Chat Service — orchestrates the full NL→SQL→Result pipeline.

Flow
----
1. Detect response mode (QUERY_ONLY / ANSWER_ONLY / QUERY_AND_ANSWER).
2. Fetch live DB schema summary.
3. Call the local LLM to generate SQL.
4. Validate SQL.
5. Execute query.
6. Build and return response.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.orm import Session

from app.db.session import get_schema_summary
from app.llm.nl2sql import generate_sql
from app.security.sql_guard import SQLGuardError, validate_and_sanitize
from app.services.query_executor import QueryExecutionError, QueryResult, execute_query

logger = logging.getLogger(__name__)


# ── Response builders ─────────────────────────────────────────────────────────

def _build_db_response(
    mode: str,
    sql: str,
    explanation: str,
    result: QueryResult | None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "type": "DB",
        "mode": mode,
        "explanation": explanation,
    }

    if mode in ("QUERY_ONLY", "QUERY_AND_ANSWER"):
        response["sql"] = sql
        response["params"] = {}

    if mode in ("ANSWER_ONLY", "QUERY_AND_ANSWER") and result is not None:
        response["result"] = {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
        }
        response["answer_text"] = _generate_answer_text(result, explanation)

    return response


def _generate_answer_text(result: QueryResult, explanation: str) -> str:
    """Produce a brief human-readable summary of the query result."""
    if result.row_count == 0:
        return "The query returned no results."

    if result.row_count == 1 and len(result.columns) == 1:
        val = result.rows[0][0]
        col = result.columns[0]
        return f"{explanation} Result: {col} = {val}"

    summary_parts = [
        f"Found {result.row_count} row(s).",
        f"Columns: {', '.join(result.columns)}.",
    ]
    if result.row_count <= 5:
        for row in result.rows:
            summary_parts.append(
                "  • " + ", ".join(f"{c}: {v}" for c, v in zip(result.columns, row))
            )
    else:
        summary_parts.append("First 3 rows:")
        for row in result.rows[:3]:
            summary_parts.append(
                "  • " + ", ".join(f"{c}: {v}" for c, v in zip(result.columns, row))
            )
        summary_parts.append(f"  … and {result.row_count - 3} more.")

    return "\n".join(summary_parts)


def _detect_response_mode(user_message: str) -> str:
    """Detect what kind of response the user expects."""
    lower = user_message.lower()

    query_keywords = [
        "sql", "query", "command only", "just the query",
        "show me the query", "give me the sql", "only the sql",
        "only sql", "sql only", "raw query",
    ]
    answer_keywords = [
        "only answer", "just the answer", "only the answer",
        "just answer", "answer only", "no sql", "without sql",
    ]

    if any(kw in lower for kw in query_keywords):
        return "QUERY_ONLY"
    if any(kw in lower for kw in answer_keywords):
        return "ANSWER_ONLY"
    return "QUERY_AND_ANSWER"


# ── Main service function ─────────────────────────────────────────────────────

async def handle_chat(message: str, db: Session) -> dict[str, Any]:
    """
    Process a user message end-to-end and return a JSON-serialisable response.
    """
    # ── Step 1: Detect desired response mode ──────────────────────────────────
    mode = _detect_response_mode(message)
    logger.info("User message: %r | Mode: %s", message, mode)

    # ── Step 2: Fetch DB schema ───────────────────────────────────────────────
    try:
        schema = get_schema_summary()
    except Exception as exc:
        logger.warning("Could not fetch schema: %s", exc)
        schema = "Schema unavailable."

    # ── Step 3: Call LLM to generate SQL ──────────────────────────────────────
    try:
        raw_sql = generate_sql(message, schema)
    except Exception as exc:
        logger.error("LLM error: %s", exc)
        return {
            "type": "CHAT",
            "answer": (
                "I encountered an error communicating with the AI model. "
                f"Details: {exc}"
            ),
        }

    # Check if we need clarification on SQL dialect
    if "Please specify which SQL dialect" in raw_sql:
        return {
            "type": "CHAT",
            "answer": raw_sql,
        }

    if not raw_sql.strip():
        return {
            "type": "CHAT",
            "answer": "I wasn't able to generate a SQL query for that question. Could you rephrase?",
        }

    explanation = f"Generated SQL for: {message}"

    # ── Step 4: Validate SQL ──────────────────────────────────────────────────
    try:
        validation = validate_and_sanitize(raw_sql)
        safe_sql = validation.sanitized_sql
    except SQLGuardError as exc:
        logger.warning("SQL guard rejected query: %s | SQL: %s", exc, raw_sql)
        return {
            "type": "CHAT",
            "answer": (
                f"The generated SQL did not pass security validation: {exc}. "
                "Please rephrase your question."
            ),
        }

    # ── Step 5: Execute (unless QUERY_ONLY) ───────────────────────────────────
    result: QueryResult | None = None

    if mode != "QUERY_ONLY":
        try:
            result = execute_query(db, safe_sql, {})
        except QueryExecutionError as exc:
            logger.error("Query execution failed: %s", exc)
            return {
                "type": "DB",
                "mode": mode,
                "sql": safe_sql,
                "params": {},
                "explanation": explanation,
                "error": str(exc),
                "answer_text": f"Query failed: {exc}",
            }

    # ── Step 6: Build final response ──────────────────────────────────────────
    return _build_db_response(
        mode=mode,
        sql=safe_sql,
        explanation=explanation,
        result=result,
    )
