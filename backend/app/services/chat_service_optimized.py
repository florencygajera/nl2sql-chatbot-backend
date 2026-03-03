"""
Optimized Chat Service — High-Performance NL→SQL→Result Pipeline.

Performance Optimizations:
- Async LLM calls with connection pooling
- Schema caching with TTL
- Concurrent operations where possible
- Detailed timing metrics

Flow
----
1. Detect response mode (QUERY_ONLY / ANSWER_ONLY / QUERY_AND_ANSWER).
2. Fetch cached DB schema (or fetch and cache).
3. Call async LLM to generate SQL.
4. Validate SQL (dialect-aware).
5. Execute query (dialect-aware).
6. Build and return response with timing data.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.schema_cache import get_schema_cache
from app.db.session import (
    get_schema_summary,
    get_schema_catalog,
    get_schema_for_tables,
    get_current_dialect,
    active_db_info,
)
from app.db.schema_retriever import parse_schema_summary, select_relevant_tables

from app.llm.async_ollama_client import generate_async
from app.llm.nl2sql import (
    _normalize_llm_sql,
    _build_mssql_prompt,
    _build_postgres_prompt,
    _build_mysql_prompt,
    _build_sqlite_prompt,
    _build_generic_prompt,
)
from app.security.sql_guard import SQLGuardError, validate_and_sanitize
from app.services.query_executor import QueryExecutionError, QueryResult, execute_query

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class TimingMetrics:
    """Request timing breakdown."""
    schema_fetch_ms: float = 0.0
    llm_generation_ms: float = 0.0
    sql_validation_ms: float = 0.0
    query_execution_ms: float = 0.0
    total_ms: float = 0.0
    schema_cached: bool = False




# ── Response builders ─────────────────────────────────────────────────────────

def _build_db_response(
    mode: str,
    sql: str,
    explanation: str,
    result: QueryResult | None,
    timings: Optional[TimingMetrics] = None,
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
    
    # Add timing metrics in debug mode
    if timings and settings.DEBUG:
        response["_timings"] = {
            "schema_fetch_ms": round(timings.schema_fetch_ms, 2),
            "llm_generation_ms": round(timings.llm_generation_ms, 2),
            "sql_validation_ms": round(timings.sql_validation_ms, 2),
            "query_execution_ms": round(timings.query_execution_ms, 2),
            "total_ms": round(timings.total_ms, 2),
            "schema_cached": timings.schema_cached,
        }

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


def _build_prompt(message: str, schema: str, dialect: str) -> str:
    """Build the appropriate prompt based on dialect."""
    dialect_lower = dialect.lower() if dialect else "unknown"
    
    if dialect_lower == "mssql":
        return _build_mssql_prompt(message, schema)
    elif dialect_lower in ("postgresql", "postgres"):
        return _build_postgres_prompt(message, schema)
    elif dialect_lower == "mysql":
        return _build_mysql_prompt(message, schema)
    elif dialect_lower == "sqlite":
        return _build_sqlite_prompt(message, schema)
    else:
        # Try to detect from user message
        user_lower = (message or "").lower()
        if any(k in user_lower for k in ["mssql", "sql server", "sqlserver"]):
            return _build_mssql_prompt(message, schema)
        elif any(k in user_lower for k in ["postgresql", "postgres", "postgre", "pg"]):
            return _build_postgres_prompt(message, schema)
        elif "mysql" in user_lower:
            return _build_mysql_prompt(message, schema)
        elif "sqlite" in user_lower:
            return _build_sqlite_prompt(message, schema)
        else:
            return _build_generic_prompt(message, schema)


# ── Main service function ─────────────────────────────────────────────────────

async def handle_chat_optimized(message: str, db: Session, db_url: Optional[str] = None) -> dict[str, Any]:
    """
    Process a user message end-to-end with performance optimizations.
    
    Args:
        message: User's natural language question
        db: Database session
        db_url: Database URL for schema caching (optional)
        
    Returns:
        Response dict with optional timing metrics
    """
    total_start = time.time()
    timings = TimingMetrics()
    
    # ── Step 0: Detect current DB dialect ─────────────────────────────────────
    dialect = get_current_dialect()
    logger.info("Chat started | dialect=%s | message=%r", dialect, message[:50])

    # ── Step 1: Detect desired response mode ──────────────────────────────────
    mode = _detect_response_mode(message)
    logger.debug("Mode detected: %s", mode)

    # ── Step 2: Fetch DB catalog + select relevant schema (with caching) ─────────
    schema_start = time.time()
    cache = get_schema_cache()

    # 2a) Fetch an *untruncated* catalog for table selection
    if db_url:
        cached_entry = cache._cache.get(cache._get_db_hash(db_url)) if hasattr(cache, "_cache") else None
        catalog = cache.get(db_url, fetch_func=get_schema_catalog) or ""
        timings.schema_cached = bool(cached_entry and cache._is_valid(cached_entry)) if cached_entry else False
    else:
        try:
            catalog = get_schema_catalog()
        except Exception as exc:
            logger.warning("Could not fetch schema catalog: %s", exc)
            catalog = ""
        timings.schema_cached = False

    # 2b) If catalog is missing, fall back to the old summary (still better than nothing)
    if not catalog.strip():
        try:
            catalog = get_schema_summary()
        except Exception as exc:
            logger.warning("Could not fetch schema summary: %s", exc)
            catalog = ""

    # 2c) Select the most relevant tables for this question
    tables = parse_schema_summary(catalog)
    top_k = getattr(settings, "NL2SQL_TOP_TABLES", 10)
    picked_tables = select_relevant_tables(question=message, tables=tables, top_k=top_k)

    # 2d) Fetch full column list for those tables (avoids schema truncation hurting accuracy)
    try:
        schema = get_schema_for_tables(picked_tables)
    except Exception as exc:
        logger.warning("Could not fetch targeted schema: %s", exc)
        schema = catalog

    timings.schema_fetch_ms = (time.time() - schema_start) * 1000
    logger.debug(
        "Schema fetch took %.2fms (cached=%s) | picked_tables=%s",
        timings.schema_fetch_ms,
        timings.schema_cached,
        picked_tables[: min(8, len(picked_tables))],
    )

# ── Step 3: Build prompt + NL→SQL generation (with retry/repair) ────────────
    base_prompt = _build_prompt(message, schema, dialect)

    max_retries = int(getattr(settings, "NL2SQL_MAX_RETRIES", 2))
    llm_tokens = int(getattr(settings, "LLM_MAX_TOKENS", 512))

    raw_sql = ""
    safe_sql = ""
    last_exec_error: Exception | None = None
    result: QueryResult | None = None

    sql_validation_total_ms = 0.0
    query_execution_total_ms = 0.0

    for attempt in range(max_retries + 1):
        # If we already executed and got an error, ask the model to repair.
        if attempt == 0:
            prompt = base_prompt
        else:
            prompt = (
                f"Fix the {dialect} SELECT query using ONLY the schema below. "
                "Output ONLY the corrected SQL SELECT query.\n\n"
                f"Schema:\n{schema}\n\n"
                f"Original question: {message}\n"
                f"Previous SQL: {safe_sql or raw_sql}\n"
                f"Execution error: {last_exec_error}\n"
                "Corrected SQL:"
            )

        # LLM call
        llm_start = time.time()
        try:
            raw_sql = await generate_async(
                prompt=prompt,
                temperature=0.0,
                max_tokens=llm_tokens,
                use_cache=True,
            )
        except Exception as exc:
            logger.error("LLM error: %s", exc)
            return {
                "type": "CHAT",
                "answer": (
                    "I encountered an error communicating with the AI model. "
                    f"Details: {exc}"
                ),
            }
        timings.llm_generation_ms += (time.time() - llm_start) * 1000

        # Quick guardrails
        if not (raw_sql or "").strip():
            return {
                "type": "CHAT",
                "answer": "I wasn't able to generate a SQL query for that question. Could you rephrase?",
            }
        if "Please specify which SQL dialect" in raw_sql:
            return {"type": "CHAT", "answer": raw_sql}

        # Normalize + validate
        norm_sql = _normalize_llm_sql(raw_sql)

        validation_start = time.time()
        try:
            validation = validate_and_sanitize(norm_sql, dialect=dialect)
            safe_sql = validation.sanitized_sql
        except SQLGuardError as exc:
            logger.warning("SQL guard rejected query: %s | SQL: %s", exc, norm_sql)
            return {
                "type": "CHAT",
                "answer": (
                    f"The generated SQL did not pass security validation: {exc}. "
                    "Please rephrase your question."
                ),
            }
        sql_validation_total_ms += (time.time() - validation_start) * 1000

        # Execute (unless QUERY_ONLY)
        if mode == "QUERY_ONLY":
            last_exec_error = None
            break

        execution_start = time.time()
        try:
            result = execute_query(db, safe_sql, {}, dialect=dialect)
            last_exec_error = None
            query_execution_total_ms += (time.time() - execution_start) * 1000
            break
        except QueryExecutionError as exc:
            query_execution_total_ms += (time.time() - execution_start) * 1000
            last_exec_error = exc
            logger.warning("Query execution failed (attempt %d/%d): %s", attempt + 1, max_retries + 1, exc)
            result = None

    # Finalize timing buckets
    timings.sql_validation_ms = sql_validation_total_ms
    timings.query_execution_ms = query_execution_total_ms

    if last_exec_error is not None and mode != "QUERY_ONLY":
        # Return last attempt SQL + error (user can refine question)
        timings.total_ms = (time.time() - total_start) * 1000
        return {
            "type": "DB",
            "mode": mode,
            "sql": safe_sql or _normalize_llm_sql(raw_sql),
            "params": {},
            "explanation": f"Generated SQL for: {message}",
            "error": str(last_exec_error),
            "answer_text": f"Query failed: {last_exec_error}",
        }

    explanation = f"Generated SQL for: {message}"

    # ── Step 6: Build final response ──────────────────────────────────────────
    timings.total_ms = (time.time() - total_start) * 1000
    
    logger.info(
        "Chat completed | total=%.2fms | schema=%.2fms | llm=%.2fms | exec=%.2fms",
        timings.total_ms,
        timings.schema_fetch_ms,
        timings.llm_generation_ms,
        timings.query_execution_ms,
    )
    
    return _build_db_response(
        mode=mode,
        sql=safe_sql,
        explanation=explanation,
        result=result,
        timings=timings,
    )


# Backwards compatibility wrapper
def handle_chat_sync(message: str, db: Session) -> dict[str, Any]:
    """Synchronous wrapper for backwards compatibility."""
    # Note: This should not be used in production async paths
    # It's provided for testing and legacy compatibility only
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're in an async context, this shouldn't happen
            logger.warning("handle_chat_sync called from async context - use handle_chat_optimized instead")
            raise RuntimeError("Cannot call sync wrapper from async context")
        else:
            return loop.run_until_complete(handle_chat_optimized(message, db))
    except RuntimeError:
        # No event loop, create one
        return asyncio.run(handle_chat_optimized(message, db))
