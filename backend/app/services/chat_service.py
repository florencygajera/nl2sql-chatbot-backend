"""
Chat Service — orchestrates the full NL→SQL→Result pipeline.

Upgrades in this version
------------------------
✅ Robust table-picking even if catalog format changes
✅ Clarification gate (stops guessing when the question is ambiguous)
✅ Auto-repair loop on SQL execution errors (invalid column/table/join)
✅ Better user-facing error guidance

Flow
----
1. Detect response mode (QUERY_ONLY / ANSWER_ONLY / QUERY_AND_ANSWER).
2. Fetch DB schema catalog/summary.
3. Select relevant tables (robust parsing + fallback).
4. Generate SQL via local LLM (dialect-aware).
5. Validate SQL (security + basic sanity).
6. Execute query (unless QUERY_ONLY).
7. If execution fails → repair SQL (1–2 tries).
8. Return response.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable

from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.db.session import (
    get_schema_summary,
    get_schema_catalog,
    get_schema_for_tables,
    get_current_dialect,
)
from app.db.schema_retriever import parse_schema_summary, select_relevant_tables
from app.core.config import get_settings

from app.llm.nl2sql import generate_sql
from app.security.sql_guard import SQLGuardError, validate_and_sanitize
from app.services.query_executor import QueryExecutionError, QueryResult, execute_query

logger = logging.getLogger(__name__)
settings = get_settings()

top_k = settings.NL2SQL_TOP_TABLES

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


# ── Schema helpers ────────────────────────────────────────────────────────────

def _extract_table_names_from_schema(schema_text: str, max_items: int = 12) -> list[str]:
    """
    Extract table names from schema text using multiple patterns.
    Works for formats like:
      - Table: dbo.TableName
      - Table: "dbo.TableName"
      - dbo.TableName(...)
    """
    found: list[str] = []

    # Pattern A: lines like `Table: ...`
    for line in schema_text.splitlines():
        if not line.strip().lower().startswith("table:"):
            continue
        raw = line.split(":", 1)[1].strip().strip('"').strip("'")
        if raw:
            found.append(raw)
        if len(found) >= max_items:
            return _dedupe_preserve_order(found)

    # Pattern B: `dbo.X` occurrences
    for m in re.finditer(r"\b([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\b", schema_text):
        found.append(m.group(1))
        if len(found) >= max_items:
            break

    return _dedupe_preserve_order(found)[:max_items]


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _get_tables_for_question(catalog_text: str, question: str, top_k: int) -> list[str]:
    """
    Robust table selection:
    - Try your existing parser (best when format is correct)
    - If parsing fails / empty, fallback to regex table extraction + keyword scoring
    """
    picked: list[str] = []

    try:
        tables = parse_schema_summary(catalog_text)
        if tables:
            picked = select_relevant_tables(question=question, tables=tables, top_k=top_k)
            if picked:
                return picked
    except Exception as exc:
        logger.warning("parse_schema_summary/select_relevant_tables failed: %s", exc)

    # Fallback: extract table names from text, score by keyword overlap
    all_tables = _extract_table_names_from_schema(catalog_text, max_items=200)
    if not all_tables:
        return []

    q_tokens = set(re.findall(r"[A-Za-z_]\w*", question.lower()))
    scored: list[tuple[int, str]] = []
    for t in all_tables:
        t_tokens = set(re.findall(r"[A-Za-z_]\w*", t.lower()))
        score = len(q_tokens & t_tokens)
        scored.append((score, t))

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    top = [t for _, t in scored[:top_k]]

    # If everything scored 0, still return something (top_k) to avoid empty schema
    if all(s == 0 for s, _ in scored[: min(len(scored), top_k)]):
        return all_tables[:top_k]

    return top


# ── Error helpers ─────────────────────────────────────────────────────────────

def _build_query_error_answer(exc: Exception, schema: str) -> str:
    """Return concise, user-friendly guidance for common SQL execution errors."""
    err = str(exc)

    invalid_object = re.search(r"Invalid object name '([^']+)'", err, re.IGNORECASE)
    if invalid_object:
        bad_table = invalid_object.group(1)
        available = _extract_table_names_from_schema(schema, max_items=12)
        if available:
            return (
                f"Table '{bad_table}' was not found in the connected database. "
                f"Try using one of these tables from your schema: {', '.join(available)}. "
                "If needed, include the schema name like dbo.TableName."
            )
        return (
            f"Table '{bad_table}' was not found in the connected database. "
            "Please verify table names in your connected schema and try again."
        )

    invalid_column = re.search(r"Invalid column name '([^']+)'", err, re.IGNORECASE)
    if invalid_column:
        bad_col = invalid_column.group(1)
        return (
            f"The query referenced a column '{bad_col}' that does not exist in the selected tables. "
            "This usually means the join/filter column name was guessed. "
            "Try specifying the exact field name or ask for the correct columns of a table."
        )

    return f"Query failed: {err}"


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


def _needs_clarification(question: str) -> str | None:
    """
    Lightweight clarification gate.
    Returns a clarifying question string if the user request is too ambiguous.
    """
    q = question.lower().strip()

    # If user is explicitly asking for SQL/query, don't block them
    if any(k in q for k in ("sql", "query", "only sql", "raw query")):
        return None

    # Very common ambiguous intents
    ambiguous_words = ("report", "details", "data", "list", "show", "history", "summary", "statement")
    has_ambiguous = any(w in q for w in ambiguous_words)

    # If they didn't specify any time period and asked for report/history/list → ask timeframe
    has_time_hint = any(
        t in q for t in (
            "today", "yesterday", "tomorrow", "this week", "last week",
            "this month", "last month", "this year", "last year",
            "between", "from", "to", "since", "before", "after"
        )
    )
    has_date_literal = bool(re.search(r"\b(20\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b", q))

    if has_ambiguous and not (has_time_hint or has_date_literal):
        return "Which date range should I use (e.g., today, last 7 days, last month, or a custom from–to range)?"

    return None


async def _repair_sql(
    *,
    question: str,
    previous_sql: str,
    error_text: str,
    schema: str,
    dialect: str,
) -> str:
    """
    Ask the LLM to repair SQL using the DB error + schema.
    Works with your existing `generate_sql()` without changing other files.
    """
    repair_prompt = (
        "You generated SQL that failed to execute.\n\n"
        f"USER QUESTION:\n{question}\n\n"
        f"FAILED SQL:\n{previous_sql}\n\n"
        f"DB ERROR:\n{error_text}\n\n"
        "TASK:\n"
        "- Return ONLY corrected SQL.\n"
        "- Use ONLY tables/columns from the provided schema.\n"
        "- Prefer simple joins on obvious key columns (Id, ...Id).\n"
        "- If a referenced column doesn't exist, choose the closest existing column name.\n\n"
        f"SCHEMA:\n{schema}\n"
    )
    return await generate_sql(repair_prompt, schema_hint=schema, dialect=dialect)


# ── Main service function ─────────────────────────────────────────────────────

async def handle_chat(message: str, db: Session, cached_schema: str = "") -> dict[str, Any]:
    """
    Process a user message end-to-end and return a JSON-serialisable response.
    """
    # ── Step 0: Detect current DB dialect ─────────────────────────────────────
    dialect = get_current_dialect()
    logger.info("Current DB dialect: %s", dialect)

    # ── Step 1: Detect desired response mode ──────────────────────────────────
    mode = _detect_response_mode(message)
    logger.info("User message: %r | Mode: %s", message, mode)

    # ── Step 1.5: Clarification gate (prevents guessing) ──────────────────────
    clarify = _needs_clarification(message)
    if clarify:
        return {
            "type": "CHAT",
            "answer": clarify,
        }

    # ── Step 2: Fetch DB catalog/summary ──────────────────────────────────────
    catalog = (cached_schema or "").strip()

    if not catalog:
        try:
            catalog = (await run_in_threadpool(get_schema_catalog)).strip()
        except Exception as exc:
            logger.warning("Could not fetch schema catalog: %s", exc)
            catalog = ""

    # Fall back to old summary (may be truncated but better than empty)
    if not catalog:
        try:
            catalog = (await run_in_threadpool(get_schema_summary)).strip()
        except Exception as exc:
            logger.warning("Could not fetch schema summary: %s", exc)
            catalog = ""

    if not catalog:
        return {
            "type": "CHAT",
            "answer": (
                "I could not retrieve the database schema. "
                "Please make sure you are connected to a database via /db/connect "
                "and pass the db_session_id in your chat request."
            ),
        }

    # ── Step 2.5: Select relevant tables & fetch targeted schema ──────────────
    settings = get_settings()
    top_k = settings.NL2SQL_TOP_TABLES
    picked_tables = _get_tables_for_question(catalog, message, top_k=top_k)

    schema = ""
    if picked_tables:
        try:
            schema = await run_in_threadpool(get_schema_for_tables, picked_tables)
        except Exception as exc:
            logger.warning("Could not fetch targeted schema: %s", exc)
            schema = ""

    # If targeted schema failed, fall back to catalog itself
    if not (schema or "").strip():
        schema = catalog

    logger.info(
        "Schema(catalog=%d chars) -> targeted_schema=%d chars | picked_tables=%s",
        len(catalog or ""),
        len(schema or ""),
        picked_tables[: min(12, len(picked_tables))],
    )

    # ── Step 3: Call LLM to generate SQL ──────────────────────────────────────
    try:
        raw_sql = await generate_sql(message, schema_hint=schema, dialect=dialect)
    except Exception as exc:
        logger.error("LLM error: %s", exc)
        return {
            "type": "CHAT",
            "answer": (
                "I encountered an error communicating with the AI model. "
                f"Details: {exc}"
            ),
        }

    if not raw_sql or not raw_sql.strip():
        return {
            "type": "CHAT",
            "answer": "I wasn't able to generate a SQL query for that question. Could you rephrase?",
        }

    if "Please specify which SQL dialect" in raw_sql:
        return {"type": "CHAT", "answer": raw_sql}

    explanation = f"Generated SQL for: {message}"

    # ── Step 4: Validate SQL (dialect-aware) ──────────────────────────────────
    try:
        validation = validate_and_sanitize(raw_sql, dialect=dialect)
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

    # ── Step 5: Execute (unless QUERY_ONLY) with repair loop ──────────────────
    result: QueryResult | None = None

    if mode != "QUERY_ONLY":
        max_retries = settings.NL2SQL_MAX_RETRIES
        attempt = 0
        last_exc: Exception | None = None

        while attempt <= max_retries:
            try:
                result = await run_in_threadpool(execute_query, db, safe_sql, {}, dialect)
                last_exc = None
                break
            except QueryExecutionError as exc:
                last_exc = exc
                logger.error("Query execution failed (attempt %d/%d): %s", attempt + 1, max_retries + 1, exc)

                # If we have no retries left, return error response
                if attempt >= max_retries:
                    return {
                        "type": "DB",
                        "mode": mode,
                        "sql": safe_sql,
                        "params": {},
                        "explanation": explanation,
                        "error": str(exc),
                        "answer_text": _build_query_error_answer(exc, schema),
                    }

                # Try to repair SQL using the DB error
                try:
                    repaired = await _repair_sql(
                        question=message,
                        previous_sql=safe_sql,
                        error_text=str(exc),
                        schema=schema,
                        dialect=dialect,
                    )
                    if not repaired or not repaired.strip():
                        # If repair fails to produce SQL, stop early with friendly error
                        return {
                            "type": "DB",
                            "mode": mode,
                            "sql": safe_sql,
                            "params": {},
                            "explanation": explanation,
                            "error": str(exc),
                            "answer_text": _build_query_error_answer(exc, schema),
                        }

                    # Validate repaired SQL
                    validation2 = validate_and_sanitize(repaired, dialect=dialect)
                    safe_sql = validation2.sanitized_sql
                    explanation = f"Repaired SQL for: {message}"
                except Exception as repair_exc:
                    logger.warning("SQL repair failed: %s", repair_exc)
                    return {
                        "type": "DB",
                        "mode": mode,
                        "sql": safe_sql,
                        "params": {},
                        "explanation": explanation,
                        "error": str(last_exc) if last_exc else "Unknown error",
                        "answer_text": _build_query_error_answer(last_exc or repair_exc, schema),
                    }

            attempt += 1

    # ── Step 6: Build final response ──────────────────────────────────────────
    return _build_db_response(
        mode=mode,
        sql=safe_sql,
        explanation=explanation,
        result=result,
    )