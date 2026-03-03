"""
Chat Service — orchestrates the full NL→SQL→Result pipeline.

Upgrades in this version
------------------------
✅ Robust table-picking even if catalog format changes
✅ Clarification gate (stops guessing when the question is ambiguous)
✅ Pre-execution SQL sanity checks (missing FROM / alias not bound)
✅ Pre-execution column existence validation (blocks hallucinated columns)
✅ Auto-repair loop on SQL execution errors (invalid column/table/join)
✅ Better user-facing error guidance

Flow
----
1. Detect response mode (QUERY_ONLY / ANSWER_ONLY / QUERY_AND_ANSWER).
2. Fetch DB schema catalog/summary.
3. Select relevant tables (robust parsing + fallback).
4. Generate SQL via local LLM (dialect-aware).
5. Validate SQL (security + basic sanity + column existence).
6. Execute query (unless QUERY_ONLY).
7. If execution fails → repair SQL (1–2 tries).
8. Return response.
"""

from __future__ import annotations

import asyncio
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

# ── SQL identifier checks (pre-execution) ─────────────────────────────────────

def _parse_schema_to_map(schema_text: str) -> dict[str, set[str]]:
    """
    Parse schema text:
      Table: "dbo.Table"
        - "Column" (type)
    -> { "dbo.Table": {"Id","Name"...}, ... }
    """
    table_map: dict[str, set[str]] = {}
    current: str | None = None

    for line in (schema_text or "").splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("Table:"):
            current = s.split("Table:", 1)[1].strip().strip('"').strip("'")
            table_map[current] = set()
            continue

        if current and s.startswith("  -"):
            raw = s[3:].strip()
            if raw.startswith('"'):
                pieces = raw.split('"')
                col = pieces[1] if len(pieces) >= 2 else ""
            else:
                col = raw.split(" ", 1)[0].strip()
            if col:
                table_map[current].add(col)

        elif current and s.startswith("-"):
            # sometimes schema has "- Column"
            raw = s[1:].strip()
            if raw.startswith('"'):
                pieces = raw.split('"')
                col = pieces[1] if len(pieces) >= 2 else ""
            else:
                col = raw.split(" ", 1)[0].strip()
            if col:
                table_map[current].add(col)

    return table_map


def _extract_tables_from_sql(sql: str) -> list[str]:
    """
    Extract table references like dbo.Table from FROM/JOIN.
    Handles:
      FROM dbo.Table
      JOIN dbo.Table
      FROM [dbo].[Table]
      JOIN [dbo].[Table]
    """
    s = sql or ""
    tables: list[str] = []

    for m in re.finditer(r"\b(?:from|join)\s+\[([^\]]+)\]\.\[([^\]]+)\]", s, flags=re.I):
        tables.append(f"{m.group(1)}.{m.group(2)}")

    for m in re.finditer(r"\b(?:from|join)\s+([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b", s, flags=re.I):
        tables.append(f"{m.group(1)}.{m.group(2)}")

    # de-dupe preserve order
    seen = set()
    out = []
    for t in tables:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _extract_column_refs(sql: str) -> set[str]:
    """
    Extract column names referenced in SQL, without incorrectly capturing table names.

    Captures:
      - alias.column
      - [alias].[column]
      - [column] (ONLY in contexts where likely a column)
    Avoids:
      - [schema].[table]
      - table names after FROM/JOIN
      - schema names like dbo
    """
    s = sql or ""
    cols: set[str] = set()

    # 1) alias.column
    for m in re.finditer(r"\b[A-Za-z_][\w]*\.([A-Za-z_][\w]*)\b", s):
        cols.add(m.group(1))

    # 2) [alias].[column]
    for m in re.finditer(r"\[[^\]]+\]\.\[([^\]]+)\]", s):
        cols.add(m.group(1))

    # 3) [column] only in SELECT/WHERE/ON/GROUP/ORDER contexts
    context_patterns = [
        r"\bselect\b[^;]*?\[([^\]]+)\]",
        r"\bwhere\b[^;]*?\[([^\]]+)\]",
        r"\bon\b[^;]*?\[([^\]]+)\]",
        r"\bgroup\s+by\b[^;]*?\[([^\]]+)\]",
        r"\border\s+by\b[^;]*?\[([^\]]+)\]",
    ]
    for pat in context_patterns:
        for m in re.finditer(pat, s, flags=re.I | re.S):
            cols.add(m.group(1))

    keywords = {
        "select", "from", "where", "join", "on", "and", "or", "group", "by", "order", "as",
        "inner", "left", "right", "count", "sum", "avg", "min", "max", "distinct",
        "top", "limit", "offset", "fetch", "dbo"
    }
    cols = {c for c in cols if c and c.lower() not in keywords}

    # Remove table/schema tokens that appear after FROM/JOIN
    table_tokens = set()
    for m in re.finditer(r"\b(?:from|join)\s+\[([^\]]+)\]\.\[([^\]]+)\]", s, flags=re.I):
        table_tokens.add(m.group(1))
        table_tokens.add(m.group(2))
    for m in re.finditer(r"\b(?:from|join)\s+([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b", s, flags=re.I):
        table_tokens.add(m.group(1))
        table_tokens.add(m.group(2))

    cols = {c for c in cols if c not in table_tokens}
    return cols


def _validate_columns_exist(sql: str, schema_text: str) -> tuple[bool, str]:
    """
    Ensure every referenced column exists in at least one selected table.
    This blocks hallucinated columns before execution.
    """
    table_map = _parse_schema_to_map(schema_text)
    if not table_map:
        return True, ""

    referenced_cols = _extract_column_refs(sql)
    if not referenced_cols:
        return True, ""

    all_cols = set().union(*table_map.values()) if table_map else set()
    missing = [c for c in referenced_cols if c not in all_cols]

    if missing:
        return False, f"SQL referenced unknown column(s): {', '.join(missing)}"
    return True, ""


def _basic_sql_sanity(sql: str) -> tuple[bool, str]:
    """
    Block obviously broken SQL before execution.
    - Must be SELECT
    - Must contain FROM (unless it's CLARIFY helper select)
    - If references alias T1., must define AS T1 somewhere
    """
    s = (sql or "").strip()
    if not s:
        return False, "Empty SQL."
    low = s.lower()

    if not low.startswith("select"):
        return False, "Only SELECT queries are allowed."

    # Allow CLARIFY helper selects without FROM
    if "clarify:" in low:
        return True, ""

    if " from " not in f" {low} ":
        return False, "Missing FROM clause."

    if re.search(r"\bT1\.", s) and not re.search(r"\bas\s+T1\b", s, flags=re.I):
        return False, "Uses alias T1 but FROM/JOIN does not define AS T1."

    return True, ""


# ── Response builders ─────────────────────────────────────────────────────────

def _build_db_response(mode: str, sql: str, explanation: str, result: QueryResult | None) -> dict[str, Any]:
    response: dict[str, Any] = {"type": "DB", "mode": mode, "explanation": explanation}

    if mode in ("QUERY_ONLY", "QUERY_AND_ANSWER"):
        response["sql"] = sql
        response["params"] = {}

    if mode in ("ANSWER_ONLY", "QUERY_AND_ANSWER") and result is not None:
        response["result"] = {"columns": result.columns, "rows": result.rows, "row_count": result.row_count}
        response["answer_text"] = _generate_answer_text(result, explanation)

    return response


def _generate_answer_text(result: QueryResult, explanation: str) -> str:
    if result.row_count == 0:
        return "The query returned no results."

    if result.row_count == 1 and len(result.columns) == 1:
        val = result.rows[0][0]
        col = result.columns[0]
        return f"{explanation} Result: {col} = {val}"

    summary_parts = [f"Found {result.row_count} row(s).", f"Columns: {', '.join(result.columns)}."]

    if result.row_count <= 5:
        for row in result.rows:
            summary_parts.append("  • " + ", ".join(f"{c}: {v}" for c, v in zip(result.columns, row)))
    else:
        summary_parts.append("First 3 rows:")
        for row in result.rows[:3]:
            summary_parts.append("  • " + ", ".join(f"{c}: {v}" for c, v in zip(result.columns, row)))
        summary_parts.append(f"  … and {result.row_count - 3} more.")

    return "\n".join(summary_parts)


# ── Schema helpers ───────────────────────────────────────────────────────────

def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _extract_table_names_from_schema(schema_text: str, max_items: int = 12) -> list[str]:
    found: list[str] = []

    for line in schema_text.splitlines():
        if not line.strip().lower().startswith("table:"):
            continue
        raw = line.split(":", 1)[1].strip().strip('"').strip("'")
        if raw:
            found.append(raw)
        if len(found) >= max_items:
            return _dedupe_preserve_order(found)

    for m in re.finditer(r"\b([A-Za-z_][\w]*\.[A-Za-z_][\w]*)\b", schema_text):
        found.append(m.group(1))
        if len(found) >= max_items:
            break

    return _dedupe_preserve_order(found)[:max_items]


def _get_tables_for_question(catalog_text: str, question: str, top_k: int) -> list[str]:
    picked: list[str] = []

    try:
        tables = parse_schema_summary(catalog_text)
        if tables:
            picked = select_relevant_tables(question=question, tables=tables, top_k=top_k)
            if picked:
                return picked
    except Exception as exc:
        logger.warning("parse_schema_summary/select_relevant_tables failed: %s", exc)

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

    if all(s == 0 for s, _ in scored[: min(len(scored), top_k)]):
        return all_tables[:top_k]

    return top


# ── Error helpers ─────────────────────────────────────────────────────────────

def _build_query_error_answer(exc: Exception, schema: str) -> str:
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
    lower = user_message.lower()

    query_keywords = [
        "sql", "query", "command only", "just the query", "show me the query",
        "give me the sql", "only the sql", "only sql", "sql only", "raw query",
    ]
    answer_keywords = [
        "only answer", "just the answer", "only the answer", "just answer",
        "answer only", "no sql", "without sql",
    ]

    if any(kw in lower for kw in query_keywords):
        return "QUERY_ONLY"
    if any(kw in lower for kw in answer_keywords):
        return "ANSWER_ONLY"
    return "QUERY_AND_ANSWER"


def _needs_clarification(question: str) -> str | None:
    q = question.lower().strip()

    if any(k in q for k in ("sql", "query", "only sql", "raw query")):
        return None

    ambiguous_words = ("report", "details", "data", "list", "show", "history", "summary", "statement")
    has_ambiguous = any(w in q for w in ambiguous_words)

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


# ── Main service function ─────────────────────────────────────────────────────

async def handle_chat(message: str, db: Session, cached_schema: str = "") -> dict[str, Any]:
    dialect = get_current_dialect()
    logger.info("Current DB dialect: %s", dialect)

    mode = _detect_response_mode(message)
    logger.info("User message: %r | Mode: %s", message, mode)

    clarify = _needs_clarification(message)
    if clarify:
        return {"type": "CHAT", "answer": clarify}

    # ── Fetch catalog/summary ────────────────────────────────────────────────
    catalog = (cached_schema or "").strip()

    if not catalog:
        try:
            catalog = (await run_in_threadpool(get_schema_catalog)).strip()
        except Exception as exc:
            logger.warning("Could not fetch schema catalog: %s", exc)
            catalog = ""

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

    # ── Select tables + targeted schema ──────────────────────────────────────
    top_k = int(getattr(settings, "NL2SQL_TOP_TABLES", 10))
    picked_tables = _get_tables_for_question(catalog, message, top_k=top_k)

    schema = ""
    if picked_tables:
        try:
            schema = await run_in_threadpool(get_schema_for_tables, picked_tables)
        except Exception as exc:
            logger.warning("Could not fetch targeted schema: %s", exc)
            schema = ""

    if not (schema or "").strip():
        schema = catalog

    logger.info(
        "Schema(catalog=%d chars) -> targeted_schema=%d chars | picked_tables=%s",
        len(catalog or ""),
        len(schema or ""),
        picked_tables[: min(12, len(picked_tables))],
    )

    # ── Generate SQL ─────────────────────────────────────────────────────────
    try:
        raw_sql = await generate_sql(message, schema_hint=schema, dialect=dialect)
    except asyncio.TimeoutError:
        logger.error("LLM timeout")
        return {
            "type": "CHAT",
            "answer": (
                "The AI model took too long to respond. This can happen if the model is loading. "
                "Please try again. If it persists, ensure Ollama is running and the model is loaded."
            ),
        }
    except Exception as exc:
        logger.error("LLM error: %s", exc)
        return {"type": "CHAT", "answer": f"I encountered an error communicating with the AI model. Details: {exc}"}

    if not raw_sql or not raw_sql.strip():
        return {"type": "CHAT", "answer": "I wasn't able to generate a SQL query for that question. Could you rephrase?"}

    if "Please specify which SQL dialect" in raw_sql:
        return {"type": "CHAT", "answer": raw_sql}

    explanation = f"Generated SQL for: {message}"

    # ── Security validation ──────────────────────────────────────────────────
    try:
        validation = validate_and_sanitize(raw_sql, dialect=dialect)
        safe_sql = validation.sanitized_sql
    except SQLGuardError as exc:
        logger.warning("SQL guard rejected query: %s | SQL: %s", exc, raw_sql)
        return {
            "type": "CHAT",
            "answer": f"The generated SQL did not pass security validation: {exc}. Please rephrase your question.",
        }

    # ── CLARIFY handling ─────────────────────────────────────────────────────
    if "CLARIFY:" in safe_sql.upper():
        return {"type": "CHAT", "answer": safe_sql}

    # ── Basic sanity checks BEFORE execution ─────────────────────────────────
    ok, reason = _basic_sql_sanity(safe_sql)
    if not ok:
        return {
            "type": "CHAT",
            "answer": f"I blocked invalid SQL generated by the model: {reason}",
            "sql": safe_sql,
        }

    # ── Execute (unless QUERY_ONLY) with repair loop ─────────────────────────
    result: QueryResult | None = None

    if mode != "QUERY_ONLY":
        max_retries = int(getattr(settings, "NL2SQL_MAX_RETRIES", 2))
        attempt = 0

        while attempt <= max_retries:
            # Validate columns exist in current schema subset
            ok_cols, reason_cols = _validate_columns_exist(safe_sql, schema)
            if not ok_cols:
                logger.error("Blocked invalid SQL before execution: %s | SQL: %s", reason_cols, safe_sql)

                referenced_tables = _extract_tables_from_sql(safe_sql)
                if referenced_tables:
                    try:
                        schema = await run_in_threadpool(get_schema_for_tables, referenced_tables)
                    except Exception as exc:
                        logger.warning("Could not refetch schema for referenced tables: %s", exc)

                if attempt >= max_retries:
                    return {
                        "type": "DB",
                        "mode": mode,
                        "sql": safe_sql,
                        "params": {},
                        "explanation": explanation,
                        "error": reason_cols,
                        "answer_text": (
                            f"I blocked the generated SQL because it used column(s) that don't exist: {reason_cols}. "
                            "Ask: 'show me columns of <table>' to confirm exact names."
                        ),
                    }

                repair_prompt = (
                    "Fix this SQL Server query. Return ONLY corrected SQL.\n"
                    "HARD RULES:\n"
                    "- Use ONLY tables and columns present in the schema.\n"
                    "- Do NOT invent join keys.\n"
                    "- If you cannot determine the correct join key, return:\n"
                    "  SELECT N'CLARIFY: Which column should be used to join these tables?' AS [NeedMoreInfo];\n\n"
                    f"USER QUESTION:\n{message}\n\n"
                    f"FAILED SQL:\n{safe_sql}\n\n"
                    f"VALIDATION ERROR:\n{reason_cols}\n\n"
                    f"SCHEMA:\n{schema}\n"
                )

                repaired = await generate_sql(repair_prompt, schema_hint=schema, dialect=dialect)
                validation2 = validate_and_sanitize(repaired, dialect=dialect)
                safe_sql = validation2.sanitized_sql
                explanation = f"Repaired SQL for: {message}"

                # If repair results in CLARIFY, stop.
                if "CLARIFY:" in safe_sql.upper():
                    return {"type": "CHAT", "answer": safe_sql}

                # sanity again
                ok2, reason2 = _basic_sql_sanity(safe_sql)
                if not ok2:
                    return {"type": "CHAT", "answer": f"I blocked invalid repaired SQL: {reason2}", "sql": safe_sql}

                attempt += 1
                continue

            try:
                result = await run_in_threadpool(execute_query, db, safe_sql, {}, dialect)
                break
            except QueryExecutionError as exc:
                logger.error(
                    "Query execution failed (attempt %d/%d): %s | SQL: %s",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    safe_sql,
                )

                referenced_tables = _extract_tables_from_sql(safe_sql)
                if referenced_tables:
                    try:
                        schema = await run_in_threadpool(get_schema_for_tables, referenced_tables)
                    except Exception as sx:
                        logger.warning("Could not refetch schema for referenced tables: %s", sx)

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

                repair_prompt = (
                    "Fix this SQL Server query. Return ONLY corrected SQL.\n"
                    "HARD RULES:\n"
                    "- Use ONLY tables and columns present in the schema.\n"
                    "- Do NOT invent join keys.\n"
                    "- If you cannot determine the correct join key, return:\n"
                    "  SELECT N'CLARIFY: Which column should be used to join these tables?' AS [NeedMoreInfo];\n\n"
                    f"USER QUESTION:\n{message}\n\n"
                    f"FAILED SQL:\n{safe_sql}\n\n"
                    f"DB ERROR:\n{exc}\n\n"
                    f"SCHEMA:\n{schema}\n"
                )

                repaired = await generate_sql(repair_prompt, schema_hint=schema, dialect=dialect)
                validation2 = validate_and_sanitize(repaired, dialect=dialect)
                safe_sql = validation2.sanitized_sql
                explanation = f"Repaired SQL for: {message}"

                if "CLARIFY:" in safe_sql.upper():
                    return {"type": "CHAT", "answer": safe_sql}

                ok2, reason2 = _basic_sql_sanity(safe_sql)
                if not ok2:
                    return {"type": "CHAT", "answer": f"I blocked invalid repaired SQL: {reason2}", "sql": safe_sql}

                attempt += 1

    # ── Build final response ─────────────────────────────────────────────────
    return _build_db_response(mode=mode, sql=safe_sql, explanation=explanation, result=result)