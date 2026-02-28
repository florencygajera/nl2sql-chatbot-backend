"""
Query Executor — executes validated SQL SELECT statements.

Safety guarantees
-----------------
- Only called with SQL that has already passed ``sql_guard.validate_and_sanitize``.
- Uses SQLAlchemy ``text()`` with bound parameters — no raw string interpolation.
- Runs inside a read-only transaction (SET TRANSACTION READ ONLY where possible).
- Enforces a hard row ceiling from settings.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


class QueryExecutionError(Exception):
    """Raised when query execution fails."""


# ── Executor ──────────────────────────────────────────────────────────────────

def execute_query(db: Session, sql: str, params: dict[str, Any], timeout_seconds: int = 30) -> QueryResult:
    """
    Execute a validated SELECT statement and return structured results.

    Parameters
    ----------
    db:
        Active SQLAlchemy session (injected via FastAPI dependency).
    sql:
        Pre-validated SQL string. Must be a SELECT.
    params:
        Named bind parameters matching :param_name placeholders in ``sql``.
    timeout_seconds:
        Query execution timeout in seconds (default: 30).

    Returns
    -------
    QueryResult
        Column names, rows (as lists), and total row count.

    Raises
    ------
    QueryExecutionError
        On any database-level error.
    """
    import signal
    from contextlib import contextmanager

    @contextmanager
    def timeout_handler(seconds: int):
        def handler(signum, frame):
            raise TimeoutError(f"Query execution timed out after {seconds} seconds")
        old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    try:
        # Attempt to set the transaction as read-only (PostgreSQL supports this)
        try:
            db.execute(text("SET TRANSACTION READ ONLY"))
        except Exception:
            pass  # graceful fallback for databases that don't support this

        stmt = text(sql)
        
        # Apply query timeout using signal-based approach
        with timeout_handler(timeout_seconds):
            cursor = db.execute(stmt, params or {})

        columns = list(cursor.keys())
        raw_rows = cursor.fetchall()

        # Enforce hard row ceiling (defensive; SQL guard already added LIMIT)
        if len(raw_rows) > settings.MAX_ROW_LIMIT:
            raw_rows = raw_rows[: settings.MAX_ROW_LIMIT]
            logger.warning(
                "Result set exceeded max_row_limit (%d); truncated.",
                settings.MAX_ROW_LIMIT,
            )

        # Convert rows to plain lists for JSON serialisation
        rows = [_serialise_row(row) for row in raw_rows]

        return QueryResult(columns=columns, rows=rows, row_count=len(rows))

    except TimeoutError as exc:
        db.rollback()
        logger.error("Query execution timed out: %s | SQL: %s", exc, sql)
        raise QueryExecutionError(str(exc)) from exc
    except Exception as exc:
        # Roll back so the session stays usable
        db.rollback()
        logger.error("Query execution error: %s | SQL: %s", exc, sql)
        raise QueryExecutionError(str(exc)) from exc


def _serialise_row(row) -> list[Any]:
    """Convert a SQLAlchemy Row to a JSON-safe list."""
    result = []
    for val in row:
        if hasattr(val, "isoformat"):          # date / datetime / time
            result.append(val.isoformat())
        elif isinstance(val, (bytes, bytearray)):
            result.append(val.decode("utf-8", errors="replace"))
        else:
            result.append(val)
    return result
