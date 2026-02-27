"""
SQL Guard — validates LLM-generated SQL before execution.

Responsibilities
----------------
1. Block any non-SELECT statement (INSERT, UPDATE, DELETE, DROP, ALTER, …).
2. Prevent multiple statements (semicolons outside string literals).
3. Detect dangerous patterns (stacked queries, comment injections, etc.).
4. Automatically inject LIMIT for non-aggregate queries.
5. Encourage :param-style placeholders (optional; keep flexible).

All public functions raise ``SQLGuardError`` on violation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.core.config import get_settings

settings = get_settings()


class SQLGuardError(Exception):
    """Raised when SQL fails a security check."""


# ── Forbidden keywords (word-boundary) ─────────────────────────────────────────
_FORBIDDEN_KEYWORDS: tuple[str, ...] = (
    r"\bINSERT\b",
    r"\bUPDATE\b",
    r"\bDELETE\b",
    r"\bDROP\b",
    r"\bALTER\b",
    r"\bTRUNCATE\b",
    r"\bCREATE\b",
    r"\bREPLACE\b",
    r"\bMERGE\b",
    r"\bCALL\b",
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
    r"\bCOMMIT\b",
    r"\bROLLBACK\b",
    r"\bSAVEPOINT\b",
    r"\bSET\b",
    r"\bCOPY\b",
    r"\bLOAD\b",
    r"\bIMPORT\b",
)

# Aggregate functions / clauses that tell us LIMIT is NOT needed
_AGGREGATE_PATTERN = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|GROUP\s+BY)\b",
    re.IGNORECASE,
)

# Detect an existing LIMIT clause
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)

# Detect semicolons outside string literals (very useful)
_SEMICOLON_OUTSIDE_LITERAL = re.compile(
    r";(?=(?:[^'\"]*['\"][^'\"]*['\"])*[^'\"]*$)"
)

# SQL comment patterns
_COMMENT_PATTERN = re.compile(r"(--|/\*|\*/|#)", re.IGNORECASE)


@dataclass
class ValidationResult:
    is_valid: bool
    sanitized_sql: str = ""
    errors: list[str] = field(default_factory=list)


def validate_and_sanitize(sql: str) -> ValidationResult:
    """
    Full validation pipeline.
    Returns ValidationResult with sanitized_sql if valid.
    Raises SQLGuardError on any failure.
    """
    sql = sql.strip()
    if not sql:
        raise SQLGuardError("SQL query is empty.")

    # ── 0) Strip a SINGLE trailing semicolon (common LLM output)
    # Allow: "SELECT ...;"
    # Still block: "SELECT ...; DROP ..."
    sql = _strip_trailing_semicolon(sql)

    # ── 1) Must start with SELECT
    normalized = " ".join(sql.split()).upper()
    if not normalized.startswith("SELECT"):
        raise SQLGuardError(f"Only SELECT statements are permitted. Got: {sql[:60]!r}")

    # ── 2) Forbidden keywords
    for pattern in _FORBIDDEN_KEYWORDS:
        if re.search(pattern, sql, re.IGNORECASE):
            keyword = pattern.replace(r"\b", "").strip()
            raise SQLGuardError(f"Forbidden keyword detected: {keyword}")

    # ── 3) No comment injections
    if _COMMENT_PATTERN.search(sql):
        raise SQLGuardError("SQL comments are not permitted (possible injection attempt).")

    # ── 4) No multiple statements
    # After stripping a trailing semicolon, any remaining semicolon outside literals means multiple statements.
    if _SEMICOLON_OUTSIDE_LITERAL.search(sql):
        raise SQLGuardError("Multiple SQL statements are not permitted (semicolon detected).")

    # ── 5) Auto-inject LIMIT for non-aggregate queries
    sql = _maybe_inject_limit(sql)

    return ValidationResult(is_valid=True, sanitized_sql=sql)


def raise_if_invalid(sql: str) -> str:
    """Convenience wrapper — returns sanitized SQL or raises SQLGuardError."""
    result = validate_and_sanitize(sql)
    if not result.is_valid:
        raise SQLGuardError("; ".join(result.errors))
    return result.sanitized_sql


# ── Internal helpers ──────────────────────────────────────────────────────────

def _strip_trailing_semicolon(sql: str) -> str:
    """
    If the SQL ends with exactly one semicolon (ignoring whitespace), remove it.
    This allows harmless single-statement queries like: SELECT ...;
    """
    s = sql.rstrip()
    if s.endswith(";"):
        s = s[:-1].rstrip()
    return s


def _maybe_inject_limit(sql: str) -> str:
    """
    Append LIMIT <DEFAULT_ROW_LIMIT> to queries that:
      - Are not aggregate queries (no COUNT/SUM/AVG/MIN/MAX/GROUP BY)
      - Do not already contain a LIMIT clause
    """
    is_aggregate = bool(_AGGREGATE_PATTERN.search(sql))
    has_limit = bool(_LIMIT_PATTERN.search(sql))

    if not is_aggregate and not has_limit:
        # Use uppercase setting name from config.py
        sql = f"{sql.rstrip()} LIMIT {settings.DEFAULT_ROW_LIMIT}"

    return sql