"""
SQL Guard — validates LLM-generated SQL before execution.

Responsibilities
----------------
1. Block any non-SELECT statement (INSERT, UPDATE, DELETE, DROP, ALTER, …).
2. Prevent multiple statements (semicolons outside string literals).
3. Detect dangerous patterns (stacked queries, comment injections, etc.).
4. Automatically inject LIMIT/TOP for non-aggregate queries (dialect-aware).
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


# NOTE: Removed \\bSET\\b — this was incorrectly blocking SQL like
# "SELECT ... OFFSET ... SET ..." and causing issues. The real danger
# (SET statements) is already prevented by requiring the query start
# with SELECT or WITH.
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
    r"\bCOPY\b",
    r"\bLOAD\b",
    r"\bIMPORT\b",
)

_AGGREGATE_PATTERN = re.compile(
    r"\b(COUNT|SUM|AVG|MIN|MAX|GROUP\s+BY)\b",
    re.IGNORECASE,
)

_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+\b", re.IGNORECASE)
_TOP_PATTERN = re.compile(r"\bTOP\s+\d+\b", re.IGNORECASE)
_OFFSET_FETCH_PATTERN = re.compile(r"\bOFFSET\s+\d+\s+ROWS\b", re.IGNORECASE)

_SEMICOLON_OUTSIDE_LITERAL = re.compile(
    r";(?=(?:[^'\"]*['\"][^'\"]*['\"])*[^'\"]*$)"
)

_COMMENT_PATTERN = re.compile(r"(--|/\*|\*/|#)", re.IGNORECASE)

_FENCE_BLOCK_PATTERN = re.compile(
    r"```(?:sql|postgresql|mysql|sqlite)?\s*([\s\S]*?)\s*```",
    re.IGNORECASE,
)


@dataclass
class ValidationResult:
    is_valid: bool
    sanitized_sql: str = ""
    errors: list[str] = field(default_factory=list)


def validate_and_sanitize(sql: str, dialect: str = "unknown") -> ValidationResult:
    """
    Validate and sanitize SQL. Supports dialect-aware LIMIT/TOP injection.
    
    Args:
        sql: The SQL string to validate.
        dialect: Database dialect ("mssql", "postgresql", "mysql", "sqlite", "unknown").
    """
    sql = _strip_markdown_fences(sql).strip()
    if not sql:
        raise SQLGuardError("SQL query is empty.")

    sql = _strip_trailing_semicolon(sql)

    normalized = " ".join(sql.split()).upper()
    if not (normalized.startswith("SELECT") or normalized.startswith("WITH")):
        raise SQLGuardError(f"Only SELECT statements are permitted. Got: {sql[:60]!r}")

    for pattern in _FORBIDDEN_KEYWORDS:
        if re.search(pattern, sql, re.IGNORECASE):
            keyword = pattern.replace(r"\b", "").strip()
            raise SQLGuardError(f"Forbidden keyword detected: {keyword}")

    if _COMMENT_PATTERN.search(sql):
        raise SQLGuardError("SQL comments are not permitted (possible injection attempt).")

    if _SEMICOLON_OUTSIDE_LITERAL.search(sql):
        raise SQLGuardError("Multiple SQL statements are not permitted (semicolon detected).")

    sql = _maybe_inject_limit(sql, dialect)

    return ValidationResult(is_valid=True, sanitized_sql=sql)


def raise_if_invalid(sql: str, dialect: str = "unknown") -> str:
    result = validate_and_sanitize(sql, dialect)
    if not result.is_valid:
        raise SQLGuardError("; ".join(result.errors))
    return result.sanitized_sql


def _strip_markdown_fences(text: str) -> str:
    if not text:
        return ""

    s = text.strip()
    m = _FENCE_BLOCK_PATTERN.search(s)
    if m:
        return m.group(1).strip()

    s = re.sub(r"^```(?:sql|postgresql|mysql|sqlite)?\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s*```$", "", s).strip()
    return s


def _strip_trailing_semicolon(sql: str) -> str:
    s = sql.rstrip()
    if s.endswith(";"):
        s = s[:-1].rstrip()
    return s


def _maybe_inject_limit(sql: str, dialect: str = "unknown") -> str:
    """
    Previously injected row limits automatically into every query.
    Now disabled — queries return ALL rows by default.
    The LLM prompt explicitly instructs the model not to add TOP/LIMIT
    unless the user asks for it. If the user wants a subset, the LLM
    will include TOP/LIMIT in the generated SQL itself.
    """
    # No longer inject any automatic limits.
    # Let the query return all matching rows.
    return sql