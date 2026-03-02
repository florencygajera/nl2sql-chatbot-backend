from __future__ import annotations

import re
from app.llm.client import LLMClient

_client = LLMClient()


def _normalize_llm_sql(raw_sql: str) -> str:
    """Normalize LLM SQL so it is executable:
    - remove ```sql fences
    - remove surrounding quotes (if whole query is wrapped)
    - fix doubled identifier quotes: ""Table"" -> "Table"
    """
    if not raw_sql:
        return ""

    s = raw_sql.strip()

    # Remove code fences
    s = re.sub(r"^\s*```(?:sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```\s*$", "", s, flags=re.IGNORECASE)
    s = s.strip()

    # If the entire query is wrapped in a single pair of quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Fix doubled quotes
    s = s.replace('""', '"')

    return s.strip()


def generate_sql(user_message: str, schema_hint: str = "", dialect: str = "unknown") -> str:
    """
    Generate SQL from a natural language question using the LLM.
    
    Args:
        user_message: The user's natural language question.
        schema_hint: Database schema information (from live introspection).
        dialect: Database dialect ("mssql", "postgresql", "mysql", "sqlite", "unknown").
    
    Returns:
        Generated SQL string.
    """
    extra = schema_hint.strip()
    if not extra:
        extra = "No schema information available."

    # --- Determine dialect for prompt ---
    dialect_lower = dialect.lower() if dialect else "unknown"
    
    if dialect_lower == "mssql":
        prompt = _build_mssql_prompt(user_message, extra)
    elif dialect_lower in ("postgresql", "postgres"):
        prompt = _build_postgres_prompt(user_message, extra)
    elif dialect_lower == "mysql":
        prompt = _build_mysql_prompt(user_message, extra)
    elif dialect_lower == "sqlite":
        prompt = _build_sqlite_prompt(user_message, extra)
    else:
        # Try to detect from user message
        user_lower = (user_message or "").lower()
        if any(k in user_lower for k in ["mssql", "sql server", "sqlserver"]):
            prompt = _build_mssql_prompt(user_message, extra)
        elif any(k in user_lower for k in ["postgresql", "postgres", "postgre", "pg"]):
            prompt = _build_postgres_prompt(user_message, extra)
        elif "mysql" in user_lower:
            prompt = _build_mysql_prompt(user_message, extra)
        elif "sqlite" in user_lower:
            prompt = _build_sqlite_prompt(user_message, extra)
        else:
            # Default to generic SQL
            prompt = _build_generic_prompt(user_message, extra)

    raw = _client.generate(prompt).strip()
    raw = _normalize_llm_sql(raw)

    return raw


def _build_mssql_prompt(user_message: str, schema: str) -> str:
    return f"""You are a SQL generator for Microsoft SQL Server (T-SQL).

Return ONLY ONE T-SQL SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.

CRITICAL SQL SERVER RULES:
- Use square brackets [] for table and column names if they contain special characters or are reserved words.
  Example: SELECT [Id], [FirstName] FROM [User_Master]
- Do NOT use LIMIT. Instead use SELECT TOP N for limiting results.
  Example: SELECT TOP 10 [Id] FROM [User_Master]
- Do not use double quotes for identifiers; use square brackets.
- Do not invent tables or columns. Use only the schema below.

Database Schema:
{schema}

Question: {user_message}""".strip()


def _build_postgres_prompt(user_message: str, schema: str) -> str:
    return f"""You are a SQL generator for PostgreSQL.

Return ONLY ONE PostgreSQL SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.

CRITICAL POSTGRES RULES:
- ALWAYS use double quotes for ALL table and column names exactly as in the schema.
  Example: SELECT "Id" FROM "User_Master";
- NEVER output doubled quotes like ""User_Master"".
- Do not invent tables/columns. Use only the schema below.

Database Schema:
{schema}

Question: {user_message}""".strip()


def _build_mysql_prompt(user_message: str, schema: str) -> str:
    return f"""You are a SQL generator for MySQL.

Return ONLY ONE MySQL SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Do not invent tables/columns. Use only the schema below.

Database Schema:
{schema}

Question: {user_message}""".strip()


def _build_sqlite_prompt(user_message: str, schema: str) -> str:
    return f"""You are a SQL generator for SQLite.

Return ONLY ONE SQLite SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Do not invent tables/columns. Use only the schema below.

Database Schema:
{schema}

Question: {user_message}""".strip()


def _build_generic_prompt(user_message: str, schema: str) -> str:
    return f"""You are a SQL generator.

Return ONLY ONE generic SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Avoid dialect-specific quoting unless necessary.
Do not invent tables/columns. Use only the schema below.

Database Schema:
{schema}

Question: {user_message}""".strip()