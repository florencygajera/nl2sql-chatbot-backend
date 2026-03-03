from __future__ import annotations

import re
from app.llm.async_ollama_client import generate_async
from app.core.config import get_settings

settings = get_settings()


def _normalize_llm_sql(raw_sql: str) -> str:
    """
    Normalize LLM SQL so it is executable:
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

    # Remove trailing semicolons/spaces safely
    s = s.strip().rstrip(";").strip()

    return s


def _extract_table_lines(schema: str) -> list[str]:
    """
    Return schema lines that look like table declarations.
    Handles:
      - Table: dbo.Users
      - Table: "dbo.Users"
      - dbo.Users(...)
      - [dbo].[Users](...)
    """
    lines = []
    for ln in (schema or "").splitlines():
        t = ln.strip()
        if not t:
            continue
        if t.lower().startswith("table:"):
            lines.append(t)
        elif re.match(r'^(?:"?[\w]+(?:\.[\w]+)?"?)\s*\(', t):
            lines.append(t)
        elif re.match(r'^\[[\w]+\]\.\[[\w]+\]\s*\(', t):
            lines.append(t)
        elif re.match(r'^\[[\w]+\]\.\[[\w]+\]$', t):
            lines.append(t)
    return lines


def _extract_first_table_for_mssql_example(schema: str) -> str:
    """
    Get an example table name in MSSQL bracket format.
    Falls back safely.
    """
    example_table = "[dbo].[TableName]"
    for line in _extract_table_lines(schema):
        if line.lower().startswith("table:"):
            raw = line.split(":", 1)[1].strip().strip('"').strip("'")
        else:
            raw = line.split("(", 1)[0].strip().strip('"').strip("'")

        # Normalize raw like dbo.Users or Users
        raw = raw.replace("[", "").replace("]", "")
        if "." in raw:
            a, b = raw.split(".", 1)
            example_table = f"[{a}].[{b}]"
        else:
            example_table = f"[{raw}]"
        break
    return example_table


def _build_mssql_prompt(user_message: str, schema: str) -> str:
    example_table = _extract_first_table_for_mssql_example(schema)

    return f"""You are an expert SQL Server (T-SQL) query generator.

Return ONLY ONE executable T-SQL SELECT statement.
- No markdown, no explanations, no commentary.
- Do NOT wrap output in ```sql fences.

HARD RULES (must follow):
1) Use ONLY tables and columns that appear in the schema provided below.
2) Always use [brackets] for ALL identifiers: [schema].[Table], [ColumnName].
3) Do NOT invent joins. Only join when there is an obvious key match:
   - [Id] to [<Other>Id] OR [<Other>Id] to [Id]
   - If no obvious join key exists, do NOT join; instead ask a question by returning:
     SELECT N'CLARIFY: <your question here>' AS [NeedMoreInfo];
4) Do NOT add TOP/OFFSET/FETCH unless the user explicitly asks for a row limit.
5) Prefer simple, correct queries over complex guesses.
6) If filtering by text values, use N'' for Unicode text.

QUALITY RULES:
- Select only columns relevant to the question (avoid SELECT * unless user says "all columns").
- If the user asks for “report/summary/total/count”, use GROUP BY / aggregates as appropriate.
- If time range is missing for “report/history”, ask for timeframe using CLARIFY output.

Schema:
{schema}

User question:
{user_message}

SQL:""".strip()


def _build_postgres_prompt(user_message: str, schema: str) -> str:
    return f"""You are an expert PostgreSQL SQL query generator.

Return ONLY ONE executable PostgreSQL SELECT statement.
- No markdown, no explanations.
- Do NOT wrap output in ```sql fences.

HARD RULES:
1) Use ONLY tables and columns that appear in the schema provided below.
2) Always use double quotes for identifiers exactly as they appear in schema.
3) Do NOT invent joins. Only join with an obvious key match ("id" to "<x>_id" etc).
   If unclear, return:
   SELECT 'CLARIFY: <your question here>' AS "NeedMoreInfo";
4) Do NOT add LIMIT unless the user explicitly asks.

Schema:
{schema}

User question:
{user_message}

SQL:""".strip()


def _build_mysql_prompt(user_message: str, schema: str) -> str:
    return f"""You are an expert MySQL SQL query generator.

Return ONLY ONE executable MySQL SELECT statement.
- No markdown, no explanations.
- Do NOT wrap output in ```sql fences.

HARD RULES:
1) Use ONLY tables and columns that appear in the schema provided below.
2) Do NOT invent joins. Only join with an obvious key match (id to <x>_id etc).
   If unclear, return:
   SELECT 'CLARIFY: <your question here>' AS NeedMoreInfo;
3) Do NOT add LIMIT unless the user explicitly asks.

Schema:
{schema}

User question:
{user_message}

SQL:""".strip()


def _build_sqlite_prompt(user_message: str, schema: str) -> str:
    return f"""You are an expert SQLite SQL query generator.

Return ONLY ONE executable SQLite SELECT statement.
- No markdown, no explanations.
- Do NOT wrap output in ```sql fences.

HARD RULES:
1) Use ONLY tables and columns that appear in the schema provided below.
2) Do NOT invent joins. Only join with an obvious key match (id to <x>_id etc).
   If unclear, return:
   SELECT 'CLARIFY: <your question here>' AS NeedMoreInfo;
3) Do NOT add LIMIT unless the user explicitly asks.

Schema:
{schema}

User question:
{user_message}

SQL:""".strip()


def _build_generic_prompt(user_message: str, schema: str) -> str:
    return f"""You are an expert SQL query generator.

Return ONLY ONE executable SELECT statement.
- No markdown, no explanations.
- Do NOT wrap output in ```sql fences.

HARD RULES:
1) Use ONLY tables and columns that appear in the schema provided below.
2) Do NOT invent joins. Only join with an obvious key match.
   If unclear, return:
   SELECT 'CLARIFY: <your question here>' AS NeedMoreInfo;
3) Do NOT add LIMIT/TOP unless the user explicitly asks.

Schema:
{schema}

User question:
{user_message}

SQL:""".strip()


async def generate_sql(user_message: str, schema_hint: str = "", dialect: str = "unknown") -> str:
    """
    Generate SQL from a natural language question using the LLM.

    Key upgrade:
    - Adds a CLARIFY escape hatch so the model does not guess joins/timeframes.
    - Keeps temperature at 0.0 and uses configurable max tokens.
    """
    schema = (schema_hint or "").strip() or "No schema information available."
    dialect_lower = (dialect or "unknown").lower()

    if dialect_lower == "mssql":
        prompt = _build_mssql_prompt(user_message, schema)
    elif dialect_lower in ("postgresql", "postgres"):
        prompt = _build_postgres_prompt(user_message, schema)
    elif dialect_lower == "mysql":
        prompt = _build_mysql_prompt(user_message, schema)
    elif dialect_lower == "sqlite":
        prompt = _build_sqlite_prompt(user_message, schema)
    else:
        # Try to detect from user message
        user_lower = (user_message or "").lower()
        if any(k in user_lower for k in ["mssql", "sql server", "sqlserver"]):
            prompt = _build_mssql_prompt(user_message, schema)
        elif any(k in user_lower for k in ["postgresql", "postgres", "postgre", "pg"]):
            prompt = _build_postgres_prompt(user_message, schema)
        elif "mysql" in user_lower:
            prompt = _build_mysql_prompt(user_message, schema)
        elif "sqlite" in user_lower:
            prompt = _build_sqlite_prompt(user_message, schema)
        else:
            prompt = _build_generic_prompt(user_message, schema)

    raw = await generate_async(
        prompt=prompt,
        temperature=0.0,
        max_tokens=getattr(settings, "LLM_MAX_TOKENS", 512),
        use_cache=True,
    )
    raw = _normalize_llm_sql((raw or "").strip())

    return raw


# Exports for optimized service
__all__ = [
    "generate_sql",
    "_normalize_llm_sql",
    "_build_mssql_prompt",
    "_build_postgres_prompt",
    "_build_mysql_prompt",
    "_build_sqlite_prompt",
    "_build_generic_prompt",
]

# Alias for backward compatibility with optimized service
generate_sql_sync = generate_sql