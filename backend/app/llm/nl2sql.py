from __future__ import annotations

from app.llm.client import LLMClient


_client = LLMClient()


def generate_sql(user_message: str, schema_hint: str) -> str:
    prompt = f"""
You are a SQL generator.

STRICT RULES:
- Output ONLY ONE SQL query.
- Output ONLY SELECT queries.
- No markdown. No explanations. No code fences.
- Never use INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/CREATE.
- If returning raw rows, add LIMIT 50.

DATABASE SCHEMA:
{schema_hint}

USER REQUEST:
{user_message}

SQL:
""".strip()

    return _client.generate(prompt).strip()