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
- EXTREMELY IMPORTANT: You MUST enclose all Table Names and Column Names in double quotes exactly as they appear in the schema (e.g. "TaxRequest_Master", "ReceiptNo"). Do not change the capitalization.
- If returning raw rows, add LIMIT 50.

DATABASE SCHEMA:
{schema_hint}

USER REQUEST:
{user_message}

SQL:
""".strip()

    return _client.generate(prompt).strip()