from __future__ import annotations

from app.llm.client import LLMClient


_client = LLMClient()


def generate_sql(user_message: str, schema_hint: str) -> str:
    prompt = f"""<|im_start|>system
You are a highly capable SQL generator.
STRICT RULES:
1. Output ONLY ONE bare SQL query. Absolutely NO markdown backticks like ```sql or ```.
2. Output ONLY SELECT queries.
3. EXTREMELY IMPORTANT: You MUST enclose all Table Names and Column Names in double quotes exactly as they appear in the schema (e.g. "TaxRequest_Master", "ReceiptNo"). Do not change the capitalization.
4. If returning raw rows, add LIMIT 50.

DATABASE SCHEMA:
{schema_hint}
<|im_end|>
<|im_start|>user
{user_message}
<|im_end|>
<|im_start|>assistant
""".strip()
    
    raw = _client.generate(prompt).strip()
    if raw.startswith("```sql") and raw.endswith("```"):
        raw = raw[6:-3].strip()
    elif raw.startswith("```") and raw.endswith("```"):
        raw = raw[3:-3].strip()
        
    return raw