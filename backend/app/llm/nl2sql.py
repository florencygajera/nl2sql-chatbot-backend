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
    
    import re
    # If the LLM wraps the query in markdown (even with conversational text before it)
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        
    return raw