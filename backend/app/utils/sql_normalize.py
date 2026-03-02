from __future__ import annotations
import re

_CODE_FENCE = re.compile(r"^\s*```(?:sql)?\s*|\s*```\s*$", re.IGNORECASE | re.MULTILINE)

def normalize_llm_sql(text: str) -> str:
    """
    Make LLM SQL safe/usable for Postgres execution:
    - Remove ```sql fences
    - Remove accidental leading/trailing quotes
    - Convert doubled quotes ""Table"" -> "Table"
    """
    if not text:
        return ""

    s = text.strip()

    # remove code fences
    s = re.sub(_CODE_FENCE, "", s).strip()

    # if wrapped inside quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # fix doubled identifier quotes caused by escaping
    s = s.replace('""', '"')

    return s.strip()