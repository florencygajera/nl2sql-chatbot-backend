"""
NL2SQL Prompt Engineering System
Dialect-aware prompt templates for natural language to SQL conversion.
"""

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class DialectRules:
    """Database dialect-specific rules and syntax."""
    name: str
    limit_syntax: str  # How to limit results
    identifier_quote: str  # How to quote identifiers
    offset_syntax: str  # How to handle offset
    top_keyword: Optional[str]  # For MSSQL
    fetch_syntax: Optional[str]  # For Oracle
    max_identifier_length: int
    supports_cte: bool
    supports_window_functions: bool


# Dialect rules configuration
DIALECT_RULES = {
    "postgres": DialectRules(
        name="postgres",
        limit_syntax="LIMIT {n}",
        identifier_quote='"',
        offset_syntax="OFFSET {n}",
        top_keyword=None,
        fetch_syntax=None,
        max_identifier_length=63,
        supports_cte=True,
        supports_window_functions=True
    ),
    "mysql": DialectRules(
        name="mysql",
        limit_syntax="LIMIT {n}",
        identifier_quote="`",
        offset_syntax="OFFSET {n}",
        top_keyword=None,
        fetch_syntax=None,
        max_identifier_length=64,
        supports_cte=True,
        supports_window_functions=True
    ),
    "mssql": DialectRules(
        name="mssql",
        limit_syntax="",  # Uses TOP keyword instead
        identifier_quote="[",
        offset_syntax="OFFSET {n} ROWS",
        top_keyword="TOP {n}",
        fetch_syntax=None,
        max_identifier_length=128,
        supports_cte=True,
        supports_window_functions=True
    ),
    "sqlite": DialectRules(
        name="sqlite",
        limit_syntax="LIMIT {n}",
        identifier_quote='"',
        offset_syntax="OFFSET {n}",
        top_keyword=None,
        fetch_syntax=None,
        max_identifier_length=1000,
        supports_cte=True,
        supports_window_functions=True
    ),
    "oracle": DialectRules(
        name="oracle",
        limit_syntax="",
        identifier_quote='"',
        offset_syntax="OFFSET {n} ROWS",
        top_keyword=None,
        fetch_syntax="FETCH FIRST {n} ROWS ONLY",
        max_identifier_length=30,
        supports_cte=True,
        supports_window_functions=True
    )
}


# System prompts for different scenarios
SYSTEM_PROMPTS = {
    "default": """You are a dialect-aware NL2SQL generator. Your task is to convert natural language questions into valid SQL queries based on the provided database schema.

Output ONLY valid JSON with the SQL query. Do not include any explanatory text outside the JSON structure.

CRITICAL REQUIREMENTS:
1. Output strictly valid JSON - no markdown, no code blocks
2. Use the correct SQL dialect syntax for the target database
3. Always limit results to prevent excessive data retrieval
4. Use proper table and column references based on the schema
5. Handle aggregations, joins, and filters correctly""",

    "strict": """You are a strict NL2SQL generator. Output ONLY JSON. No explanations, no markdown, no code blocks.

Schema context:
{schema_summary}

Dialect: {dialect}

Dialect-specific rules:
{dialect_rules}

Return JSON with:
- sql_query: The generated SQL
- dialect: The target dialect
- confidence_score: 0.0-1.0
- referenced_tables: List of tables used
- needs_clarification: boolean
- clarification_question: Question if clarification needed""",

    "repair": """Fix the JSON output. The previous output was not valid JSON. Return ONLY valid JSON with no other text.

Required fields:
- sql_query: The SQL query
- dialect: The SQL dialect
- confidence_score: 0.0-1.0
- referenced_tables: List of tables
- needs_clarification: boolean
- clarification_question: String if needed"""
}


def get_system_prompt(dialect: str = "postgres", strict: bool = False) -> str:
    """Get the appropriate system prompt based on dialect and mode."""
    if strict:
        return SYSTEM_PROMPTS["strict"].format(
            schema_summary="{schema_summary}",
            dialect=dialect,
            dialect_rules=get_dialect_rules_string(dialect)
        )
    return SYSTEM_PROMPTS["default"]


def get_dialect_rules_string(dialect: str) -> str:
    """Get a string representation of dialect rules."""
    if dialect not in DIALECT_RULES:
        dialect = "postgres"
    
    rules = DIALECT_RULES[dialect]
    return f"""
- Use LIMIT {rules.limit_syntax.replace('{n}', 'N')} for limiting rows
- Quote identifiers using {rules.identifier_quote}identifier{')' if rules.identifier_quote == '[' else '"'}
- Use OFFSET {rules.offset_syntax.replace('{n}', 'N')} for pagination
{f"- Use TOP {rules.top_keyword.replace('{n}', 'N')} for limiting" if rules.top_keyword else ""}
{f"- Use FETCH FIRST N ROWS ONLY for limiting" if rules.fetch_syntax else ""}
- CTEs supported: {rules.supports_cte}
- Window functions supported: {rules.supports_window_functions}
"""


def build_user_prompt(
    question: str,
    schema_summary: str,
    retrieved_schema_snippet: str,
    dialect: str = "postgres",
    include_examples: bool = True
) -> str:
    """
    Build the user prompt with schema context and question.
    
    Args:
        question: Natural language question
        schema_summary: Full schema summary
        retrieved_schema_snippet: TF-IDF retrieved relevant schema portion
        dialect: Target SQL dialect
        include_examples: Whether to include examples
    
    Returns:
        Formatted user prompt
    """
    prompt = f"""Given the following database schema and user question, generate a SQL query.

DATABASE SCHEMA (Full):
{schema_summary}

RELEVANT SCHEMA (TF-IDF Retrieved):
{retrieved_schema_snippet}

USER QUESTION: {question}

TARGET DIALECT: {dialect}

DIALECT RULES:
{get_dialect_rules_string(dialect)}

OUTPUT FORMAT (JSON):
```json
{{
  "sql_query": "SELECT ...",
  "dialect": "{dialect}",
  "confidence_score": 0.95,
  "referenced_tables": ["table1", "table2"],
  "needs_clarification": false,
  "clarification_question": null
}}
```

Remember:
1. Output only valid JSON - no markdown, no explanations
2. Use correct dialect syntax
3. Include LIMIT to prevent excessive results (default 1000 rows)
4. Use proper table and column names from the schema
5. Handle JOINs correctly using foreign key relationships"""
    
    return prompt


def build_test_prompt(
    question: str,
    schema_summary: str,
    dialect: str = "postgres"
) -> str:
    """Build a test prompt for evaluation."""
    return f"""Question: {question}

Schema: {schema_summary}

Generate SQL for dialect: {dialect}

Output only JSON:"""


def parse_model_response(response_text: str) -> Dict[str, Any]:
    """
    Parse model response into structured JSON.
    
    Args:
        response_text: Raw model output
    
    Returns:
        Parsed JSON or error structure
    """
    # Try to extract JSON from response
    try:
        # First attempt: direct parse
        result = json.loads(response_text.strip())
        return {
            "raw_text": response_text,
            "parsed": result,
            "errors": []
        }
    except json.JSONDecodeError:
        pass
    
    # Second attempt: extract JSON object from text
    try:
        # Look for JSON object in the text
        import re
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return {
                "raw_text": response_text,
                "parsed": result,
                "errors": ["Had to extract JSON from text"]
            }
    except (json.JSONDecodeError, AttributeError):
        pass
    
    # Third attempt: try to fix common issues
    try:
        # Remove markdown code blocks if present
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        result = json.loads(cleaned)
        return {
            "raw_text": response_text,
            "parsed": result,
            "errors": ["Had to clean markdown"]
        }
    except json.JSONDecodeError:
        pass
    
    # Failed to parse
    return {
        "raw_text": response_text,
        "parsed": None,
        "errors": ["Failed to parse response as JSON"]
    }


def format_sql_for_dialect(sql: str, dialect: str) -> str:
    """Format SQL query according to dialect rules."""
    if dialect not in DIALECT_RULES:
        dialect = "postgres"
    
    rules = DIALECT_RULES[dialect]
    
    # Ensure LIMIT clause is present
    if "LIMIT" not in sql.upper() and "TOP" not in sql.upper():
        sql = sql.strip().rstrip(";") + f" LIMIT 1000"
    
    # For MSSQL, convert LIMIT to TOP if not present
    if dialect == "mssql" and "TOP" not in sql.upper():
        sql = sql.replace("SELECT", "SELECT TOP 1000", 1)
    
    return sql


def get_limit_clause(dialect: str, n: int = 1000) -> str:
    """Get the appropriate LIMIT clause for the dialect."""
    if dialect not in DIALECT_RULES:
        dialect = "postgres"
    
    rules = DIALECT_RULES[dialect]
    
    if rules.top_keyword:
        return rules.top_keyword.format(n=n)
    elif rules.fetch_syntax:
        return rules.fetch_syntax.format(n=n)
    else:
        return rules.limit_syntax.format(n=n)


# Training prompt templates
TRAINING_PROMPTS = {
    "system": """You are a dialect-aware NL2SQL generator. Output ONLY valid JSON.""",
    
    "user_template": """Schema: {schema}
Question: {question}
Dialect: {dialect}""",
    
    "assistant_template": """{output_json}"""
}


def format_training_example(
    schema: str,
    question: str,
    sql: str,
    dialect: str,
    referenced_tables: List[str],
    confidence: float = 0.95
) -> Dict[str, str]:
    """Format a training example in conversation format."""
    output_json = json.dumps({
        "sql_query": sql,
        "dialect": dialect,
        "confidence_score": confidence,
        "referenced_tables": referenced_tables,
        "needs_clarification": False,
        "clarification_question": None
    }, ensure_ascii=False)
    
    return {
        "system": TRAINING_PROMPTS["system"],
        "user": TRAINING_PROMPTS["user_template"].format(
            schema=schema,
            question=question,
            dialect=dialect
        ),
        "assistant": output_json
    }


# Prompt for JSON repair
JSON_REPAIR_PROMPT = """The following text was supposed to be valid JSON but isn't. Fix it and return ONLY valid JSON:

{broken_json}

Return ONLY JSON, no explanations:"""