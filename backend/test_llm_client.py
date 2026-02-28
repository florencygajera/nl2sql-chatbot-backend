import sys
sys.path.insert(0, '.')

# Force reload of settings
import importlib
import app.core.config
importlib.reload(app.core.config)

from app.core.config import get_settings

settings = get_settings()
print(f"OLLAMA_MODEL: {settings.OLLAMA_MODEL}")

# Test LLM client with SQL generation
from app.llm.nl2sql import generate_sql
from app.db.session import get_schema_summary

print("\n--- Testing full flow ---")
print("Getting schema...")
schema = get_schema_summary()
print(f"Schema loaded: {len(schema)} characters")

print("\nGenerating SQL...")
sql = generate_sql("Show me all users", schema)
print(f"Generated SQL: {sql}")
