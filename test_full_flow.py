import sys
sys.path.insert(0, 'backend')

import logging
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

print("Step 1: Starting...", file=sys.stdout)
sys.stdout.flush()

from app.llm.nl2sql import generate_sql
from app.db.session import get_schema_summary

print("Step 2: Getting schema...", file=sys.stdout)
sys.stdout.flush()
schema = get_schema_summary()
print(f"Schema loaded: {len(schema)} chars", file=sys.stdout)
sys.stdout.flush()

print("Step 3: Generating SQL...", file=sys.stdout)
sys.stdout.flush()
sql = generate_sql("Show me all users", schema)
print(f"SQL: {sql}", file=sys.stdout)
sys.stdout.flush()

print("Done!", file=sys.stdout)
