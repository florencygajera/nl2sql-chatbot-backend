import time
import requests
import json
from app.db.session import get_schema_summary

schema = get_schema_summary()
prompt = f"Schema:\n{schema}\n\nTask: Output a SQL query to select all from \"User_Master\""

print(f"Sending prompt of length {len(prompt)} to Ollama")
start = time.time()
resp = requests.post(
    'http://localhost:11434/api/generate',
    json={
        'model': 'qwen2.5-coder:0.5b',
        'prompt': prompt,
        'stream': False
    }
)
elapsed = time.time() - start
print(f"Time: {elapsed:.2f}s")
try:
    print(resp.json()['response'])
except Exception as e:
    print("Error:", e, resp.text)
