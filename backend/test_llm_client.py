import sys
sys.path.insert(0, '.')

from app.core.config import get_settings

settings = get_settings()
print(f"OLLAMA_BASE_URL: {settings.OLLAMA_BASE_URL}")
print(f"OLLAMA_MODEL: {settings.OLLAMA_MODEL}")

# Now test the LLM client
import requests

prompt = "What is 2+2?"
print(f"\nCalling Ollama directly with prompt: {prompt}")

r = requests.post(
    f"{settings.OLLAMA_BASE_URL}/api/generate",
    json={
        "model": settings.OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    },
    timeout=300,
)

print(f"Status: {r.status_code}")
data = r.json()
print(f"Response: {data.get('response', 'NO RESPONSE')}")
