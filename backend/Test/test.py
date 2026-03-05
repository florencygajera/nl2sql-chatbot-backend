import requests

def call_ollama(prompt):
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "mistral",   # or deepseek-coder
            "prompt": prompt,
            "stream": False
        }
    )
    return response.json()["response"]

prompt = """
You are a SQL generator.
Convert this into SQL:
Total salary of all employees.
"""

print(call_ollama(prompt))