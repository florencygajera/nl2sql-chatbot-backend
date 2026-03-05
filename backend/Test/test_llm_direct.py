import requests
import json

def test_ollama():
    prompt = 'What is 2+2?'

    resp = requests.post(
        'http://localhost:11434/api/generate',
        json={
            'model': 'qwen2.5-coder:0.5b',
            'prompt': prompt,
            'stream': False
        },
        timeout=300
    )

    print('Status code:', resp.status_code)
    result = resp.json()
    print('Response field:', repr(result.get('response')))
    return result

if __name__ == "__main__":
    test_ollama()
