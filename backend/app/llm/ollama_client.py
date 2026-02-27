from __future__ import annotations

import requests
from app.core.config import get_settings


def call_ollama(prompt: str) -> str:
    """
    Calls local Ollama server and returns model response text.
    """
    s = get_settings()

    resp = requests.post(
        f"{s.OLLAMA_BASE_URL}/api/generate",
        json={
            "model": s.OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", "")