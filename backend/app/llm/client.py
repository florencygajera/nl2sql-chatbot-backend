from __future__ import annotations

import requests
from app.core.config import get_settings


class LLMClient:
    def __init__(self) -> None:
        self.settings = get_settings()

    def generate(self, prompt: str) -> str:
        provider = (self.settings.LLM_PROVIDER or "").lower()

        if provider == "ollama":
            return self._ollama_generate(prompt)

        raise RuntimeError(f"Unsupported LLM_PROVIDER: {self.settings.LLM_PROVIDER}")

    def _ollama_generate(self, prompt: str) -> str:
        r = requests.post(
            f"{self.settings.OLLAMA_BASE_URL}/api/generate",
            json={
                "model": self.settings.OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
            },
            timeout=60,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()