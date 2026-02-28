"""
Application configuration loaded from environment variables.
"""

from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+psycopg2://postgres:postgres@localhost:5432/nagrpalika"

    # ── LLM (Local, free) ─────────────────────────────────────────────────────
    LLM_PROVIDER: str = "ollama"
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "qwen2.5-coder:0.5b"

    # ── Query safety ──────────────────────────────────────────────────────────
    DEFAULT_ROW_LIMIT: int = 50
    MAX_ROW_LIMIT: int = 500

    # ── App ───────────────────────────────────────────────────────────────────
    APP_NAME: str = "NL2SQL Chatbot"
    DEBUG: bool = False
    CORS_ORIGINS: List[str] = ["*"]

    # ── Connection pool ───────────────────────────────────────────────────────
    DB_POOL_SIZE: int = 500
    DB_MAX_OVERFLOW: int = 1000
    DB_POOL_TIMEOUT: int = 300000000
    DB_POOL_RECYCLE: int = 1800000


@lru_cache
def get_settings() -> Settings:
    return Settings()