"""
Application configuration loaded from environment variables.
"""

from __future__ import annotations

from functools import lru_cache
from typing import List
from cryptography.fernet import Fernet
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    def validate_settings(s: Settings) -> None:
        # Validate Fernet key
        try:
            Fernet(s.FERNET_KEY.encode("utf-8"))
        except Exception as e:
            raise RuntimeError(
                "Invalid FERNET_KEY. Generate one with:\n"
                'python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
        ) from e

    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL: str = "sqlite:///./default.db"

    # ── LLM (Local, free) ─────────────────────────────────────────────────────
    LLM_PROVIDER: str = "ollama"
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "qwen2.5-coder:0.5b"
    
    # ── NL2SQL Local LLM Service ───────────────────────────────────────────────
    LLM_SERVICE_URL: str = "http://localhost:8000"  # Local NL2SQL inference server

    # ── Query safety ──────────────────────────────────────────────────────────
    DEFAULT_ROW_LIMIT: int = 50
    MAX_ROW_LIMIT: int = 500

    # ── DB sessions (dynamic attach) ─────────────────────────────────────────
    DB_SESSION_TTL_SECONDS: int = 604800  # 7 days (essentially until refresh)

    # ── App ───────────────────────────────────────────────────────────────────
    APP_NAME: str = "NL2SQL Chatbot"
    DEBUG: bool = False
    CORS_ORIGINS: List[str] = ["*"]

    # ── Connection pool ───────────────────────────────────────────────────────
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800

    # ── Performance / Caching ───────────────────────────────────────────────────
    ENABLE_SCHEMA_CACHE: bool = True
    SCHEMA_CACHE_TTL_SECONDS: int = 300  # 5 minutes
    ENABLE_LLM_CACHE: bool = True
    LLM_CACHE_TTL_SECONDS: int = 600  # 10 minutes
    LLM_MAX_CONNECTIONS: int = 10
    LLM_TIMEOUT_SECONDS: int = 600
    ENABLE_PERFORMANCE_MONITORING: bool = True
    # ── LLM generation tuning ─────────────────────────────────────────────────
    LLM_NUM_CTX: int = 8192
    LLM_MAX_TOKENS: int = 256

    # ── NL→SQL accuracy tuning ───────────────────────────────────────────────
    NL2SQL_TOP_TABLES: int = 10
    NL2SQL_MAX_RETRIES: int = 2



@lru_cache
def get_settings() -> Settings:
    return Settings()


_settings = get_settings()