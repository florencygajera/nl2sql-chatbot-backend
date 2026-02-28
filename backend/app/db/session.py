"""
Database engine, session factory, and schema introspection utilities.
"""

from __future__ import annotations

import logging
from typing import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

DEFAULT_DATABASE_URL = settings.DATABASE_URL

def _mask_db_url(url: str) -> str:
    """Hide passwords in URLs for safe logging / status responses."""
    try:
        if "://" not in url or "@" not in url:
            return url
        scheme, rest = url.split("://", 1)
        creds_and_host = rest.split("@", 1)
        if len(creds_and_host) != 2:
            return url
        creds, host_and_path = creds_and_host
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{scheme}://{user}:***@{host_and_path}"
        return url
    except Exception:
        return url


# NOTE: This is an in-memory state (single-process). If you run multiple
# workers, each worker keeps its own active DB engine.
active_db_info = {
    "url": settings.DATABASE_URL,
    "masked_url": _mask_db_url(settings.DATABASE_URL),
    "status": "connected",
    "source": {
        "attach_mode": "ENV_DEFAULT",  # ENV_DEFAULT | UPLOAD_FILE | CONNECTION
        "db_type": "postgres",
        "details": {},
    },
}

def create_app_engine(url: str):
    kwargs = {"pool_pre_ping": True, "echo": settings.DEBUG}
    if not url.startswith("sqlite"):
        kwargs.update({
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "pool_timeout": settings.DB_POOL_TIMEOUT,
            "pool_recycle": settings.DB_POOL_RECYCLE,
        })
    else:
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)

engine = create_app_engine(settings.DATABASE_URL)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)

def set_database_url(new_url: str) -> None:
    global engine, SessionLocal, active_db_info
    
    # 1. Test connection first
    test_engine = create_engine(new_url, pool_pre_ping=True)
    with test_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    test_engine.dispose()
    
    # 2. Cleanup old engine
    engine.dispose()
    
    # 3. Create new engine
    engine = create_app_engine(new_url)
    SessionLocal.configure(bind=engine)
    active_db_info["url"] = new_url
    active_db_info["masked_url"] = _mask_db_url(new_url)
    active_db_info["status"] = "connected"


def set_database_source(*, attach_mode: str, db_type: str, details: dict) -> None:
    """Attach metadata about how the current DB was configured."""
    active_db_info["source"] = {
        "attach_mode": attach_mode,
        "db_type": db_type,
        "details": details,
    }


def reset_database_url() -> None:
    """Reset to the DATABASE_URL from environment/config."""
    set_database_url(DEFAULT_DATABASE_URL)
    set_database_source(attach_mode="ENV_DEFAULT", db_type="postgres", details={})


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_schema_summary() -> str:
    inspector = inspect(engine)
    lines: list[str] = []

    ignore_cols = {"insertedby", "inserteddatetime", "updatedby", "updateddatetime"}
    ignore_tables = {"__EFMigrationsHistory"}

    for table_name in inspector.get_table_names():
        if table_name in ignore_tables:
            continue
            
        lines.append(f'Table: "{table_name}"')
        for col in inspector.get_columns(table_name):
            if col['name'].lower() in ignore_cols:
                continue
            lines.append(f'  - "{col["name"]}" ({col["type"]})')

        for fk in inspector.get_foreign_keys(table_name):
            lines.append(
                f'  FK: "{fk["constrained_columns"][0]}" -> '
                f'"{fk["referred_table"]}"."{fk["referred_columns"][0]}"'
            )
        lines.append("")

    return "\n".join(lines).strip()

def ping_database() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        active_db_info["status"] = "connected"
        return True
    except Exception as e:
        logger.error(f"Database ping failed: {e}")
        active_db_info["status"] = "unreachable"
        return False