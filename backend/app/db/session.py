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

active_db_info = {
    "url": settings.DATABASE_URL,
    "status": "connected"
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
    active_db_info["status"] = "connected"


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_schema_summary() -> str:
    inspector = inspect(engine)
    lines: list[str] = []

    for table_name in inspector.get_table_names():
        lines.append(f"Table: {table_name}")
        for col in inspector.get_columns(table_name):
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            lines.append(f"  - {col['name']} ({col['type']}, {nullable})")

        for fk in inspector.get_foreign_keys(table_name):
            lines.append(
                f"  FK: {fk['constrained_columns']} -> "
                f"{fk['referred_table']}.{fk['referred_columns']}"
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