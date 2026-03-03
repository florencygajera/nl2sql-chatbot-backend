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
        import re
        # Handle odbc_connect URLs
        if "odbc_connect" in url:
            return re.sub(r'PWD=[^;%]*', 'PWD=***', url)
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


def _detect_db_dialect(url: str) -> str:
    """Detect the database dialect from the URL."""
    url_lower = url.lower()
    if "mssql" in url_lower or "sqlserver" in url_lower:
        return "mssql"
    elif "postgresql" in url_lower or "postgres" in url_lower:
        return "postgresql"
    elif "mysql" in url_lower:
        return "mysql"
    elif "sqlite" in url_lower:
        return "sqlite"
    elif "oracle" in url_lower:
        return "oracle"
    return "unknown"


# NOTE: This is an in-memory state (single-process). If you run multiple
# workers, each worker keeps its own active DB engine.
active_db_info = {
    "url": settings.DATABASE_URL,
    "masked_url": _mask_db_url(settings.DATABASE_URL),
    "status": "connected",
    "dialect": _detect_db_dialect(settings.DATABASE_URL),
    "source": {
        "attach_mode": "ENV_DEFAULT",  # ENV_DEFAULT | UPLOAD_FILE | CONNECTION
        "db_type": _detect_db_dialect(settings.DATABASE_URL),
        "details": {},
    },
}


def create_app_engine(url: str):
    kwargs = {"pool_pre_ping": True, "echo": settings.DEBUG}

    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
        return create_engine(url, **kwargs)

    # Common pool settings for server DBs
    kwargs.update({
        "pool_size": settings.DB_POOL_SIZE,
        "max_overflow": settings.DB_MAX_OVERFLOW,
        "pool_timeout": settings.DB_POOL_TIMEOUT,
        "pool_recycle": settings.DB_POOL_RECYCLE,
    })

    dialect = _detect_db_dialect(url)

    # Postgres-specific connect args
    if dialect == "postgresql":
        kwargs["connect_args"] = {
            "connect_timeout": 10,
            "options": "-c statement_timeout=30000"
        }

    # MSSQL: if using odbc_connect URL, do NOT pass connect_args —
    # the Connection Timeout is already embedded in the ODBC string.
    # Passing connect_args overrides it and causes 30s timeouts.
    elif dialect == "mssql":
        if "odbc_connect" not in url:
            kwargs["connect_args"] = {"timeout": 30}
        # else: no connect_args needed; ODBC string has Connection Timeout=60

    # MySQL and others
    else:
        kwargs["connect_args"] = {"connect_timeout": 10}

    logger.warning(
        "DB Pool Configuration - pool_size=%d, max_overflow=%d, pool_timeout=%d (seconds), pool_recycle=%d (seconds)",
        settings.DB_POOL_SIZE,
        settings.DB_MAX_OVERFLOW,
        settings.DB_POOL_TIMEOUT,
        settings.DB_POOL_RECYCLE
    )

    return create_engine(url, **kwargs)


engine = create_app_engine(settings.DATABASE_URL)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


def _test_engine_connect(new_url: str) -> None:
    """
    Fast connection test with DB-specific timeout args to avoid pyodbc prelogin hangs
    and to prevent invalid attributes from being passed to the wrong driver.
    """
    test_kwargs = {"pool_pre_ping": True}
    dialect = _detect_db_dialect(new_url)

    if new_url.startswith("sqlite"):
        test_kwargs["connect_args"] = {"check_same_thread": False}
    elif dialect == "mssql" and "odbc_connect" not in new_url:
        test_kwargs["connect_args"] = {"timeout": 30}
    elif dialect == "postgresql":
        test_kwargs["connect_args"] = {"connect_timeout": 10}
    elif dialect != "mssql":
        test_kwargs["connect_args"] = {"connect_timeout": 10}
    # mssql with odbc_connect: no connect_args — timeout is in ODBC string

    test_engine = create_engine(new_url, **test_kwargs)
    try:
        with test_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    finally:
        test_engine.dispose()


def set_database_url(new_url: str) -> None:
    global engine, SessionLocal, active_db_info

    dialect = _detect_db_dialect(new_url)

    # 1) Test connection first with correct timeout arg per DB
    test_kwargs = {"pool_pre_ping": True}

    if new_url.startswith("sqlite"):
        test_kwargs["connect_args"] = {"check_same_thread": False}
    elif dialect == "mssql" and "odbc_connect" not in new_url:
        test_kwargs["connect_args"] = {"timeout": 30}
    elif dialect == "postgresql":
        test_kwargs["connect_args"] = {"connect_timeout": 10}
    elif dialect != "mssql":
        test_kwargs["connect_args"] = {"connect_timeout": 10}
    # mssql with odbc_connect: no connect_args — timeout is in ODBC string

    test_engine = create_engine(new_url, **test_kwargs)
    try:
        with test_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    finally:
        test_engine.dispose()

    # 2) Cleanup old engine
    engine.dispose()

    # 3) Create new engine
    engine = create_app_engine(new_url)
    SessionLocal.configure(bind=engine)
    active_db_info["url"] = new_url
    active_db_info["masked_url"] = _mask_db_url(new_url)
    active_db_info["status"] = "connected"
    active_db_info["dialect"] = dialect


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
    default_dialect = _detect_db_dialect(DEFAULT_DATABASE_URL)
    set_database_source(attach_mode="ENV_DEFAULT", db_type=default_dialect, details={})


def get_db() -> Generator[Session, None, None]:
    import time
    start = time.time()
    db = SessionLocal()
    acquire_time = time.time() - start
    logger.debug("Database session acquired in %.3f seconds", acquire_time)
    if acquire_time > 5:
        logger.warning("SLOW DB CONNECTION ACQUIRE: took %.3f seconds!", acquire_time)
    try:
        yield db
    finally:
        db.close()


def get_current_dialect() -> str:
    """Return the current DB dialect (mssql, postgresql, mysql, sqlite, oracle, unknown)."""
    return active_db_info.get("dialect", "unknown")


def get_schema_summary() -> str:
    """
    Fetch schema exactly once using a single SQL query (no N+1 inspector calls).
    
    Uses INFORMATION_SCHEMA.COLUMNS for MSSQL/PostgreSQL/MySQL (one round trip),
    and falls back to SQLAlchemy inspector for SQLite.
    """
    dialect = active_db_info.get("dialect", "unknown")
    ignore_cols = {"insertedby", "inserteddatetime", "updatedby", "updateddatetime"}
    ignore_tables = {"__efmigrationshistory", "sysdiagrams"}

    # ── Single-query path for server databases ─────────────────────────────────
    if dialect in ("mssql", "postgresql", "mysql"):
        if dialect == "mssql":
            sql = text("""
                SELECT
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
            """)
        elif dialect == "postgresql":
            sql = text("""
                SELECT
                    t.table_name AS TABLE_NAME,
                    c.column_name AS COLUMN_NAME,
                    c.data_type AS DATA_TYPE
                FROM information_schema.tables t
                JOIN information_schema.columns c
                    ON c.table_name = t.table_name
                    AND c.table_schema = t.table_schema
                WHERE t.table_type = 'BASE TABLE'
                  AND t.table_schema = 'public'
                ORDER BY t.table_name, c.ordinal_position
            """)
        else:  # mysql
            sql = text("""
                SELECT
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND t.TABLE_SCHEMA = DATABASE()
                ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
            """)

        try:
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()

            lines: list[str] = []
            current_table = None
            for row in rows:
                if dialect == "mssql":
                    table_schema, table_name, col_name, data_type = row
                    full_table_name = f'{table_schema}.{table_name}'
                else:
                    table_name, col_name, data_type = row
                    full_table_name = table_name

                if table_name.lower() in ignore_tables:
                    continue
                if col_name.lower() in ignore_cols:
                    continue
                if full_table_name != current_table:
                    if current_table is not None:
                        lines.append("")
                    lines.append(f'Table: "{full_table_name}"')
                    current_table = full_table_name
                lines.append(f'  - "{col_name}" ({data_type})')
            return "\n".join(lines).strip()

        except Exception as e:
            logger.warning("Fast schema query failed (%s), falling back to inspector", e)
            # Fall through to inspector below

    # ── SQLite (and fallback) path ─────────────────────────────────────────────
    inspector = inspect(engine)
    lines = []
    for table_name in inspector.get_table_names():
        if table_name.lower() in ignore_tables:
            continue
        lines.append(f'Table: "{table_name}"')
        for col in inspector.get_columns(table_name):
            if col['name'].lower() in ignore_cols:
                continue
            lines.append(f'  - "{col["name"]}" ({col["type"]})')
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
