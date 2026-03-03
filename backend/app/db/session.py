"""
Database engine, session factory, and schema introspection utilities.

Upgrades in this version
------------------------
✅ Adds foreign-key relationship extraction (sys.foreign_keys / pg / mysql) for better joins
✅ get_schema_catalog() is untruncated and consistent format
✅ get_schema_for_tables() returns ONLY requested tables + ALL columns + FK hints
✅ Keeps existing connection handling (ODBC SQL Server included)
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
        if "odbc_connect" in url:
            return re.sub(r"PWD=[^;%]*", "PWD=***", url)
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
    url_lower = (url or "").lower()
    if "mssql" in url_lower or "sqlserver" in url_lower:
        return "mssql"
    if "postgresql" in url_lower or "postgres" in url_lower:
        return "postgresql"
    if "mysql" in url_lower:
        return "mysql"
    if "sqlite" in url_lower:
        return "sqlite"
    if "oracle" in url_lower:
        return "oracle"
    return "unknown"


# NOTE: single-process in-memory state. Multiple workers => separate state.
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
    kwargs.update(
        {
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "pool_timeout": settings.DB_POOL_TIMEOUT,
            "pool_recycle": settings.DB_POOL_RECYCLE,
        }
    )

    dialect = _detect_db_dialect(url)

    if dialect == "postgresql":
        kwargs["connect_args"] = {"connect_timeout": 10, "options": "-c statement_timeout=30000"}
    elif dialect == "mssql":
        # If using odbc_connect URL, do NOT pass connect_args; timeout is in ODBC string.
        if "odbc_connect" not in url:
            kwargs["connect_args"] = {"timeout": 30}
    else:
        kwargs["connect_args"] = {"connect_timeout": 10}

    logger.warning(
        "DB Pool Configuration - pool_size=%d, max_overflow=%d, pool_timeout=%d (seconds), pool_recycle=%d (seconds)",
        settings.DB_POOL_SIZE,
        settings.DB_MAX_OVERFLOW,
        settings.DB_POOL_TIMEOUT,
        settings.DB_POOL_RECYCLE,
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
    """Fast connection test with DB-specific timeout args."""
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
    active_db_info["source"] = {"attach_mode": attach_mode, "db_type": db_type, "details": details}


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


def ping_database() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        active_db_info["status"] = "connected"
        return True
    except Exception as e:
        logger.error("Database ping failed: %s", e)
        active_db_info["status"] = "unreachable"
        return False


# ── FK introspection (KEY accuracy booster) ───────────────────────────────────

def get_foreign_keys_catalog() -> str:
    """
    Return foreign key relationships as text hints:
      FK: dbo.Child(ChildCol) -> dbo.Parent(ParentCol)

    If DB has no FK constraints, returns empty string.
    """
    dialect = active_db_info.get("dialect", "unknown")

    try:
        if dialect == "mssql":
            sql = text(
                """
                SELECT
                  OBJECT_SCHEMA_NAME(fk.parent_object_id) AS parent_schema,
                  OBJECT_NAME(fk.parent_object_id)        AS parent_table,
                  pc.name                                  AS parent_column,
                  OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
                  OBJECT_NAME(fk.referenced_object_id)        AS ref_table,
                  rc.name                                  AS ref_column
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                  ON fk.object_id = fkc.constraint_object_id
                JOIN sys.columns pc
                  ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
                JOIN sys.columns rc
                  ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
                ORDER BY parent_schema, parent_table
                """
            )
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()

        elif dialect == "postgresql":
            sql = text(
                """
                SELECT
                  tc.table_schema AS parent_schema,
                  tc.table_name   AS parent_table,
                  kcu.column_name AS parent_column,
                  ccu.table_schema AS ref_schema,
                  ccu.table_name   AS ref_table,
                  ccu.column_name  AS ref_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                 AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                ORDER BY parent_schema, parent_table
                """
            )
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()

        elif dialect == "mysql":
            sql = text(
                """
                SELECT
                  kcu.TABLE_SCHEMA  AS parent_schema,
                  kcu.TABLE_NAME    AS parent_table,
                  kcu.COLUMN_NAME   AS parent_column,
                  kcu.REFERENCED_TABLE_SCHEMA AS ref_schema,
                  kcu.REFERENCED_TABLE_NAME   AS ref_table,
                  kcu.REFERENCED_COLUMN_NAME  AS ref_column
                FROM information_schema.KEY_COLUMN_USAGE kcu
                WHERE kcu.REFERENCED_TABLE_NAME IS NOT NULL
                  AND kcu.TABLE_SCHEMA = DATABASE()
                ORDER BY parent_table
                """
            )
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()
        else:
            return ""

        if not rows:
            return ""

        lines = ["Foreign Keys:"]
        for parent_schema, parent_table, parent_col, ref_schema, ref_table, ref_col in rows:
            p_full = f"{parent_schema}.{parent_table}" if parent_schema else str(parent_table)
            r_full = f"{ref_schema}.{ref_table}" if ref_schema else str(ref_table)
            lines.append(f"  FK: {p_full}({parent_col}) -> {r_full}({ref_col})")
        return "\n".join(lines).strip()

    except Exception as exc:
        logger.warning("FK introspection failed: %s", exc)
        return ""


# ── Schema introspection (summary + catalog + targeted) ───────────────────────

def get_schema_summary() -> str:
    """
    Truncated schema summary (fast preview): max 50 tables, 15 columns/table.
    """
    dialect = active_db_info.get("dialect", "unknown")
    ignore_cols = {"insertedby", "inserteddatetime", "updatedby", "updateddatetime"}
    ignore_tables = {"__efmigrationshistory", "sysdiagrams"}

    if dialect in ("mssql", "postgresql", "mysql"):
        if dialect == "mssql":
            sql = text(
                """
                SELECT
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )
        elif dialect == "postgresql":
            sql = text(
                """
                SELECT
                    t.table_schema AS TABLE_SCHEMA,
                    t.table_name   AS TABLE_NAME,
                    c.column_name  AS COLUMN_NAME,
                    c.data_type    AS DATA_TYPE,
                    c.ordinal_position AS ORDINAL_POSITION
                FROM information_schema.tables t
                JOIN information_schema.columns c
                    ON c.table_name = t.table_name
                    AND c.table_schema = t.table_schema
                WHERE t.table_type = 'BASE TABLE'
                  AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY t.table_schema, t.table_name, c.ordinal_position
                """
            )
        else:  # mysql
            sql = text(
                """
                SELECT
                    t.TABLE_SCHEMA AS TABLE_SCHEMA,
                    t.TABLE_NAME   AS TABLE_NAME,
                    c.COLUMN_NAME  AS COLUMN_NAME,
                    c.DATA_TYPE    AS DATA_TYPE,
                    c.ORDINAL_POSITION AS ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND t.TABLE_SCHEMA = DATABASE()
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )

        try:
            with engine.connect() as conn:
                rows = conn.execute(sql).fetchall()

            lines: list[str] = []
            current_table: str | None = None
            tables_processed = 0
            col_count = 0

            for table_schema, table_name, col_name, data_type, _pos in rows:
                if table_name and table_name.lower() in ignore_tables:
                    continue
                if col_name and col_name.lower() in ignore_cols:
                    continue

                full_table = (
                    f"{table_schema}.{table_name}"
                    if dialect == "mssql"
                    else (table_name if table_schema in ("public", "", None) else f"{table_schema}.{table_name}")
                )

                if full_table != current_table:
                    if tables_processed >= 50:
                        lines.append("... (schema truncated)")
                        break
                    if current_table is not None:
                        lines.append("")
                    lines.append(f'Table: "{full_table}"')
                    current_table = full_table
                    tables_processed += 1
                    col_count = 0

                col_count += 1
                if col_count <= 15:
                    lines.append(f'  - "{col_name}" ({data_type})')

            return "\n".join(lines).strip()

        except Exception as e:
            logger.warning("Fast schema query failed (%s), falling back to inspector", e)

    # SQLite / fallback
    inspector = inspect(engine)
    lines: list[str] = []
    tables_processed = 0
    for table_name in inspector.get_table_names():
        if table_name.lower() in ignore_tables:
            continue
        if tables_processed >= 50:
            lines.append("... (schema truncated)")
            break
        lines.append(f'Table: "{table_name}"')
        tables_processed += 1
        col_count = 0
        for col in inspector.get_columns(table_name):
            if col["name"].lower() in ignore_cols:
                continue
            col_count += 1
            if col_count <= 15:
                lines.append(f'  - "{col["name"]}" ({col["type"]})')
        lines.append("")
    return "\n".join(lines).strip()


def get_schema_catalog() -> str:
    """
    Untruncated schema catalog: ALL tables and ALL columns.
    Format matches get_schema_summary() (Table: ... + columns).
    Appends FK hints at the end (if present).
    """
    dialect = active_db_info.get("dialect", "unknown")
    ignore_cols = {"insertedby", "inserteddatetime", "updatedby", "updateddatetime"}
    ignore_tables = {"__efmigrationshistory", "sysdiagrams"}

    if dialect in ("mssql", "postgresql", "mysql"):
        if dialect == "mssql":
            sql = text(
                """
                SELECT
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )
        elif dialect == "postgresql":
            sql = text(
                """
                SELECT
                    t.table_schema AS TABLE_SCHEMA,
                    t.table_name   AS TABLE_NAME,
                    c.column_name  AS COLUMN_NAME,
                    c.data_type    AS DATA_TYPE,
                    c.ordinal_position AS ORDINAL_POSITION
                FROM information_schema.tables t
                JOIN information_schema.columns c
                    ON c.table_name = t.table_name
                    AND c.table_schema = t.table_schema
                WHERE t.table_type = 'BASE TABLE'
                  AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY t.table_schema, t.table_name, c.ordinal_position
                """
            )
        else:  # mysql
            sql = text(
                """
                SELECT
                    t.TABLE_SCHEMA AS TABLE_SCHEMA,
                    t.TABLE_NAME   AS TABLE_NAME,
                    c.COLUMN_NAME  AS COLUMN_NAME,
                    c.DATA_TYPE    AS DATA_TYPE,
                    c.ORDINAL_POSITION AS ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND t.TABLE_SCHEMA = DATABASE()
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )

        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()

        lines: list[str] = []
        current_table: str | None = None
        for table_schema, table_name, col_name, data_type, _pos in rows:
            if table_name and table_name.lower() in ignore_tables:
                continue
            if col_name and col_name.lower() in ignore_cols:
                continue

            full_table = (
                f"{table_schema}.{table_name}"
                if dialect == "mssql"
                else (table_name if table_schema in ("public", "", None) else f"{table_schema}.{table_name}")
            )

            if full_table != current_table:
                if current_table is not None:
                    lines.append("")
                lines.append(f'Table: "{full_table}"')
                current_table = full_table

            lines.append(f'  - "{col_name}" ({data_type})')

        fk_text = get_foreign_keys_catalog()
        if fk_text:
            lines.append("")
            lines.append(fk_text)

        return "\n".join(lines).strip()

    # SQLite / fallback
    inspector = inspect(engine)
    lines: list[str] = []
    for table_name in inspector.get_table_names():
        if table_name.lower() in ignore_tables:
            continue
        lines.append(f'Table: "{table_name}"')
        for col in inspector.get_columns(table_name):
            if col["name"].lower() in ignore_cols:
                continue
            lines.append(f'  - "{col["name"]}" ({col["type"]})')
        lines.append("")

    # (SQLite FK introspection via SQLAlchemy is inconsistent; skip by default)
    return "\n".join(lines).strip()


def get_schema_for_tables(table_names: list[str]) -> str:
    """
    Return schema text for only the requested tables (ALL columns),
    plus FK hints filtered to those tables if possible.

    table_names may include schema-qualified values (schema.table).
    """
    dialect = active_db_info.get("dialect", "unknown")
    if not table_names:
        return get_schema_summary()

    # Normalize requested tables
    wanted: list[tuple[str | None, str]] = []
    for t in table_names:
        t = (t or "").strip().strip('"').strip("'")
        if not t:
            continue
        if "." in t:
            sch, tbl = t.split(".", 1)
            wanted.append((sch, tbl))
        else:
            wanted.append((None, t))

    ignore_cols = {"insertedby", "inserteddatetime", "updatedby", "updateddatetime"}
    ignore_tables = {"__efmigrationshistory", "sysdiagrams"}

    if dialect in ("mssql", "postgresql", "mysql"):
        params: dict[str, str] = {}
        conds: list[str] = []

        if dialect == "mssql":
            for i, (sch, tbl) in enumerate(wanted):
                sch = sch or "dbo"
                conds.append(f"(t.TABLE_SCHEMA = :sch{i} AND t.TABLE_NAME = :tbl{i})")
                params[f"sch{i}"] = sch
                params[f"tbl{i}"] = tbl
            where = " OR ".join(conds) or "1=0"
            sql = text(
                f"""
                SELECT
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND ({where})
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )

        elif dialect == "postgresql":
            for i, (sch, tbl) in enumerate(wanted):
                sch = sch or "public"
                conds.append(f"(t.table_schema = :sch{i} AND t.table_name = :tbl{i})")
                params[f"sch{i}"] = sch
                params[f"tbl{i}"] = tbl
            where = " OR ".join(conds) or "1=0"
            sql = text(
                f"""
                SELECT
                    t.table_schema AS TABLE_SCHEMA,
                    t.table_name   AS TABLE_NAME,
                    c.column_name  AS COLUMN_NAME,
                    c.data_type    AS DATA_TYPE,
                    c.ordinal_position AS ORDINAL_POSITION
                FROM information_schema.tables t
                JOIN information_schema.columns c
                    ON c.table_name = t.table_name
                    AND c.table_schema = t.table_schema
                WHERE t.table_type = 'BASE TABLE'
                  AND ({where})
                ORDER BY t.table_schema, t.table_name, c.ordinal_position
                """
            )

        else:  # mysql
            for i, (_sch, tbl) in enumerate(wanted):
                conds.append(f"(t.TABLE_SCHEMA = DATABASE() AND t.TABLE_NAME = :tbl{i})")
                params[f"tbl{i}"] = tbl
            where = " OR ".join(conds) or "1=0"
            sql = text(
                f"""
                SELECT
                    t.TABLE_SCHEMA AS TABLE_SCHEMA,
                    t.TABLE_NAME   AS TABLE_NAME,
                    c.COLUMN_NAME  AS COLUMN_NAME,
                    c.DATA_TYPE    AS DATA_TYPE,
                    c.ORDINAL_POSITION AS ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON c.TABLE_NAME = t.TABLE_NAME
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                  AND ({where})
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
                """
            )

        with engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        lines: list[str] = []
        current: str | None = None
        kept_tables: set[str] = set()

        for table_schema, table_name, col_name, data_type, _pos in rows:
            if table_name and table_name.lower() in ignore_tables:
                continue
            if col_name and col_name.lower() in ignore_cols:
                continue

            full = (
                f"{table_schema}.{table_name}"
                if dialect == "mssql"
                else (table_name if table_schema in ("public", "", None) else f"{table_schema}.{table_name}")
            )

            if full != current:
                if current is not None:
                    lines.append("")
                lines.append(f'Table: "{full}"')
                current = full
                kept_tables.add(full)

            lines.append(f'  - "{col_name}" ({data_type})')

        # Add FK hints (filtered best-effort by table name substring)
        fk_text = get_foreign_keys_catalog()
        if fk_text and kept_tables:
            fk_lines = [ln for ln in fk_text.splitlines() if ln.strip()]
            filtered = ["Foreign Keys:"]
            for ln in fk_lines:
                if not ln.startswith("  FK:"):
                    continue
                # include if either side table appears in kept_tables
                if any(tbl in ln for tbl in kept_tables) or any(tbl.split(".")[-1] in ln for tbl in kept_tables):
                    filtered.append(ln)
            if len(filtered) > 1:
                lines.append("")
                lines.extend(filtered)

        return "\n".join(lines).strip() or get_schema_summary()

    # SQLite fallback
    inspector = inspect(engine)
    names = set(inspector.get_table_names())
    lines: list[str] = []
    for _sch, tbl in wanted:
        if tbl.lower() in ignore_tables:
            continue
        if tbl not in names:
            continue
        lines.append(f'Table: "{tbl}"')
        for col in inspector.get_columns(tbl):
            if col["name"].lower() in ignore_cols:
                continue
            lines.append(f'  - "{col["name"]}" ({col["type"]})')
        lines.append("")

    return "\n".join(lines).strip() or get_schema_summary()