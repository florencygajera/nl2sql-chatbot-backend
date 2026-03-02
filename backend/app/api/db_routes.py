from __future__ import annotations

import os
import subprocess
import gzip
import shutil
import uuid
import re
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, File, UploadFile, HTTPException
from pydantic import BaseModel, Field

from app.db.session import (
    set_database_url,
    set_database_source,
    reset_database_url,
    get_schema_summary,
    active_db_info,
    ping_database
)

router = APIRouter(prefix="/db", tags=["db"])

UPLOAD_DIR = Path("uploaded_db_files")
UPLOAD_DIR.mkdir(exist_ok=True, parents=True)

DbType = Literal["postgres", "mysql", "sqlite"]

AllowedUploadExt = {".sql", ".dump", ".backup", ".tar", ".gz", ".sqlite", ".db", ".bak"}


# ---------------------------
# Helpers
# ---------------------------

def build_database_url(
    connection_string: str | None,
    db_type: DbType,
    host: str | None,
    port: int | None,
    database: str | None,
    username: str | None,
    password: str | None,
    sslmode: str | None = None,
) -> str:
    if connection_string:
        return connection_string

    if db_type == "sqlite":
        if not database:
            raise ValueError("database must be a sqlite file path or name.")
        db_path = Path(database)
        if db_path.suffix not in (".sqlite", ".db"):
            db_path = UPLOAD_DIR / f"{database}.sqlite"
        abs_path = db_path.resolve().as_posix()
        return f"sqlite:///{abs_path}"

    if not all([host, port, database, username, password]):
        raise ValueError("host, port, database, username, password are required")

    if db_type == "postgres":
        url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        if sslmode:
            url += f"?sslmode={sslmode}"
        return url

    if db_type == "mysql":
        url = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        return url

    raise ValueError("Unsupported db_type")


def _safe_filename(name: str) -> str:
    name = name.replace("..", "_").replace("/", "_").replace("\\", "_")
    return name.strip() or "uploaded"


def _require_pg_tools() -> None:
    """Ensure createdb/psql/pg_restore exist when importing PG dumps."""
    import shutil as _shutil

    missing = [x for x in ("createdb", "psql", "pg_restore") if _shutil.which(x) is None]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                "PostgreSQL client tools are required to import Postgres files. "
                f"Missing: {', '.join(missing)}. "
                "Install PostgreSQL client tools and ensure they are on PATH, or upload a .sqlite/.db file instead."
            ),
        )


def _make_unique_db_name(base: str) -> str:
    base = (base or "uploaded_db").strip().lower()
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "uploaded_db"
    return f"{base}_{uuid.uuid4().hex[:8]}"


def _is_sqlite_file(path: Path) -> bool:
    # SQLite files start with: b"SQLite format 3\x00"
    head = path.read_bytes()[:16]
    return head.startswith(b"SQLite format 3\x00")


def _looks_like_sql_file(path: Path) -> bool:
    # Heuristic: if it’s text-like and contains SQL keywords near the start
    head = path.read_bytes()[:4096]
    if b"\x00" in head:
        return False
    text = head.decode("utf-8", errors="ignore").lstrip().upper()
    if text.startswith("--") or text.startswith("/*"):
        return True
    keywords = ("CREATE ", "INSERT ", "ALTER ", "DROP ", "SET ", "BEGIN ", "COMMIT ")
    return any(k in text for k in keywords)


def _is_pg_dump_archive(path: Path, env: dict, pg_host: str, pg_port: int, pg_user: str) -> bool:
    # pg_restore -l works only for pg_dump archives (custom/tar)
    p = subprocess.run(
        ["pg_restore", "-h", pg_host, "-p", str(pg_port), "-U", pg_user, "-l", str(path)],
        env=env,
        capture_output=True,
        text=True,
    )
    return p.returncode == 0


def _run_or_raise(cmd: list[str], env: dict, err_prefix: str) -> None:
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode != 0:
        detail = (p.stderr or "").strip() or (p.stdout or "").strip() or f"{err_prefix} failed"
        raise HTTPException(status_code=400, detail=detail)


# ---------------------------
# 1) Connect via form/json
# ---------------------------

class DBConnectRequest(BaseModel):
    connection_string: Optional[str] = None
    db_type: DbType = "postgres"
    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    sslmode: Optional[str] = Field(default=None, description="disable|require|verify-ca|verify-full")


@router.post("/connect")
def connect_db(payload: DBConnectRequest):
    try:
        url = build_database_url(
            connection_string=payload.connection_string,
            db_type=payload.db_type,
            host=payload.host,
            port=payload.port,
            database=payload.database,
            username=payload.username,
            password=payload.password,
            sslmode=payload.sslmode,
        )
        set_database_url(url)
        set_database_source(
            attach_mode="CONNECTION",
            db_type=payload.db_type,
            details={
                "host": payload.host,
                "port": payload.port,
                "database": payload.database,
                "username": payload.username,
                "sslmode": payload.sslmode,
                "used_connection_string": bool(payload.connection_string),
            },
        )
        return {
            "ok": True,
            "database_url": url,
            "schema": get_schema_summary(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------
# 1b) Status + detach
# ---------------------------

class ActiveDBResponse(BaseModel):
    ok: bool
    status: str
    masked_database_url: str
    source: dict
    db_schema: str | None = None


@router.get("/source", response_model=ActiveDBResponse)
def get_active_db_source(include_schema: bool = False):
    db_ok = ping_database()
    return ActiveDBResponse(
        ok=db_ok,
        status=active_db_info.get("status", "unknown"),
        masked_database_url=active_db_info.get("masked_url") or active_db_info.get("url"),
        source=active_db_info.get("source", {}),
        db_schema=get_schema_summary() if include_schema and db_ok else None,
    )


@router.post("/detach")
def detach_db_source():
    """Reset back to the default DATABASE_URL from .env/config."""
    reset_database_url()
    return {"ok": True, "masked_database_url": active_db_info.get("masked_url")}


# ---------------------------
# 2) Upload DB file
# ---------------------------

class UploadResponse(BaseModel):
    ok: bool
    database_url: str
    database_name: str
    db_schema: str


@router.post("/upload", response_model=UploadResponse)
async def upload_db_file(
    file: UploadFile = File(...),
    db_name: str = "uploaded_db",
    pg_user: str = "postgres",
    pg_password: str = "postgres",
    pg_host: str = "localhost",
    pg_port: int = 5432,
):
    """
    Upload and import:
    - .sql -> psql import into a NEW db name (unique)
    - .dump/.backup/.tar -> pg_restore into a NEW db name (unique)
    - .sqlite/.db -> direct sqlite connection
    - .gz -> handles compressed formats
    - no extension -> auto detect sqlite/sql/pg_dump archive
    """

    filename = _safe_filename(file.filename or "uploaded")
    ext = Path(filename).suffix.lower()

    # Allow "no extension" uploads (we will detect format later)
    if ext and ext not in AllowedUploadExt:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {', '.join(sorted(AllowedUploadExt))} or no extension.",
        )

    if ext == ".bak":
        raise HTTPException(
            status_code=400,
            detail="'.bak' is not a PostgreSQL dump format (often SQL Server backup). Upload .sql or pg_dump (.dump/.backup/.tar), or .sqlite/.db.",
        )

    save_path = UPLOAD_DIR / filename
    data = await file.read()
    save_path.write_bytes(data)

    # Handle gzip
    if ext == ".gz":
        uncompressed_path = save_path.with_suffix("")
        with gzip.open(save_path, "rb") as f_in:
            with open(uncompressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        save_path.unlink()
        save_path = uncompressed_path
        ext = save_path.suffix.lower()  # may become "" if original had no ext

    # ---- 1) Direct sqlite handling (by extension OR by header) ----
    if ext in [".sqlite", ".db"] or (ext == "" and _is_sqlite_file(save_path)):
        abs_path = save_path.resolve().as_posix()
        url = f"sqlite:///{abs_path}"
        try:
            set_database_url(url)
            set_database_source(
                attach_mode="UPLOAD_FILE",
                db_type="sqlite",
                details={"filename": save_path.name, "path": abs_path},
            )
            return UploadResponse(ok=True, database_url=url, database_name=save_path.name, db_schema=get_schema_summary())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    # ---- 2) Postgres import (sql or pg_dump archive) ----
    # For dynamic uploads, always restore into a fresh unique DB
    final_db_name = _make_unique_db_name(db_name)

    # If ext indicates postgres OR ext is blank (we will detect)
    if ext in [".sql", ".dump", ".backup", ".tar"] or ext == "":
        _require_pg_tools()

        env = os.environ.copy()
        env["PGPASSWORD"] = pg_password

        # Create fresh DB (do not ignore failures; show real reason)
        _run_or_raise(
            ["createdb", "-h", pg_host, "-p", str(pg_port), "-U", pg_user, final_db_name],
            env=env,
            err_prefix="createdb",
        )

        detected_sql = False
        detected_archive = False

        if ext == "":
            if _looks_like_sql_file(save_path):
                detected_sql = True
            else:
                detected_archive = _is_pg_dump_archive(save_path, env, pg_host, pg_port, pg_user)

            if not detected_sql and not detected_archive:
                raise HTTPException(
                    status_code=400,
                    detail="Unknown file format (not SQLite, not plain SQL, and not a pg_dump archive). Upload .sql or pg_dump (.dump/.backup/.tar), or .sqlite/.db.",
                )

        is_sql = (ext == ".sql") or detected_sql

        if is_sql:
            # psql import (stop on errors)
            _run_or_raise(
                [
                    "psql",
                    "-h", pg_host,
                    "-p", str(pg_port),
                    "-U", pg_user,
                    "-d", final_db_name,
                    "-v", "ON_ERROR_STOP=1",
                    "-f", str(save_path),
                ],
                env=env,
                err_prefix="psql import",
            )
        else:
            # pg_restore import (NO CLEAN for dynamic uploads)
            _run_or_raise(
                [
                    "pg_restore",
                    "-h", pg_host,
                    "-p", str(pg_port),
                    "-U", pg_user,
                    "-d", final_db_name,
                    "--no-owner",
                    "--no-privileges",
                    str(save_path),
                ],
                env=env,
                err_prefix="pg_restore",
            )

        url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{final_db_name}"
        try:
            set_database_url(url)
            set_database_source(
                attach_mode="UPLOAD_FILE",
                db_type="postgres",
                details={
                    "filename": save_path.name,
                    "db_name": final_db_name,
                    "pg_host": pg_host,
                    "pg_port": pg_port,
                    "pg_user": pg_user,
                },
            )
            return UploadResponse(ok=True, database_url=url, database_name=final_db_name, db_schema=get_schema_summary())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    raise HTTPException(status_code=400, detail="Unsupported upload format.")


# ---------------------------
# 2b) New "Database Source" alias endpoints
# ---------------------------

router.add_api_route(
    "/source/connect",
    connect_db,
    methods=["POST"],
    summary="Attach DB Source (connect)",
)

router.add_api_route(
    "/source/upload",
    upload_db_file,
    methods=["POST"],
    summary="Attach DB Source (upload file)",
)


# ---------------------------
# 3) Active DB Status
# ---------------------------
@router.get("/active")
def get_active_db():
    # Avoid leaking secrets
    return {
        "status": active_db_info.get("status"),
        "masked_url": active_db_info.get("masked_url"),
        "source": active_db_info.get("source", {}),
    }


# ---------------------------
# 4) DB Health
# ---------------------------
@router.get("/health")
def get_db_health():
    is_up = ping_database()
    return {
        "status": "up" if is_up else "down",
        "masked_url": active_db_info.get("masked_url"),
        "error": None if is_up else "Ping failed"
    }