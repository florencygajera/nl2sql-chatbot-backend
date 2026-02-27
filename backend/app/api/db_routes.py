from __future__ import annotations

import os
import subprocess
import gzip
import shutil
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, File, UploadFile, HTTPException
from pydantic import BaseModel, Field

from app.db.session import (
    set_database_url,
    get_schema_summary,
    active_db_info,
    ping_database
)

router = APIRouter(prefix="/db", tags=["db"])

UPLOAD_DIR = Path("uploaded_db_files")
UPLOAD_DIR.mkdir(exist_ok=True, parents=True)

DbType = Literal["postgres", "mysql", "sqlite"]

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
        return {
            "ok": True,
            "database_url": url,
            "schema": get_schema_summary(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ---------------------------
# 2) Upload DB file
# ---------------------------

class UploadResponse(BaseModel):
    ok: bool
    database_url: str
    database_name: str
    schema: str


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
    - .sql -> psql import into db_name
    - .dump/.backup/.tar -> pg_restore into db_name
    - .sqlite/.db -> direct sqlite connection
    - .gz -> handles compressed formats
    """

    filename = file.filename or "uploaded"
    ext = Path(filename).suffix.lower()
    save_path = UPLOAD_DIR / filename

    data = await file.read()
    save_path.write_bytes(data)

    if ext == ".gz":
        uncompressed_path = save_path.with_suffix("")
        with gzip.open(save_path, 'rb') as f_in:
            with open(uncompressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        save_path.unlink()
        save_path = uncompressed_path
        ext = save_path.suffix.lower()

    if ext in [".sqlite", ".db"]:
        abs_path = save_path.resolve().as_posix()
        url = f"sqlite:///{abs_path}"
        try:
            set_database_url(url)
            return UploadResponse(ok=True, database_url=url, database_name=save_path.name, schema=get_schema_summary())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    if ext in [".sql", ".dump", ".backup", ".tar"]:
        env = os.environ.copy()
        env["PGPASSWORD"] = pg_password

        # try to create DB (ignore if exists)
        subprocess.run(
            ["createdb", "-h", pg_host, "-p", str(pg_port), "-U", pg_user, db_name],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )

        if ext == ".sql":
            p = subprocess.run(
                ["psql", "-h", pg_host, "-p", str(pg_port), "-U", pg_user, "-d", db_name, "-f", str(save_path)],
                env=env,
                capture_output=True,
                text=True,
                shell=True,
            )
            if p.returncode != 0:
                raise HTTPException(status_code=400, detail=p.stderr)
        else:
            p = subprocess.run(
                ["pg_restore", "-h", pg_host, "-p", str(pg_port), "-U", pg_user, "-d", db_name, "-c", str(save_path)],
                env=env,
                capture_output=True,
                text=True,
                shell=True,
            )
            if p.returncode != 0:
                raise HTTPException(status_code=400, detail=p.stderr)

        url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{db_name}"
        try:
            set_database_url(url)
            return UploadResponse(ok=True, database_url=url, database_name=db_name, schema=get_schema_summary())
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")


# ---------------------------
# 3) Active DB Status
# ---------------------------
@router.get("/active")
def get_active_db():
    return active_db_info


# ---------------------------
# 4) DB Health
# ---------------------------
@router.get("/health")
def get_db_health():
    is_up = ping_database()
    return {
        "status": "up" if is_up else "down",
        "url": active_db_info.get("url"),
        "error": None if is_up else "Ping failed"
    }
