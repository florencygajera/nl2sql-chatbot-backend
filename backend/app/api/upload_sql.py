from __future__ import annotations

from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException

router = APIRouter(tags=["sql-upload"])

UPLOAD_DIR = Path("data/sql_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_BYTES = 5 * 1024 * 1024  # 5MB


@router.post("/upload-sql")
async def upload_sql(file: UploadFile = File(...)):
    name = (file.filename or "").strip()
    if not name.lower().endswith(".sql"):
        raise HTTPException(status_code=400, detail="Only .sql files are allowed.")

    data = await file.read()
    if len(data) > MAX_BYTES:
        raise HTTPException(status_code=413, detail="File too large (max 5MB).")

    # very basic filename sanitization
    safe_name = "".join(c for c in name if c.isalnum() or c in ("-", "_", ".", " "))
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename.")

    path = UPLOAD_DIR / safe_name
    path.write_bytes(data)

    return {"status": "ok", "filename": safe_name, "bytes": len(data)}