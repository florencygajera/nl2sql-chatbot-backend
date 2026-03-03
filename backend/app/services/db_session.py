from __future__ import annotations

import json
import logging
import secrets
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Persist sessions to this file so they survive server restarts
_SESSION_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "db_sessions.json"


@dataclass
class DBSession:
    db_url: str
    source: dict
    created_at: float
    last_used_at: float
    cached_schema: str = ""


# In-memory sessions, synced to disk
_SESSIONS: Dict[str, DBSession] = {}


def _load_from_disk() -> None:
    """Load sessions from disk on startup."""
    global _SESSIONS
    try:
        if _SESSION_FILE.exists():
            raw = json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
            for sid, data in raw.items():
                _SESSIONS[sid] = DBSession(**data)
            logger.info("Loaded %d DB sessions from disk", len(_SESSIONS))
    except Exception as e:
        logger.warning("Could not load sessions from disk: %s", e)


def _save_to_disk() -> None:
    """Persist sessions to disk."""
    try:
        _SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        raw = {sid: asdict(s) for sid, s in _SESSIONS.items()}
        _SESSION_FILE.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        logger.warning("Could not save sessions to disk: %s", e)


# Load on import
_load_from_disk()


def create_session(db_url: str, source: dict, cached_schema: str = "") -> str:
    sid = secrets.token_urlsafe(24)
    now = time.time()
    _SESSIONS[sid] = DBSession(
        db_url=db_url,
        source=source or {},
        created_at=now,
        last_used_at=now,
        cached_schema=cached_schema,
    )
    _save_to_disk()
    return sid


def get_session(sid: str) -> Optional[DBSession]:
    s = _SESSIONS.get(sid)
    if s:
        s.last_used_at = time.time()
    return s


def delete_session(sid: str) -> None:
    _SESSIONS.pop(sid, None)
    _save_to_disk()


def cleanup_expired(ttl_seconds: int = 900) -> int:
    """
    Remove sessions idle for more than ttl_seconds (default 15 minutes).
    Returns number of sessions removed.
    """
    now = time.time()
    dead = [k for k, v in _SESSIONS.items() if (now - v.last_used_at) > ttl_seconds]
    for k in dead:
        _SESSIONS.pop(k, None)
    if dead:
        _save_to_disk()
    return len(dead)