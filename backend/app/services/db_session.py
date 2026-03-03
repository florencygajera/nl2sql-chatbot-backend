from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class DBSession:
    db_url: str
    source: dict
    created_at: float
    last_used_at: float
    cached_schema: str = ""


# In-memory sessions (single-process). Good for local/dev.
_SESSIONS: Dict[str, DBSession] = {}


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
    return sid


def get_session(sid: str) -> Optional[DBSession]:
    s = _SESSIONS.get(sid)
    if s:
        s.last_used_at = time.time()
    return s


def delete_session(sid: str) -> None:
    _SESSIONS.pop(sid, None)


def cleanup_expired(ttl_seconds: int = 900) -> int:
    """
    Remove sessions idle for more than ttl_seconds (default 15 minutes).
    Returns number of sessions removed.
    """
    now = time.time()
    dead = [k for k, v in _SESSIONS.items() if (now - v.last_used_at) > ttl_seconds]
    for k in dead:
        _SESSIONS.pop(k, None)
    return len(dead)