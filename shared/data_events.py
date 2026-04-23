# -*- coding: utf-8 -*-
"""shared/data_events.py — Minimal event + escalation tracker.

Phase B Step 2 (Jeff-approved minimal recovery).

Public API (fixed — 4 call sites depend on these exact names and semantics):

    Level                                — IntEnum, 5 levels
    emit_event(source, level, code,      — records one event, auto-manages
               message, details=None,      escalation state for (source, code)
               telegram=False)
    get_events(limit=50, min_level=None, — circular buffer read, newest first
               sources=None)
    get_escalation_states()              — snapshot of currently-open
                                           escalations as list[dict]

Observed call sites (ctrl-F grep results, none may regress):

    emit_event:
        us/data/alpaca_provider.py:190,220        level=Level.CRITICAL / Level.INFO
        kr/tools/health_check.py:119              level=str "CRITICAL"/"WARN"
        kr/tray_server.py:1054 (via web.*)        level=Level.CRITICAL
    get_events + get_escalation_states:
        us/web/app.py:416                         /api/debug/data_events
        kr/web/app.py:2806 (via web.*)            /api/debug/data_events

Escalation semantics (inferred from alpaca_provider.py auth-recovery pattern):
    - First WARN/ERROR/CRITICAL emit for a (source, code) → opens escalation
    - Same/higher level re-emit → updates last_seen_at + last_message
    - Lower level (INFO/DEBUG) emit for same (source, code) → CLEARS escalation

Storage:
    - In-memory circular buffer (capped at 10000 events)
    - Thread-safe via one module-level lock
    - NO JSONL persist, NO DB, NO async, NO queue — Jeff Step-2 contract
      ("기능 확장 금지 / 새 기능 넣지 말 것")

Test hygiene:
    `_reset_for_tests()` clears state. Production code must never call it.
"""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Iterable, Optional


class Level(IntEnum):
    """Severity levels, ordered. Higher == worse."""
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    CRITICAL = 4


_LEVEL_FROM_STR: dict[str, Level] = {
    "DEBUG": Level.DEBUG,
    "INFO": Level.INFO,
    "WARN": Level.WARN,
    "WARNING": Level.WARN,      # alias for friendlier input
    "ERROR": Level.ERROR,
    "CRITICAL": Level.CRITICAL,
    "FATAL": Level.CRITICAL,    # alias
}


def _normalize_level(lv: Any) -> Level:
    """Accept Level / int / str (case-insensitive). Unknown → INFO (safe default)."""
    if isinstance(lv, Level):
        return lv
    if isinstance(lv, int) and not isinstance(lv, bool):
        try:
            return Level(lv)
        except ValueError:
            return Level.INFO
    if isinstance(lv, str):
        return _LEVEL_FROM_STR.get(lv.strip().upper(), Level.INFO)
    return Level.INFO


# ---------- module state ----------

_EVENT_BUFFER_MAX = 10000
_events: deque = deque(maxlen=_EVENT_BUFFER_MAX)
_escalations: dict[tuple[str, str], dict] = {}
_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------- public API ----------

def emit_event(
    source: str,
    level: Any,
    code: str,
    message: str,
    details: Optional[dict] = None,
    telegram: bool = False,
) -> dict:
    """Record one event. Returns the stored event dict.

    Does NOT send Telegram here — the `telegram` flag is stored on the
    event so a separate consumer (notifier/tray/dashboard) can decide
    whether to forward. Decoupling keeps this module dependency-free.
    """
    lv = _normalize_level(level)
    ts = _now_iso()
    src = str(source)
    cd = str(code)

    event: dict = {
        "ts": ts,
        "source": src,
        "level": lv.name,
        "code": cd,
        "message": str(message),
        "details": dict(details) if details else {},
        "telegram": bool(telegram),
    }

    with _lock:
        _events.append(event)
        key = (src, cd)
        if lv >= Level.WARN:
            prev = _escalations.get(key)
            if prev is None:
                _escalations[key] = {
                    "source": src,
                    "code": cd,
                    "level": lv.name,
                    "level_rank": int(lv),
                    "opened_at": ts,
                    "last_seen_at": ts,
                    "last_message": str(message),
                }
            else:
                # Same or higher severity — update; lower severity retained as peak
                if int(lv) > prev.get("level_rank", 0):
                    prev["level"] = lv.name
                    prev["level_rank"] = int(lv)
                prev["last_seen_at"] = ts
                prev["last_message"] = str(message)
        else:
            # INFO/DEBUG clears a prior escalation for the same (source, code)
            _escalations.pop(key, None)

    return event


def get_events(
    limit: int = 50,
    min_level: Optional[str] = None,
    sources: Optional[Iterable[str]] = None,
) -> list[dict]:
    """Return up to `limit` recent events, newest first.

    - `min_level` as string (DEBUG/INFO/WARN/ERROR/CRITICAL). None = all.
    - `sources` is an iterable of substrings; an event matches if ANY
      substring is contained in its `source` field. None = no filter.
    """
    min_rank: int = int(_normalize_level(min_level)) if min_level else Level.DEBUG
    src_list = [s for s in (sources or []) if s]

    with _lock:
        snapshot = list(_events)

    out: list[dict] = []
    for ev in reversed(snapshot):
        if int(_normalize_level(ev["level"])) < min_rank:
            continue
        if src_list and not any(s in ev["source"] for s in src_list):
            continue
        out.append(ev)
        if len(out) >= max(0, int(limit)):
            break
    return out


def get_escalation_states() -> list[dict]:
    """Snapshot of currently-open escalations, newest opened first."""
    with _lock:
        items = list(_escalations.values())
    return sorted(items, key=lambda d: d.get("opened_at", ""), reverse=True)


# ---------- test hygiene ----------

def _reset_for_tests() -> None:
    """Clear buffer + escalations. Tests only — production must not call."""
    with _lock:
        _events.clear()
        _escalations.clear()
