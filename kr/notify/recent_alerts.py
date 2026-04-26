# -*- coding: utf-8 -*-
"""recent_alerts.py — JSONL-backed shared alert log (KR market).

Replaces the earlier in-memory deque so ALL processes that send Telegram
messages (web, main, batch, backup, watchdog, ...) deposit into a single
file the dashboard can read. Each market has its own file:

    kr/data/notify/recent_alerts.jsonl   (this module)
    us/data/notify/recent_alerts.jsonl   (us/notify/recent_alerts.py)

File format: one JSON object per line, fields:
    ts            ISO-8601 UTC timestamp
    market        "KR"
    level         INFO | WARN | ERROR
    title         first non-blank line of message, HTML stripped, ≤80 chars
    message       full message body, HTML stripped
    send_status   "sent" | "failed"
    source        "web" | "main" | "batch" | "backup" | "watchdog" |
                  "tray" | "unknown"

Public API matches the previous in-memory version (record / list_recent /
clear) so callers — primarily kr/notify/telegram_bot.send() — don't change.

Concurrency model:
  * record() append uses Python's "a" mode which on Windows opens the
    file with FILE_APPEND_DATA, making single-write appends atomic at the
    OS level. No explicit lock required for sparse alert traffic.
  * list_recent() reads the whole file, filters to the time window, and
    when prune is needed atomically replaces the file via os.replace().
    A msvcrt lock guards against a second reader pruning concurrently.
    A writer racing with a prune is a known small loss window — Jeff
    accepted this tradeoff (run-time alerts, not durable accounting).

The module is import-safe even when notify/ is on sys.path with no
data directory yet — the parent dir is created lazily on first record().
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("gen4.notify.recent_alerts")

MARKET = "KR"
WINDOW_SECONDS = 86400
MAX_ROWS = 2000  # safety cap; prune trims to this many rows max

_ALERTS_PATH = Path(__file__).resolve().parent.parent / "data" / "notify" / "recent_alerts.jsonl"
_thread_lock = threading.Lock()
_SOURCE_CACHE: Optional[str] = None


def _detect_source() -> str:
    """Best-effort source label from sys.argv (cached — argv is immutable)."""
    global _SOURCE_CACHE
    if _SOURCE_CACHE is not None:
        return _SOURCE_CACHE
    try:
        argv = " ".join(sys.argv).lower()
        if "uvicorn" in argv or "web.app" in argv or "web/app" in argv:
            _SOURCE_CACHE = "web"
        elif "tray_server" in argv:
            _SOURCE_CACHE = "tray"
        elif "main.py" in argv:
            if "--batch" in argv:
                _SOURCE_CACHE = "batch"
            else:
                _SOURCE_CACHE = "main"
        elif "backup" in argv:
            _SOURCE_CACHE = "backup"
        elif "watchdog" in argv:
            _SOURCE_CACHE = "watchdog"
        else:
            _SOURCE_CACHE = "unknown"
    except Exception:
        _SOURCE_CACHE = "unknown"
    return _SOURCE_CACHE


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _first_line(text: str, limit: int = 80) -> str:
    plain = _strip_html(text).strip()
    head = plain.splitlines()[0] if plain else ""
    return head[:limit]


def _ensure_dir() -> None:
    _ALERTS_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def _file_lock():
    """Soft cross-process lock around a sentinel .lock file.

    On Windows uses msvcrt.locking; on POSIX falls back to fcntl.flock.
    Acquisition failures are non-fatal — callers proceed unlocked,
    accepting a small race window. Sentinel approach avoids holding
    a lock on the data file itself, which would conflict with append.
    """
    lock_path = str(_ALERTS_PATH) + ".lock"
    lf = None
    locked = False
    try:
        try:
            _ensure_dir()
            lf = open(lock_path, "w")
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lf.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            locked = True
        except Exception as e:
            logger.debug(f"[recent_alerts] file lock acquire skipped: {e}")
        yield
    finally:
        if lf is not None:
            if locked:
                try:
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(lf.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
                except Exception:
                    pass
            try:
                lf.close()
            except Exception:
                pass


def record(severity: str, text: str, status: str = "sent",
           source: Optional[str] = None) -> None:
    """Append one alert row to the JSONL file. Never raises.

    `source` may be passed explicitly by a caller that knows its context
    (e.g., a backup script tagging "backup"); otherwise auto-detected.
    """
    try:
        sev = (severity or "INFO").upper()
        if sev not in ("INFO", "WARN", "ERROR", "CRITICAL"):
            sev = "INFO"
        # CRITICAL is a Telegram-bot severity — the UI level taxonomy
        # is INFO/WARN/ERROR, so collapse CRITICAL → ERROR for display.
        level = "ERROR" if sev == "CRITICAL" else sev
        item = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "market": MARKET,
            "level": level,
            "title": _first_line(text),
            "message": _strip_html(text),
            "send_status": "sent" if status == "sent" else "failed",
            "source": source or _detect_source(),
        }
        line = json.dumps(item, ensure_ascii=False) + "\n"
        # Append is atomic at the OS level for line-sized writes; no
        # cross-process lock needed here. Thread lock guards against
        # the same process racing with itself.
        with _thread_lock:
            _ensure_dir()
            with open(_ALERTS_PATH, "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as e:
        logger.warning(f"[recent_alerts] record failed: {e}")


def list_recent(window_seconds: int = WINDOW_SECONDS) -> List[Dict]:
    """Return entries within the last `window_seconds`, newest first.

    Read-then-prune semantics: rows older than the window are removed
    via atomic replace. Broken JSON lines are silently skipped (counted
    and logged once). Returns [] on any unexpected failure to keep the
    UI working.
    """
    cutoff_ts = datetime.now(timezone.utc).timestamp() - window_seconds
    kept: List[Dict] = []
    try:
        with _thread_lock, _file_lock():
            if not _ALERTS_PATH.exists():
                return []
            broken_count = 0
            try:
                with open(_ALERTS_PATH, "r", encoding="utf-8") as f:
                    for raw in f:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            broken_count += 1
                            continue
                        try:
                            obj_ts = datetime.fromisoformat(obj.get("ts", "")).timestamp()
                        except Exception:
                            broken_count += 1
                            continue
                        if obj_ts >= cutoff_ts:
                            kept.append(obj)
                if broken_count:
                    logger.warning(
                        f"[recent_alerts] skipped {broken_count} broken JSONL line(s)"
                    )
            except Exception as e:
                logger.warning(f"[recent_alerts] read failed: {e}")
                return []

            # Cap at MAX_ROWS so a runaway sender can't unbound the file.
            if len(kept) > MAX_ROWS:
                kept = kept[-MAX_ROWS:]

            # Atomic replace prune — only rewrite if pruning actually
            # changed anything (cheap comparison via line count).
            try:
                fd, tmp_path = tempfile.mkstemp(
                    prefix=".recent_alerts_", suffix=".tmp",
                    dir=str(_ALERTS_PATH.parent),
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as tf:
                        for row in kept:
                            tf.write(json.dumps(row, ensure_ascii=False) + "\n")
                    os.replace(tmp_path, _ALERTS_PATH)
                except Exception:
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
                    raise
            except Exception as e:
                logger.warning(f"[recent_alerts] prune failed: {e}")
    except Exception as e:
        logger.warning(f"[recent_alerts] list_recent failed: {e}")
        return []

    # Newest first
    return list(reversed(kept))


def clear() -> None:
    """Test helper. Not used in production."""
    with _thread_lock:
        try:
            if _ALERTS_PATH.exists():
                _ALERTS_PATH.unlink()
        except Exception:
            pass
