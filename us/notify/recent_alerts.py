# -*- coding: utf-8 -*-
"""recent_alerts.py — in-memory ring buffer of Telegram sends (US market).

Operator visibility for the last 24h of Telegram alerts, surfaced in the
US dashboard's diagnostics panel. Process-local — restarting the web/main
process clears history. No DB, no migration, no persistence.

Mirror of kr/notify/recent_alerts.py (MARKET="KR") with MARKET="US".
"""
from __future__ import annotations

import logging
import re
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, List

logger = logging.getLogger("qtron.us.recent_alerts")

MARKET = "US"
MAX_BUFFER = 300

_buffer: Deque[Dict] = deque(maxlen=MAX_BUFFER)
_lock = threading.Lock()


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "")


def _first_line(text: str, limit: int = 80) -> str:
    plain = _strip_html(text).strip()
    head = plain.splitlines()[0] if plain else ""
    return head[:limit]


def record(severity: str, text: str, status: str = "sent") -> None:
    """Append one alert to the ring buffer. Never raises."""
    try:
        sev = (severity or "INFO").upper()
        if sev not in ("INFO", "WARN", "ERROR", "CRITICAL"):
            sev = "INFO"
        level = "ERROR" if sev == "CRITICAL" else sev
        item = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "market": MARKET,
            "level": level,
            "title": _first_line(text),
            "message": _strip_html(text),
            "send_status": "sent" if status == "sent" else "failed",
        }
        with _lock:
            _buffer.append(item)
    except Exception as e:
        logger.warning(f"[recent_alerts] record failed: {e}")


def list_recent(window_seconds: int = 86400) -> List[Dict]:
    """Return entries within the last `window_seconds`, newest first."""
    cutoff_ts = datetime.now(timezone.utc).timestamp() - window_seconds
    with _lock:
        while _buffer:
            head = _buffer[0]
            try:
                head_ts = datetime.fromisoformat(head["ts"]).timestamp()
            except Exception:
                head_ts = 0
            if head_ts < cutoff_ts:
                _buffer.popleft()
            else:
                break
        snapshot = list(_buffer)
    snapshot.reverse()
    return snapshot


def clear() -> None:
    with _lock:
        _buffer.clear()
