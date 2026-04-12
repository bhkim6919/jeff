# -*- coding: utf-8 -*-
"""
alert_state.py — Alert state persistence (SQLite)
===================================================
Dedup, burst limit, state transition tracking.
Uses dashboard.db → alert_state table.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("gen4.notify.alert_state")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "dashboard" / "dashboard.db"

DEDUP_TTL = 1800      # 30분
BURST_LIMIT = 3       # 카테고리별 최대
BURST_WINDOW = 300    # 5분


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""
        CREATE TABLE IF NOT EXISTS alert_state (
            event_key TEXT PRIMARY KEY,
            last_sent_at REAL DEFAULT 0,
            last_state TEXT DEFAULT '',
            severity TEXT DEFAULT '',
            burst_count INTEGER DEFAULT 0,
            burst_window_start REAL DEFAULT 0,
            updated_at TEXT
        )
    """)
    c.commit()
    return c


def can_send(event_key: str, severity: str, category: str = "") -> bool:
    """
    Check if alert can be sent (dedup + burst).
    Returns True if allowed.
    """
    now = time.time()
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM alert_state WHERE event_key=?", (event_key,)
        ).fetchone()

        if row:
            last_sent = row["last_sent_at"] or 0
            last_severity = row["severity"] or ""

            # WARN→CRITICAL escalation bypasses dedup
            if severity == "CRITICAL" and last_severity != "CRITICAL":
                return True

            # Dedup: same event within TTL
            if (now - last_sent) < DEDUP_TTL:
                return False

        # Burst limit: category-based
        if category:
            cat_rows = conn.execute(
                "SELECT COUNT(*) FROM alert_state WHERE event_key LIKE ? AND burst_window_start > ?",
                (f"{category}%", now - BURST_WINDOW)
            ).fetchone()
            if cat_rows and cat_rows[0] >= BURST_LIMIT:
                logger.info(f"[AlertState] Burst suppressed: {category} ({cat_rows[0]}/{BURST_LIMIT})")
                return False

        return True
    finally:
        conn.close()


def record_sent(event_key: str, severity: str, state: str = "") -> None:
    """Record that an alert was sent."""
    now = time.time()
    conn = _conn()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO alert_state
            (event_key, last_sent_at, last_state, severity, burst_count, burst_window_start, updated_at)
            VALUES (?, ?, ?, ?,
                    COALESCE((SELECT CASE
                        WHEN burst_window_start > ? THEN burst_count + 1
                        ELSE 1 END FROM alert_state WHERE event_key=?), 1),
                    CASE WHEN (SELECT burst_window_start FROM alert_state WHERE event_key=?) > ?
                        THEN (SELECT burst_window_start FROM alert_state WHERE event_key=?)
                        ELSE ? END,
                    ?)
        """, (event_key, now, state, severity,
              now - BURST_WINDOW, event_key,
              event_key, now - BURST_WINDOW, event_key,
              now,
              time.strftime("%Y-%m-%dT%H:%M:%S")))
        conn.commit()
    finally:
        conn.close()


def get_last_state(event_key: str) -> Optional[str]:
    """Get last known state for an event (for transition detection)."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT last_state FROM alert_state WHERE event_key=?", (event_key,)
        ).fetchone()
        return row["last_state"] if row else None
    finally:
        conn.close()


def daily_rollover() -> None:
    """Reset burst counts. Run at midnight."""
    conn = _conn()
    try:
        conn.execute("UPDATE alert_state SET burst_count=0, burst_window_start=0")
        conn.commit()
        logger.info("[AlertState] Daily rollover complete")
    finally:
        conn.close()
