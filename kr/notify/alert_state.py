# -*- coding: utf-8 -*-
"""
alert_state.py — Alert state persistence (PostgreSQL)
======================================================
Dedup, burst limit, state transition tracking.
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("gen4.notify.alert_state")

DEDUP_TTL = 1800      # 30분
BURST_LIMIT = 3       # 카테고리별 최대
BURST_WINDOW = 300    # 5분


def can_send(event_key: str, severity: str, category: str = "") -> bool:
    """Check if alert can be sent (dedup + burst). Returns True if allowed."""
    now = time.time()
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_sent, severity FROM dashboard_alert_state WHERE alert_key=%s",
            (event_key,),
        )
        row = cur.fetchone()

        if row:
            last_sent = row[0].timestamp() if row[0] else 0
            last_severity = row[1] or ""

            if severity == "CRITICAL" and last_severity != "CRITICAL":
                cur.close()
                return True

            if (now - last_sent) < DEDUP_TTL:
                cur.close()
                return False

        if category:
            cur.execute(
                "SELECT COUNT(*) FROM dashboard_alert_state "
                "WHERE alert_key LIKE %s AND updated_at > NOW() - INTERVAL '%s seconds'",
                (f"{category}%", BURST_WINDOW),
            )
            cnt = cur.fetchone()[0]
            if cnt >= BURST_LIMIT:
                logger.info(f"[AlertState] Burst suppressed: {category} ({cnt}/{BURST_LIMIT})")
                cur.close()
                return False

        cur.close()
    return True


def record_sent(event_key: str, severity: str, state: str = "") -> None:
    """Record that an alert was sent."""
    with connection() as conn:
        cur = conn.cursor()

        # 기존 row 조회
        cur.execute(
            "SELECT send_count, suppressed FROM dashboard_alert_state WHERE alert_key=%s",
            (event_key,),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE dashboard_alert_state SET
                    last_sent = NOW(),
                    send_count = send_count + 1,
                    severity = %s,
                    run_ts = %s,
                    updated_at = NOW()
                WHERE alert_key = %s
            """, (severity, now_utc(), event_key))
        else:
            cur.execute("""
                INSERT INTO dashboard_alert_state
                    (alert_key, last_sent, send_count, suppressed,
                     severity, run_ts, updated_at)
                VALUES (%s, NOW(), 1, 0, %s, %s, NOW())
                ON CONFLICT (alert_key) DO UPDATE SET
                    last_sent = NOW(),
                    send_count = dashboard_alert_state.send_count + 1,
                    severity = EXCLUDED.severity,
                    run_ts = EXCLUDED.run_ts,
                    updated_at = NOW()
            """, (event_key, severity, now_utc()))

        conn.commit()
        cur.close()


def get_last_state(event_key: str) -> Optional[str]:
    """Get last known state for an event (for transition detection)."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT alert_key FROM dashboard_alert_state WHERE alert_key=%s",
            (event_key,),
        )
        row = cur.fetchone()
        cur.close()
    return row[0] if row else None


def daily_rollover() -> None:
    """Reset burst counts. Run at midnight."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE dashboard_alert_state SET send_count=0, suppressed=0")
        conn.commit()
        cur.close()
    logger.info("[AlertState] Daily rollover complete")
