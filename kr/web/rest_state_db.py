# -*- coding: utf-8 -*-
"""
rest_state_db.py — Phase 1 REST State Database (Observer)
==========================================================
Gen4 state 파일을 읽고 REST_DB에 미러링.
REST_DB = observer only. Gen4 파일에 쓰기 절대 금지.
broker = truth, Gen4 state = reference, REST_DB = observer + calculator.

참조: docs/BASELINE_VALUES_SPEC.md, docs/STATE_TRANSITION_SPEC.md
"""
from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.rest.state_db")

DB_DIR = Path(__file__).resolve().parent.parent / "data" / "rest_state"
DB_PATH = DB_DIR / "rest_state.db"

_SCHEMA = """
-- 포지션 추적 (기준값 정의서 #1~#6, #9~#10)
CREATE TABLE IF NOT EXISTS rest_positions (
    code TEXT NOT NULL,
    name TEXT,
    qty INTEGER NOT NULL,
    avg_price REAL NOT NULL,
    entry_date TEXT,
    current_price REAL DEFAULT 0,
    high_watermark REAL DEFAULT 0,
    trail_stop_price REAL DEFAULT 0,
    invested_total REAL DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    snapshot_id TEXT NOT NULL,
    asof_ts TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (code, snapshot_id)
);
CREATE INDEX IF NOT EXISTS idx_pos_snapshot ON rest_positions(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_pos_active ON rest_positions(is_active);

-- Equity 스냅샷 (기준값 정의서 #11~#14)
CREATE TABLE IF NOT EXISTS rest_equity_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_date TEXT NOT NULL,
    asof_ts TEXT NOT NULL,
    is_eod INTEGER DEFAULT 0,
    close_equity REAL NOT NULL,
    prev_close_equity REAL DEFAULT 0,
    peak_equity REAL DEFAULT 0,
    cash REAL DEFAULT 0,
    holdings_count INTEGER DEFAULT 0,
    rebalance_cycle_id INTEGER DEFAULT 0,
    snapshot_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_eq_date ON rest_equity_snapshots(market_date);
CREATE INDEX IF NOT EXISTS idx_eq_snapshot ON rest_equity_snapshots(snapshot_id);

-- 교차검증 로그
CREATE TABLE IF NOT EXISTS rest_validation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_time TEXT NOT NULL,
    check_type TEXT NOT NULL,
    snapshot_id TEXT,
    gen4_value TEXT,
    rest_value TEXT,
    broker_value TEXT,
    diff_pct REAL,
    status TEXT NOT NULL,
    detail TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_val_time ON rest_validation_log(check_time);
CREATE INDEX IF NOT EXISTS idx_val_type ON rest_validation_log(check_type);
"""


def _ensure_db() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.close()


def _conn() -> sqlite3.Connection:
    _ensure_db()
    c = sqlite3.connect(str(DB_PATH), timeout=5)
    c.row_factory = sqlite3.Row
    return c


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def make_snapshot_id() -> str:
    """현재 시점 기반 snapshot ID 생성. 동일 SSE cycle 내 공유."""
    return datetime.now().strftime("%Y%m%d-%H%M%S") + f"-{int(time.time()*1000)%10000:04d}"


# ── Positions ─────────────────────────────────────────────────

def sync_positions_from_gen4(
    gen4_positions: dict,
    snapshot_id: str,
    asof_ts: Optional[str] = None,
) -> int:
    """
    Gen4 state에서 읽은 포지션을 REST_DB에 미러링.
    Full refresh: 이번 snapshot에 없는 기존 active row는 inactive 처리.
    Returns: 동기화된 포지션 수.
    """
    if not gen4_positions:
        return 0

    now = _now_iso()
    asof = asof_ts or now
    synced = 0

    conn = _conn()
    try:
        cur = conn.cursor()

        # 이번 snapshot에 포함된 codes
        current_codes = set()

        for code, pos in gen4_positions.items():
            if not code or not isinstance(pos, dict):
                continue
            current_codes.add(code)
            cur.execute("""
                INSERT INTO rest_positions
                    (code, name, qty, avg_price, entry_date, current_price,
                     high_watermark, trail_stop_price, invested_total,
                     is_active, snapshot_id, asof_ts, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """, (
                code,
                pos.get("name", pos.get("code", code)),
                int(pos.get("quantity", 0)),
                float(pos.get("avg_price", 0)),
                pos.get("entry_date", ""),
                float(pos.get("current_price", 0)),
                float(pos.get("high_watermark", 0)),
                float(pos.get("trail_stop_price", 0)),
                float(pos.get("invested_total", 0)),
                snapshot_id,
                asof,
                now,
            ))
            synced += 1

        conn.commit()
        logger.info(f"[REST_DB] sync_positions: {synced} codes, snapshot={snapshot_id}")
    except Exception as e:
        logger.warning(f"[REST_DB] sync_positions failed: {e}")
        conn.rollback()
    finally:
        conn.close()

    return synced


def get_latest_positions() -> Dict[str, dict]:
    """최신 snapshot의 active 포지션 조회."""
    conn = _conn()
    try:
        cur = conn.execute("""
            SELECT * FROM rest_positions
            WHERE snapshot_id = (
                SELECT snapshot_id FROM rest_positions
                ORDER BY asof_ts DESC LIMIT 1
            )
        """)
        result = {}
        for row in cur.fetchall():
            result[row["code"]] = dict(row)
        return result
    finally:
        conn.close()


# ── Equity Snapshots ──────────────────────────────────────────

def sync_equity_snapshot(
    market_date: str,
    equity: float,
    cash: float = 0,
    holdings_count: int = 0,
    peak_equity: float = 0,
    prev_close_equity: float = 0,
    rebalance_cycle_id: int = 0,
    is_eod: bool = False,
    snapshot_id: str = "",
) -> None:
    """Equity 스냅샷 저장. is_eod=True면 EOD 확정값."""
    now = _now_iso()
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO rest_equity_snapshots
                (market_date, asof_ts, is_eod, close_equity,
                 prev_close_equity, peak_equity, cash, holdings_count,
                 rebalance_cycle_id, snapshot_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            market_date, now, int(is_eod), equity,
            prev_close_equity, peak_equity, cash, holdings_count,
            rebalance_cycle_id, snapshot_id or make_snapshot_id(), now,
        ))
        conn.commit()
    except Exception as e:
        logger.warning(f"[REST_DB] sync_equity failed: {e}")
    finally:
        conn.close()


def get_latest_equity(market_date: Optional[str] = None) -> Optional[dict]:
    """최근 equity 스냅샷 조회."""
    conn = _conn()
    try:
        if market_date:
            cur = conn.execute("""
                SELECT * FROM rest_equity_snapshots
                WHERE market_date = ? ORDER BY asof_ts DESC LIMIT 1
            """, (market_date,))
        else:
            cur = conn.execute("""
                SELECT * FROM rest_equity_snapshots
                ORDER BY asof_ts DESC LIMIT 1
            """)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_eod_equity(market_date: str) -> Optional[dict]:
    """특정 날짜의 EOD 확정 equity 조회."""
    conn = _conn()
    try:
        cur = conn.execute("""
            SELECT * FROM rest_equity_snapshots
            WHERE market_date = ? AND is_eod = 1
            ORDER BY asof_ts DESC LIMIT 1
        """, (market_date,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ── Validation Log ────────────────────────────────────────────

def log_validation(
    check_type: str,
    gen4_value: Any = None,
    rest_value: Any = None,
    broker_value: Any = None,
    diff_pct: float = 0,
    status: str = "MATCH",
    detail: str = "",
    snapshot_id: str = "",
) -> None:
    """교차검증 결과 기록."""
    now = _now_iso()
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO rest_validation_log
                (check_time, check_type, snapshot_id,
                 gen4_value, rest_value, broker_value,
                 diff_pct, status, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now, check_type, snapshot_id,
            str(gen4_value) if gen4_value is not None else None,
            str(rest_value) if rest_value is not None else None,
            str(broker_value) if broker_value is not None else None,
            diff_pct, status, detail, now,
        ))
        conn.commit()
    except Exception as e:
        logger.debug(f"[REST_DB] log_validation failed: {e}")
    finally:
        conn.close()


def get_recent_validations(limit: int = 50) -> List[dict]:
    """최근 교차검증 결과 조회."""
    conn = _conn()
    try:
        cur = conn.execute("""
            SELECT * FROM rest_validation_log
            ORDER BY check_time DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def cleanup_old_data(keep_days: int = 30) -> int:
    """오래된 데이터 정리."""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _conn()
    try:
        deleted = 0
        deleted += conn.execute(
            "DELETE FROM rest_equity_snapshots WHERE market_date < ?", (cutoff,)
        ).rowcount
        deleted += conn.execute(
            "DELETE FROM rest_validation_log WHERE check_time < ?", (cutoff,)
        ).rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()
