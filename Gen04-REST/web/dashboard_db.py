# -*- coding: utf-8 -*-
"""
dashboard_db.py — SQLite storage for dashboard time-series data
================================================================
KOSPI, portfolio equity, PnL 등 시계열 데이터를 DB에 저장.
SSE에서 주기적으로 insert, API에서 당일 이력 조회.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger("gen4.rest.dashboard_db")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "dashboard" / "dashboard.db"


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_date TEXT NOT NULL,
            ts TEXT NOT NULL,
            epoch REAL NOT NULL,
            kospi_price REAL,
            kospi_change_pct REAL,
            kosdaq_price REAL,
            kosdaq_change_pct REAL,
            portfolio_equity REAL,
            portfolio_pnl_pct REAL,
            portfolio_cash REAL,
            holdings_count INTEGER,
            source TEXT DEFAULT 'sse'
        );

        CREATE INDEX IF NOT EXISTS idx_snap_date ON market_snapshots(market_date);
        CREATE INDEX IF NOT EXISTS idx_snap_epoch ON market_snapshots(epoch);
    """)
    conn.close()


def _conn() -> sqlite3.Connection:
    _ensure_db()
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def save_snapshot(
    kospi_price: float = 0,
    kospi_change_pct: float = 0,
    kosdaq_price: float = 0,
    kosdaq_change_pct: float = 0,
    portfolio_equity: float = 0,
    portfolio_pnl_pct: float = 0,
    portfolio_cash: float = 0,
    holdings_count: int = 0,
) -> None:
    """Insert a market snapshot. Called from SSE generator every ~60s."""
    now = datetime.now()
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO market_snapshots (
                market_date, ts, epoch,
                kospi_price, kospi_change_pct,
                kosdaq_price, kosdaq_change_pct,
                portfolio_equity, portfolio_pnl_pct,
                portfolio_cash, holdings_count, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
            time.time(),
            kospi_price, kospi_change_pct,
            kosdaq_price, kosdaq_change_pct,
            portfolio_equity, portfolio_pnl_pct,
            portfolio_cash, holdings_count,
            "sse",
        ))
        conn.commit()
    except Exception as e:
        logger.warning(f"[DashDB] save_snapshot failed: {e}")
    finally:
        conn.close()


def load_today_snapshots() -> List[Dict[str, Any]]:
    """Load all snapshots for today. For compare chart."""
    today_str = date.today().strftime("%Y-%m-%d")
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT ts, kospi_change_pct, portfolio_pnl_pct FROM market_snapshots "
            "WHERE market_date=? ORDER BY epoch ASC",
            (today_str,)
        ).fetchall()
        return [{"t": r["ts"], "kospi": r["kospi_change_pct"], "portfolio": r["portfolio_pnl_pct"]} for r in rows]
    finally:
        conn.close()


def load_snapshots_by_date(market_date: str) -> List[Dict[str, Any]]:
    """Load snapshots for a specific date."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM market_snapshots WHERE market_date=? ORDER BY epoch ASC",
            (market_date,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_snapshot_count_today() -> int:
    """Quick count for diagnostics."""
    today_str = date.today().strftime("%Y-%m-%d")
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM market_snapshots WHERE market_date=?",
            (today_str,)
        ).fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def cleanup_old_snapshots(keep_days: int = 30) -> int:
    """Delete snapshots older than keep_days. Run daily."""
    from datetime import timedelta
    cutoff = (date.today() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _conn()
    try:
        cur = conn.execute("DELETE FROM market_snapshots WHERE market_date < ?", (cutoff,))
        conn.commit()
        deleted = cur.rowcount
        if deleted > 0:
            logger.info(f"[DashDB] Cleaned {deleted} old snapshots (before {cutoff})")
        return deleted
    finally:
        conn.close()
