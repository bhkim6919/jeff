# -*- coding: utf-8 -*-
"""
dashboard_db.py — PostgreSQL storage for dashboard time-series data
====================================================================
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("gen4.rest.dashboard_db")


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
    epoch = time.time()
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO dashboard_snapshots (
                    market_date, epoch, ts,
                    kospi_price, kospi_change_pct,
                    kosdaq_price, kosdaq_change_pct,
                    portfolio_equity, portfolio_pnl_pct,
                    portfolio_cash, holdings_count, source, run_ts
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_date, epoch) DO NOTHING
            """, (
                now.strftime("%Y-%m-%d"),
                epoch,
                now.strftime("%H:%M:%S"),
                kospi_price, kospi_change_pct,
                kosdaq_price, kosdaq_change_pct,
                portfolio_equity, portfolio_pnl_pct,
                portfolio_cash, holdings_count,
                "sse", now_utc(),
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.warning(f"[DashDB] save_snapshot failed: {e}")


def load_today_snapshots() -> List[Dict[str, Any]]:
    """Load all snapshots for today. For compare chart."""
    today_str = date.today().strftime("%Y-%m-%d")
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT ts, kospi_change_pct, portfolio_pnl_pct "
            "FROM dashboard_snapshots "
            "WHERE market_date=%s ORDER BY epoch ASC",
            (today_str,),
        )
        rows = cur.fetchall()
        cur.close()
    return [{"t": r[0], "kospi": r[1], "portfolio": r[2]} for r in rows]


def load_snapshots_by_date(market_date: str) -> List[Dict[str, Any]]:
    """Load snapshots for a specific date."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM dashboard_snapshots "
            "WHERE market_date=%s ORDER BY epoch ASC",
            (market_date,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


def get_prev_day_last_equity(today: str) -> Dict[str, Any]:
    """today 이전 가장 최근 market_date의 last equity snapshot.

    Returns: {"market_date": str, "equity": float, "ts": str} or {} if none.
    """
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT market_date, ts, portfolio_equity "
            "FROM dashboard_snapshots "
            "WHERE market_date<%s AND portfolio_equity>0 "
            "ORDER BY market_date DESC, epoch DESC LIMIT 1",
            (today,),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return {}
    return {"market_date": row[0], "ts": row[1], "equity": float(row[2])}


def get_alltime_equity_peak(exclude_today: str = "") -> Dict[str, Any]:
    """전체 이력의 MAX portfolio_equity + 날짜. monthly_dd 계산용.

    exclude_today 지정 시 해당 날짜 제외 (오늘 tick이 peak을 갱신하는 경우 따로 판단).
    Returns: {"peak": float, "peak_date": str, "count": int}
    """
    result = {"peak": 0.0, "peak_date": "", "count": 0}
    with connection() as conn:
        cur = conn.cursor()
        if exclude_today:
            cur.execute(
                "SELECT COUNT(*), MAX(portfolio_equity) FROM dashboard_snapshots "
                "WHERE portfolio_equity>0 AND market_date<>%s",
                (exclude_today,),
            )
        else:
            cur.execute(
                "SELECT COUNT(*), MAX(portfolio_equity) FROM dashboard_snapshots "
                "WHERE portfolio_equity>0"
            )
        row = cur.fetchone()
        if not row or not row[0]:
            cur.close()
            return result
        result["count"] = int(row[0])
        result["peak"] = float(row[1] or 0)
        if result["peak"] > 0:
            params = (result["peak"],)
            if exclude_today:
                cur.execute(
                    "SELECT market_date FROM dashboard_snapshots "
                    "WHERE portfolio_equity=%s AND market_date<>%s "
                    "ORDER BY market_date DESC LIMIT 1",
                    (result["peak"], exclude_today),
                )
            else:
                cur.execute(
                    "SELECT market_date FROM dashboard_snapshots "
                    "WHERE portfolio_equity=%s ORDER BY market_date DESC LIMIT 1",
                    params,
                )
            r = cur.fetchone()
            if r:
                result["peak_date"] = r[0]
        cur.close()
    return result


def get_today_equity_peak_trough(today: str) -> Dict[str, Any]:
    """오늘 tick의 MAX/MIN portfolio_equity + 시각.

    Returns: {"peak": float, "peak_ts": str, "trough": float, "trough_ts": str, "count": int}
    """
    result = {"peak": 0.0, "peak_ts": "", "trough": 0.0, "trough_ts": "", "count": 0}
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), MAX(portfolio_equity), MIN(portfolio_equity) "
            "FROM dashboard_snapshots WHERE market_date=%s AND portfolio_equity>0",
            (today,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            cur.close()
            return result
        result["count"] = int(row[0])
        result["peak"] = float(row[1] or 0)
        result["trough"] = float(row[2] or 0)

        if result["peak"] > 0:
            cur.execute(
                "SELECT ts FROM dashboard_snapshots "
                "WHERE market_date=%s AND portfolio_equity=%s ORDER BY epoch DESC LIMIT 1",
                (today, result["peak"]),
            )
            r = cur.fetchone()
            if r:
                result["peak_ts"] = r[0]

        if result["trough"] > 0:
            cur.execute(
                "SELECT ts FROM dashboard_snapshots "
                "WHERE market_date=%s AND portfolio_equity=%s ORDER BY epoch ASC LIMIT 1",
                (today, result["trough"]),
            )
            r = cur.fetchone()
            if r:
                result["trough_ts"] = r[0]
        cur.close()
    return result


def get_snapshot_count_today() -> int:
    """Quick count for diagnostics."""
    today_str = date.today().strftime("%Y-%m-%d")
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM dashboard_snapshots WHERE market_date=%s",
            (today_str,),
        )
        cnt = cur.fetchone()[0]
        cur.close()
    return cnt


def cleanup_old_snapshots(keep_days: int = 30) -> int:
    """Delete snapshots older than keep_days. Run daily."""
    cutoff = (date.today() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM dashboard_snapshots WHERE market_date < %s",
            (cutoff,),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()
    if deleted > 0:
        logger.info(f"[DashDB] Cleaned {deleted} old snapshots (before {cutoff})")
    return deleted
