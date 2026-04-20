# -*- coding: utf-8 -*-
"""
rest_state_db.py — Phase 1 REST State Database (Observer)
==========================================================
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
broker = truth, Gen4 state = reference, REST_DB = observer + calculator.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("gen4.rest.state_db")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def make_snapshot_id() -> str:
    """현재 시점 기반 snapshot ID 생성."""
    return datetime.now().strftime("%Y%m%d-%H%M%S") + f"-{int(time.time()*1000)%10000:04d}"


# ── Positions ─────────────────────────────────────────────────

def sync_positions_from_gen4(
    gen4_positions: dict,
    snapshot_id: str,
    asof_ts: Optional[str] = None,
) -> int:
    """Gen4 state 포지션을 REST_DB에 미러링. Returns: 동기화 수."""
    if not gen4_positions:
        return 0

    now = _now_iso()
    asof = asof_ts or now
    today = datetime.now().strftime("%Y-%m-%d")
    synced = 0
    run_ts = now_utc()

    try:
        with connection() as conn:
            cur = conn.cursor()
            for code, pos in gen4_positions.items():
                if not code or not isinstance(pos, dict):
                    continue
                cur.execute("""
                    INSERT INTO rest_positions
                        (code, name, qty, avg_price, entry_date, current_price,
                         high_watermark, trail_stop_price, invested_total,
                         is_active, snapshot_id, snapshot_date, asof_ts,
                         run_ts, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s, %s, %s)
                    ON CONFLICT (snapshot_date, code) DO UPDATE SET
                        qty = EXCLUDED.qty,
                        avg_price = EXCLUDED.avg_price,
                        current_price = EXCLUDED.current_price,
                        high_watermark = EXCLUDED.high_watermark,
                        trail_stop_price = EXCLUDED.trail_stop_price,
                        snapshot_id = EXCLUDED.snapshot_id,
                        run_ts = EXCLUDED.run_ts,
                        updated_at = EXCLUDED.updated_at
                    WHERE rest_positions.run_ts < EXCLUDED.run_ts
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
                    snapshot_id, today, asof, run_ts, now,
                ))
                synced += 1

            conn.commit()
            cur.close()
            logger.info(f"[REST_DB] sync_positions: {synced} codes, snapshot={snapshot_id}")
    except Exception as e:
        logger.warning(f"[REST_DB] sync_positions failed: {e}")

    return synced


def get_latest_positions() -> Dict[str, dict]:
    """최신 snapshot의 active 포지션 조회."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM rest_positions
            WHERE snapshot_date = (
                SELECT snapshot_date FROM rest_positions
                ORDER BY asof_ts DESC LIMIT 1
            )
        """)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return {r[cols.index("code")]: dict(zip(cols, r)) for r in rows}


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
    """Equity 스냅샷 저장."""
    now = _now_iso()
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            # snapshot_seq: 같은 날 몇 번째 스냅샷인지
            cur.execute(
                "SELECT COALESCE(MAX(snapshot_seq), -1) + 1 "
                "FROM rest_equity_snapshots WHERE market_date=%s",
                (market_date,),
            )
            seq = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO rest_equity_snapshots
                    (market_date, snapshot_seq, asof_ts, is_eod, close_equity,
                     prev_close_equity, peak_equity, cash, holdings_count,
                     rebalance_cycle_id, snapshot_id, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_date, snapshot_seq) DO NOTHING
            """, (
                market_date, seq, now, int(is_eod), equity,
                prev_close_equity, peak_equity, cash, holdings_count,
                rebalance_cycle_id, snapshot_id or make_snapshot_id(),
                run_ts, now,
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.warning(f"[REST_DB] sync_equity failed: {e}")


def get_latest_equity(market_date: Optional[str] = None) -> Optional[dict]:
    """최근 equity 스냅샷 조회."""
    with connection() as conn:
        cur = conn.cursor()
        if market_date:
            cur.execute(
                "SELECT * FROM rest_equity_snapshots "
                "WHERE market_date=%s ORDER BY asof_ts DESC LIMIT 1",
                (market_date,),
            )
        else:
            cur.execute(
                "SELECT * FROM rest_equity_snapshots "
                "ORDER BY asof_ts DESC LIMIT 1"
            )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        cur.close()
    return dict(zip(cols, row))


def get_eod_equity(market_date: str) -> Optional[dict]:
    """특정 날짜의 EOD 확정 equity 조회."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM rest_equity_snapshots "
            "WHERE market_date=%s AND is_eod=1 "
            "ORDER BY asof_ts DESC LIMIT 1",
            (market_date,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        cur.close()
    return dict(zip(cols, row))


def get_prev_eod_equity(today: str) -> Optional[dict]:
    """today 이전 가장 최근 EOD snapshot 조회."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM rest_equity_snapshots "
            "WHERE market_date<%s AND is_eod=1 "
            "ORDER BY market_date DESC LIMIT 1",
            (today,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        cur.close()
    return dict(zip(cols, row))


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
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO rest_validation_log
                    (check_time, check_type, snapshot_id,
                     gen4_value, rest_value, broker_value,
                     diff_pct, status, detail, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (check_time, check_type) DO NOTHING
            """, (
                now, check_type, snapshot_id,
                str(gen4_value) if gen4_value is not None else None,
                str(rest_value) if rest_value is not None else None,
                str(broker_value) if broker_value is not None else None,
                diff_pct, status, detail, now_utc(), now,
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.debug(f"[REST_DB] log_validation failed: {e}")


def get_recent_validations(limit: int = 50) -> List[dict]:
    """최근 교차검증 결과 조회."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM rest_validation_log "
            "ORDER BY check_time DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


def cleanup_old_data(keep_days: int = 30) -> int:
    """오래된 데이터 정리."""
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM rest_equity_snapshots WHERE market_date < %s", (cutoff,))
        d1 = cur.rowcount
        cur.execute("DELETE FROM rest_validation_log WHERE check_time < %s", (cutoff,))
        d2 = cur.rowcount
        conn.commit()
        cur.close()
    return d1 + d2
