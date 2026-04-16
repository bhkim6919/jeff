# -*- coding: utf-8 -*-
"""
dual_write.py — Dual Write + Mismatch Ledger
==============================================
검증 기간 동안 PG + SQLite 양쪽에 동시 기록.
Phase 4에서 SQLite 경로 완전 제거 예정.

[정책]
- PG = Source of Truth (강제)
- PG 실패 → raise (시스템 중단)
- SQLite 실패 → mismatch 기록 + Telegram + 계속 진행
- PG → SQLite 방향 복구만 허용
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from shared.db.pg_base import connection

logger = logging.getLogger("qtron.dual_write")

# ── SKIPPED 금지 테이블 ──────────────────────────────────────

_NO_SKIP_TABLES = frozenset({
    "meta_market_context", "meta_strategy_daily", "meta_strategy_exposure",
    "meta_run_quality", "meta_strategy_risk", "meta_strategy_outcome",
    "meta_recommendation", "meta_execution_decision", "meta_universe_snapshot",
    "meta_market_context_us", "meta_strategy_daily_us", "meta_strategy_exposure_us",
    "meta_run_quality_us", "meta_strategy_risk_us", "meta_strategy_outcome_us",
    "meta_recommendation_us", "meta_execution_decision_us", "meta_universe_snapshot_us",
    "regime_predictions", "regime_actuals", "regime_scores", "regime_theme_daily",
    "rest_positions", "rest_equity_snapshots",
})

MAX_RETRY = 3
SKIP_AFTER_DAYS = 5


# ── Mismatch Ledger DDL ─────────────────────────────────────

MISMATCH_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS dual_write_mismatch_log (
    mismatch_id       SERIAL PRIMARY KEY,
    context           TEXT NOT NULL,
    table_name        TEXT NOT NULL,
    pk_repr           TEXT NOT NULL,
    occurred_at       TIMESTAMPTZ DEFAULT NOW(),
    pg_ok             BOOLEAN NOT NULL,
    sqlite_ok         BOOLEAN NOT NULL,
    error_message     TEXT,
    retry_count       INTEGER DEFAULT 0,
    resolved_at       TIMESTAMPTZ,
    resolution_status TEXT
);
"""


def ensure_mismatch_table() -> None:
    """mismatch ledger 테이블 생성 (idempotent)."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(MISMATCH_TABLE_DDL)
        conn.commit()
        cur.close()


# ── Core Dual Write ──────────────────────────────────────────

def dual_write(
    save_pg_fn: Callable[[], Any],
    save_sqlite_fn: Callable[[], Any],
    context: str,
    table_name: str,
    pk_repr: str,
) -> None:
    """
    PG + SQLite 동시 기록.

    Args:
        save_pg_fn: PG 저장 함수 (실패 시 raise)
        save_sqlite_fn: SQLite 저장 함수 (실패 시 mismatch 기록)
        context: 호출 위치 (예: 'regime.save_prediction')
        table_name: 대상 테이블명
        pk_repr: PK 값 문자열 표현
    """
    # PG = source of truth. 실패 시 즉시 중단.
    try:
        save_pg_fn()
    except Exception as e:
        logger.error(
            f"[DUAL_WRITE_FAIL][PG] {context} table={table_name} pk={pk_repr}",
            exc_info=e,
        )
        raise

    # SQLite = fallback/검증용. 실패 시 mismatch 기록 + 계속 진행.
    try:
        save_sqlite_fn()
    except Exception as e:
        logger.error(
            f"[DUAL_WRITE_FAIL][SQLITE] {context} table={table_name} pk={pk_repr}",
            exc_info=e,
        )
        _record_mismatch(
            context=context,
            table_name=table_name,
            pk_repr=pk_repr,
            pg_ok=True,
            sqlite_ok=False,
            error_message=str(e),
        )


# ── Mismatch CRUD ────────────────────────────────────────────

def _record_mismatch(
    context: str,
    table_name: str,
    pk_repr: str,
    pg_ok: bool,
    sqlite_ok: bool,
    error_message: str = "",
) -> None:
    """mismatch 기록 (PG에 저장)."""
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO dual_write_mismatch_log "
                "(context, table_name, pk_repr, pg_ok, sqlite_ok, error_message) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (context, table_name, pk_repr, pg_ok, sqlite_ok, error_message),
            )
            conn.commit()
            cur.close()
        logger.warning(
            f"[DUAL_WRITE_MISMATCH] {context} table={table_name} pk={pk_repr}"
        )
    except Exception as e:
        # mismatch 기록 실패 = 로그만 남기고 진행
        logger.error(f"[DUAL_WRITE_MISMATCH] failed to record: {e}")


def get_unresolved(table_name: Optional[str] = None) -> List[Dict]:
    """미해결 mismatch 목록."""
    with connection() as conn:
        cur = conn.cursor()
        sql = (
            "SELECT mismatch_id, context, table_name, pk_repr, "
            "occurred_at, retry_count "
            "FROM dual_write_mismatch_log "
            "WHERE resolution_status IS NULL"
        )
        params = []
        if table_name:
            sql += " AND table_name = %s"
            params.append(table_name)
        sql += " ORDER BY occurred_at"
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

    return [
        {
            "mismatch_id": r[0], "context": r[1], "table_name": r[2],
            "pk_repr": r[3], "occurred_at": r[4], "retry_count": r[5],
        }
        for r in rows
    ]


def get_unresolved_count(table_group: Optional[str] = None) -> int:
    """미해결 mismatch 수. cutover 차단 판단용."""
    with connection() as conn:
        cur = conn.cursor()
        if table_group:
            cur.execute(
                "SELECT COUNT(*) FROM dual_write_mismatch_log "
                "WHERE resolution_status IS NULL AND table_name LIKE %s",
                (f"%{table_group}%",),
            )
        else:
            cur.execute(
                "SELECT COUNT(*) FROM dual_write_mismatch_log "
                "WHERE resolution_status IS NULL"
            )
        (cnt,) = cur.fetchone()
        cur.close()
    return cnt


def mark_resolved(mismatch_id: int, status: str) -> None:
    """mismatch 해결 표시."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dual_write_mismatch_log "
            "SET resolved_at = NOW(), resolution_status = %s "
            "WHERE mismatch_id = %s",
            (status, mismatch_id),
        )
        conn.commit()
        cur.close()


def increment_retry(mismatch_id: int) -> int:
    """retry 횟수 증가. 반환: 현재 retry_count."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE dual_write_mismatch_log "
            "SET retry_count = retry_count + 1 "
            "WHERE mismatch_id = %s "
            "RETURNING retry_count",
            (mismatch_id,),
        )
        (cnt,) = cur.fetchone()
        conn.commit()
        cur.close()
    return cnt


# ── Reconcile (PG → SQLite 방향만) ──────────────────────────

def reconcile_mismatches(
    read_from_pg_fn: Callable[[str, str], Any],
    write_to_sqlite_fn: Callable[[str, Any], None],
) -> Dict[str, int]:
    """
    매일 batch 시작 시 실행.
    unresolved mismatch를 PG 기준으로 SQLite에 재동기화.

    Args:
        read_from_pg_fn: (table_name, pk_repr) → row data
        write_to_sqlite_fn: (table_name, row_data) → None

    Returns:
        {"resolved": N, "failed": N, "skipped": N}
    """
    stats = {"resolved": 0, "failed": 0, "skipped": 0}
    unresolved = get_unresolved()

    for m in unresolved:
        mid = m["mismatch_id"]
        tbl = m["table_name"]
        pk = m["pk_repr"]
        retries = m["retry_count"]
        age_days = (
            datetime.now(timezone.utc) - m["occurred_at"].replace(tzinfo=timezone.utc)
        ).days

        # SKIPPED 허용 조건 확인
        if retries >= MAX_RETRY:
            if tbl in _NO_SKIP_TABLES:
                logger.error(
                    f"[MISMATCH_RECONCILE_FAIL] {tbl} pk={pk} "
                    f"max retries, MANUAL_REQUIRED (no-skip table)"
                )
                mark_resolved(mid, "MANUAL_REQUIRED")
                stats["failed"] += 1
                continue
            if age_days >= SKIP_AFTER_DAYS:
                logger.warning(
                    f"[MISMATCH_RECONCILE_SKIP] {tbl} pk={pk} "
                    f"age={age_days}d, skipping"
                )
                mark_resolved(mid, "SKIPPED")
                stats["skipped"] += 1
                continue
            stats["failed"] += 1
            continue

        try:
            row = read_from_pg_fn(tbl, pk)
            write_to_sqlite_fn(tbl, row)
            mark_resolved(mid, "RETRY_OK")
            logger.info(f"[MISMATCH_RECONCILE_OK] {tbl} pk={pk}")
            stats["resolved"] += 1
        except Exception as e:
            new_count = increment_retry(mid)
            logger.warning(
                f"[MISMATCH_RECONCILE_FAIL] {tbl} pk={pk} "
                f"retry {new_count}/{MAX_RETRY}: {e}"
            )
            stats["failed"] += 1

    return stats


# ── Cutover Gate ─────────────────────────────────────────────

def is_cutover_allowed(table_group: Optional[str] = None) -> bool:
    """unresolved mismatch > 0 이면 cutover 금지."""
    count = get_unresolved_count(table_group)
    if count > 0:
        logger.error(
            f"[CUTOVER_BLOCKED] {table_group or 'all'}: "
            f"{count} unresolved mismatches"
        )
        return False
    return True
