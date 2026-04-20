"""
promotion/db.py — PG access layer for promotion artifacts
==========================================================
Per Phase C (2026-04-20): Promotion artifacts(regime_history, ops_metrics,
transition_log)를 파일(JSONL/JSON)에서 PostgreSQL로 이전.

정책
----
- **DB primary**: 모든 쓰기/읽기가 PG 우선.
- **파일 fallback**: DB 연결 실패 시에만 기존 file I/O 경로 사용.
- **Unknown vs 0 분리**: ops snapshot에서 value=NULL은 UNKNOWN (evidence missing).
- **Idempotency**:
    - regime: (trade_date, strategy_name, snapshot_version) 유일.
    - ops_snapshot: field_name UPSERT.
    - transition: application layer에서 직전 new_status 비교로 중복 방지.

예약어 주의
----------
Column name ``window_scope`` 는 PG 예약어 ``window`` 를 피하기 위해 rename됨.
Application 측 dataclass는 여전히 ``window`` 로 표기 (migration boundary에서만 변환).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from shared.db.pg_base import connection

logger = logging.getLogger("promotion.db")


# ─── Regime History ─────────────────────────────────────────────────

def insert_regime_record(
    *,
    trade_date: str,
    strategy_name: str,
    regime_label: str,
    confidence: float,
    regime_source_version: str,
    snapshot_version: str,
) -> bool:
    """Regime 이력 1건 INSERT.

    (trade_date, strategy_name, snapshot_version) UNIQUE 위반 시 False 반환
    (idempotent — 동일 snapshot 재호출 안전).
    """
    sql = """
    INSERT INTO promotion_regime_history
      (trade_date, strategy_name, regime_label, regime_source_version,
       confidence, snapshot_version, recorded_at)
    VALUES (%s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (trade_date, strategy_name, snapshot_version) DO NOTHING
    RETURNING id
    """
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            trade_date, strategy_name, regime_label,
            regime_source_version, confidence, snapshot_version,
        ))
        row = cur.fetchone()
        conn.commit()
        cur.close()
    return row is not None


def load_regime_records(strategy_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """전체 또는 특정 전략의 regime 이력. trade_date 오름차순."""
    if strategy_name:
        sql = """
        SELECT trade_date, strategy_name, regime_label, regime_source_version,
               confidence::float, snapshot_version, recorded_at
        FROM promotion_regime_history
        WHERE strategy_name = %s
        ORDER BY trade_date ASC, recorded_at ASC
        """
        params = (strategy_name,)
    else:
        sql = """
        SELECT trade_date, strategy_name, regime_label, regime_source_version,
               confidence::float, snapshot_version, recorded_at
        FROM promotion_regime_history
        ORDER BY trade_date ASC, recorded_at ASC
        """
        params = ()

    out: List[Dict[str, Any]] = []
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        for td, sn, rl, rsv, cf, sv, rat in cur.fetchall():
            out.append({
                "trade_date": td.strftime("%Y-%m-%d") if hasattr(td, "strftime") else str(td),
                "strategy_name": sn,
                "regime_label": rl,
                "regime_source_version": rsv,
                "confidence": float(cf or 0.0),
                "snapshot_version": sv or "",
                "recorded_at": rat.isoformat(timespec="seconds") if rat else "",
            })
        cur.close()
    return out


# ─── Ops Snapshot (current state — UPSERT) ──────────────────────────

def upsert_ops_field(
    field_name: str,
    value: Optional[int],
    source: str,
    window: str,
    ts: str,
    write_origin: str = "eod_finalize",
) -> None:
    """단일 ops field UPSERT. value=None은 UNKNOWN으로 명시 저장."""
    sql = """
    INSERT INTO promotion_ops_snapshot
      (field_name, value, source, window_scope, ts, write_origin)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (field_name) DO UPDATE
      SET value = EXCLUDED.value,
          source = EXCLUDED.source,
          window_scope = EXCLUDED.window_scope,
          ts = EXCLUDED.ts,
          write_origin = EXCLUDED.write_origin
    """
    # ts가 문자열이면 그대로 (PG TIMESTAMPTZ 파싱), None이면 NOW() 사용은 생략 (항상 넘김).
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (field_name, value, source, window, ts, write_origin))
        conn.commit()
        cur.close()


def load_ops_snapshot() -> Dict[str, Any]:
    """현재 저장된 모든 field 반환. File 기반 스키마와 호환되는 dict 생성.

    구조:
      {
        "_write_ts": ts of most recent row,
        "_write_origin": origin of most recent row,
        "<field_name>": {"value":..., "source":..., "window":..., "ts":...},
        ...
      }
    """
    sql = """
    SELECT field_name, value, source, window_scope, ts, write_origin
    FROM promotion_ops_snapshot
    """
    out: Dict[str, Any] = {}
    latest_ts: Optional[datetime] = None
    latest_origin = ""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        for fn, val, src, win, ts, origin in cur.fetchall():
            out[fn] = {
                "value": val,  # None = UNKNOWN preserved
                "source": src,
                "window": win,
                "ts": ts.isoformat(timespec="seconds") if ts else "",
            }
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts
                latest_origin = origin or ""
        cur.close()
    if latest_ts:
        out["_write_ts"] = latest_ts.isoformat(timespec="seconds")
        out["_write_origin"] = latest_origin
    return out


# ─── Ops Events (append-only) ───────────────────────────────────────

def append_ops_event(event_type: str, payload: Dict[str, Any]) -> None:
    """Ops event 1건 append. Best-effort (DB 실패는 caller가 fallback)."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO promotion_ops_events (event_type, payload) VALUES (%s, %s::jsonb)",
            (event_type, json.dumps(payload, default=str)),
        )
        conn.commit()
        cur.close()


# ─── Transition Log (append; dedup at app layer) ────────────────────

def last_transition_status(strategy: str) -> Optional[str]:
    """해당 전략의 가장 최근 new_status 반환."""
    sql = """
    SELECT new_status FROM promotion_transition_log
    WHERE strategy = %s
    ORDER BY evaluated_at DESC, id DESC
    LIMIT 1
    """
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (strategy,))
        row = cur.fetchone()
        cur.close()
    return row[0] if row else None


def insert_transition(
    *,
    strategy: str,
    old_status: Optional[str],
    new_status: str,
    reason: str,
    blockers: List[str],
    score: Optional[int],
    versions: Dict[str, Any],
) -> None:
    sql = """
    INSERT INTO promotion_transition_log
      (strategy, old_status, new_status, reason, blockers, score, versions)
    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s::jsonb)
    """
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (
            strategy, old_status, new_status, reason,
            json.dumps(list(blockers or []), ensure_ascii=False),
            score,
            json.dumps(dict(versions or {}), ensure_ascii=False),
        ))
        conn.commit()
        cur.close()


def load_transitions(
    strategy: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    base = """
    SELECT strategy, evaluated_at, old_status, new_status,
           reason, blockers, score, versions
    FROM promotion_transition_log
    """
    if strategy:
        sql = base + " WHERE strategy = %s ORDER BY evaluated_at ASC, id ASC"
        params: tuple = (strategy,)
    else:
        sql = base + " ORDER BY evaluated_at ASC, id ASC"
        params = ()

    out: List[Dict[str, Any]] = []
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

    # load_transitions의 기존 동작: 마지막 N개 반환. SQL LIMIT가 아닌 Python slice로.
    if limit > 0 and len(rows) > limit:
        rows = rows[-limit:]

    for strat, eat, old, new, rsn, blk, sc, ver in rows:
        out.append({
            "strategy": strat,
            "evaluated_at": eat.isoformat(timespec="seconds") if eat else "",
            "old_status": old,
            "new_status": new,
            "reason": rsn or "",
            "blockers": list(blk) if blk else [],
            "score": sc,
            "versions": dict(ver) if ver else {},
        })
    return out


# ─── Connectivity check ─────────────────────────────────────────────

def is_db_available() -> bool:
    """PG 연결 시도. 실패 시 False — caller가 파일 fallback 판정용."""
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
        return True
    except Exception as e:
        logger.debug(f"[PROMO_DB_UNAVAILABLE] {e}")
        return False
