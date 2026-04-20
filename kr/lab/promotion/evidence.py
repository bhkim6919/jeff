"""
evidence.py — Ops Evidence Collector
=====================================
Per spec §1. 운영 증거(ops metrics)를 **4단계 우선순위** source에서 수집.

우선순위:
  1) structured ops snapshot (``kr/data/ops/ops_metrics.json`` — 누적 evidence 전용)
  2) runtime_state_live.json (엔진이 매 cycle write, 현재 상태 위주)
  3) log summary JSON (reconcile/decision 로그 집계)
  4) 없으면 UNKNOWN (None 반환, 0으로 대체 금지)

출력: OpsMetrics (hard_gates.py의 dataclass) — None 값 = UNKNOWN

**핵심 규칙**:
- default 0 금지. 증거 부재는 반드시 None.
- 각 필드에 evidence_source 기록 (감사/디버깅)
- source가 없거나 stale이면 UNKNOWN
- ops_metrics.json 은 누적 evidence 전용, runtime_state 는 현재 상태 전용으로 분리
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

from .hard_gates import OpsMetrics

logger = logging.getLogger("promotion.evidence")

# Evidence freshness threshold — 이 이상 오래되면 UNKNOWN
# runtime_state는 엔진이 매 cycle write → 10분 이상이면 stale
RUNTIME_STATE_MAX_AGE_MIN = 30

# ops_metrics.json 은 EOD 확정 시점에 한 번만 write 되므로 daily cadence.
# 최대 2 영업일(주말 1일 + 1 영업일 = 최대 3일) 까지 허용.
OPS_SNAPSHOT_MAX_AGE_H = 72

# Structured fields in runtime_state_live.json — 실제 운영 감시자가 기록하는 값
# None인 경우 명시적으로 null 저장 필수 (not missing key)
_STRUCTURED_FIELDS = {
    "recon_ok_streak": "recon_ok_streak_days",
    "unresolved_broker_mismatch": "broker_mismatch_unresolved_count",
    "duplicate_execution_count": "duplicate_execution_incident_count",
    "stale_decision_input_count": "stale_decision_input_incident_count",
    "dirty_exit_recovery_fail_count": "dirty_exit_recovery_fail_count",
    "pending_external_stale_cleanup_fail_count": "pending_external_stale_cleanup_fail_count",
    "state_uncertain_days_recent": "state_uncertain_days_recent",
    "recon_unreliable_events_24h": "recon_unreliable_24h",
    "order_timeout_events_24h": "order_timeout_24h",
    "ghost_fill_events_24h": "ghost_fill_24h",
    "telegram_failures_24h": "telegram_fail_24h",
    "log_rotation_failures": "log_rotation_failures",
}


def _read_structured_state(runtime_state_path: Path) -> Tuple[Dict, bool]:
    """
    Returns (state_dict, is_fresh).
    is_fresh = False 면 stale → 필드 조회 결과 UNKNOWN 처리.
    """
    if not runtime_state_path.exists():
        return {}, False
    try:
        raw = runtime_state_path.read_text(encoding="utf-8")
        state = json.loads(raw)
    except Exception as e:
        logger.warning(f"[EVIDENCE] runtime_state load failed: {e}")
        return {}, False

    # Freshness check — _write_ts 기준
    ts_raw = state.get("_write_ts", "")
    if ts_raw:
        try:
            ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - ts
            if age > timedelta(minutes=RUNTIME_STATE_MAX_AGE_MIN):
                logger.info(
                    f"[EVIDENCE] runtime_state stale: age={age.total_seconds()/60:.1f}min"
                )
                return state, False
        except Exception:
            return state, False
    else:
        return state, False

    return state, True


def _field_from_state(state: Dict, key: str, is_fresh: bool) -> Tuple[Optional[int], str]:
    """
    Structured runtime_state에서 필드 읽기.
    Returns (value, source_tag).
      - value=None, source="unknown": 키 없음 / stale / not int
      - value=int, source="structured_state": 정상
    """
    if not is_fresh:
        return None, "stale_state"

    if key not in state:
        return None, "unknown"

    raw = state.get(key)
    # null 명시적 저장 → UNKNOWN 유지
    if raw is None:
        return None, "explicit_null"

    try:
        return int(raw), "structured_state"
    except (TypeError, ValueError):
        return None, "parse_error"


def _try_log_summary(summary_path: Path, key: str) -> Tuple[Optional[int], str]:
    """
    Fallback 3단계: log summary JSON (reconcile/decision summary).
    예시 경로: kr/data/logs/summary/runtime_summary.json
    """
    if not summary_path.exists():
        return None, "unknown"
    try:
        raw = summary_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if key in data and data[key] is not None:
            try:
                return int(data[key]), "log_summary"
            except (TypeError, ValueError):
                return None, "summary_parse_error"
    except Exception:
        return None, "summary_read_error"
    return None, "unknown"


def _read_ops_snapshot(snapshot_path: Path) -> Tuple[Dict, bool]:
    """ops_metrics.json 읽기 + freshness 체크.

    Returns (snapshot_dict, is_fresh).
    EOD 확정 시점에만 write 되므로 72h 까지 허용.
    """
    if not snapshot_path.exists():
        return {}, False
    try:
        snap = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[EVIDENCE] ops snapshot load failed: {e}")
        return {}, False
    ts_raw = snap.get("_write_ts", "")
    if not ts_raw:
        return snap, False
    try:
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - ts
        if age > timedelta(hours=OPS_SNAPSHOT_MAX_AGE_H):
            logger.info(
                f"[EVIDENCE] ops snapshot stale: age={age.total_seconds()/3600:.1f}h"
            )
            return snap, False
    except Exception:
        return snap, False
    return snap, True


def _field_from_snapshot(snap: Dict, key: str, is_fresh: bool) -> Tuple[Optional[int], str]:
    """ops_metrics.json 에서 FieldEvidence dict 읽기.

    Returns (value, source_tag). snapshot 의 구조:
      snap[key] = {"value": int|null, "source": "...", "window": "...", "ts": "..."}
    """
    if not is_fresh:
        return None, "stale_snapshot"
    ev = snap.get(key)
    if not isinstance(ev, dict):
        return None, "unknown"
    val = ev.get("value")
    if val is None:
        return None, "explicit_null_snapshot"
    try:
        return int(val), f"ops_snapshot:{ev.get('source', 'unknown')}"
    except (TypeError, ValueError):
        return None, "parse_error"


def collect_ops_evidence(
    runtime_state_path: Path,
    log_summary_path: Optional[Path] = None,
    ops_snapshot_path: Optional[Path] = None,
) -> OpsMetrics:
    """Structured ops snapshot + runtime state + log summary 병합 수집.

    각 OpsMetrics 필드 — 4단계 우선순위:
      1) ops_metrics.json (누적 evidence 전용 snapshot)
      2) runtime_state_live.json (현재 상태 필드)
      3) log summary JSON (fallback)
      4) 없으면 None (UNKNOWN)

    Returns: OpsMetrics (일부 None 가능)
    """
    # 1st: ops snapshot (신규 — 누적 evidence)
    snap: Dict = {}
    snap_fresh = False
    if ops_snapshot_path is not None:
        snap, snap_fresh = _read_ops_snapshot(ops_snapshot_path)

    # 2nd: runtime state (현재 상태)
    state, is_fresh = _read_structured_state(runtime_state_path)

    ops = OpsMetrics()
    sources: Dict[str, str] = {}

    for ops_field, structured_key in _STRUCTURED_FIELDS.items():
        # 1st priority — ops snapshot
        val, src = _field_from_snapshot(snap, structured_key, snap_fresh)

        # 2nd — runtime state
        if val is None:
            val, src_rt = _field_from_state(state, structured_key, is_fresh)
            if val is not None:
                src = src_rt
            elif src == "unknown" or src == "stale_snapshot":
                src = src_rt  # runtime state 의 src 로 사유 교체

        # 3rd — log summary
        if val is None and log_summary_path is not None:
            val2, src2 = _try_log_summary(log_summary_path, structured_key)
            if val2 is not None:
                val = val2
                src = src2

        setattr(ops, ops_field, val)
        sources[ops_field] = src

    ops.evidence_sources = sources

    # 감사 로그
    unknown_fields = [k for k, v in sources.items()
                      if v in ("unknown", "stale_snapshot", "stale_state",
                               "explicit_null", "explicit_null_snapshot")]
    if unknown_fields:
        logger.info(
            f"[EVIDENCE] {len(unknown_fields)} fields UNKNOWN (evidence missing): "
            f"{', '.join(unknown_fields[:5])}{'...' if len(unknown_fields) > 5 else ''}"
        )

    return ops


def is_fully_known(ops: OpsMetrics) -> bool:
    """모든 CRITICAL 필드가 관측됐는지."""
    return not ops.has_any_unknown_critical()
