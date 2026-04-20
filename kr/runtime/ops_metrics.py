"""
ops_metrics.py — Structured Ops Evidence Store
===============================================

Per spec: runtime_state_live.json은 현재 상태(shutdown_reason, session_start, ...)
만 유지하고, 누적 incident/recovery evidence는 본 모듈이 관리하는 별도 snapshot
파일(``ops_metrics.json``)에 기록한다.

Promotion evidence collector (kr/lab/promotion/evidence.py) 가 이 파일을 최우선
source로 조회한다.

## 파일 구조

- ``kr/data/ops/ops_metrics.json`` — 최신 ops snapshot (collector가 읽는 파일).
  각 CRITICAL/HIGH/MEDIUM 필드에 대해:
    {
      "value": int | null,   # null = UNKNOWN (evidence missing)
      "source": "...",       # tracker / reconcile / startup / fallback
      "window": "session" | "24h" | "7d" | "total",
      "ts": ISO8601
    }
- ``kr/data/ops/ops_events.jsonl`` — append-only 이벤트 log (옵션, 감사용).

## 규칙

- **0과 UNKNOWN을 절대 혼동하지 말 것.** 명시적 None 만 UNKNOWN.
- **total_count 하나로 몰지 말 것.** recent (24h) / unresolved (open) / total
  (session) 를 분리하여 저장한다.
- **EOD 확정 이후에만 snapshot persist.** intraday 중간값은 write하지 않는다.
- runtime_state는 collector 의 2순위 fallback 으로만 사용된다.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger("gen4.ops_metrics")


DEFAULT_SNAPSHOT_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "ops" / "ops_metrics.json"
)
DEFAULT_EVENTS_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "ops" / "ops_events.jsonl"
)


# ─── FieldEvidence: 한 ops 필드에 대한 단일 관측 ─────────────────────

@dataclass
class FieldEvidence:
    """Single structured observation for one ops metric field.

    value=None 은 UNKNOWN (evidence missing). 0 과 명확히 구분.
    """
    value: Optional[int] = None
    source: str = "unknown"      # tracker / reconcile / startup / runtime_state
    window: str = "session"      # session / 24h / 7d / total / open
    ts: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


# ─── Snapshot I/O ────────────────────────────────────────────────────

def load_ops_snapshot(path: Optional[Path] = None) -> Dict[str, Any]:
    """Read the latest ops snapshot.

    Storage: **PG primary** (promotion_ops_snapshot) / 파일 fallback.
    - ``path`` 명시 호출 → 파일 모드 (테스트 호환)
    - 없으면 DB 시도 → 실패 시 default 파일 경로

    Structure:
      {
        "_write_ts": "...",
        "_write_origin": "...",
        "<field_name>": {"value": int|null, "source": "...",
                         "window": "...", "ts": "..."},
        ...
      }
    """
    if path is not None:
        return _load_ops_snapshot_file(path)

    # DB primary
    try:
        from lab.promotion import db as _pdb
        snap = _pdb.load_ops_snapshot()
        if snap:
            return snap
        # DB 빈 결과 → 파일 fallback (DB 최초 부팅 시 과거 파일 데이터 보존)
        return _load_ops_snapshot_file(DEFAULT_SNAPSHOT_PATH)
    except Exception as e:
        logger.warning(f"[OPS_METRICS] DB load failed, falling back to file: {e}")
        return _load_ops_snapshot_file(DEFAULT_SNAPSHOT_PATH)


def _load_ops_snapshot_file(path: Path) -> Dict[str, Any]:
    """File-based load."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[OPS_METRICS] file load failed {path}: {e}")
        return {}


def save_ops_snapshot(
    fields: Dict[str, FieldEvidence],
    *,
    path: Optional[Path] = None,
    origin: str = "eod_finalize",
) -> bool:
    """Atomically write a new ops snapshot.

    Storage: **PG primary** (UPSERT per field) / 파일 fallback.
    - ``path`` 명시 호출 → 파일 모드 (테스트 호환)

    ``fields`` — mapping of structured-field-name → FieldEvidence.
    UNKNOWN fields must still be written with ``value=None``.
    """
    # Validate + normalize ts
    ts_now = _now_iso()
    for name, ev in fields.items():
        if not isinstance(ev, FieldEvidence):
            raise TypeError(f"field {name!r} must be FieldEvidence, got {type(ev)}")
        if not ev.ts:
            ev.ts = ts_now

    # 명시 파일 경로 → 파일 모드
    if path is not None:
        return _save_ops_snapshot_file(fields, path=path, origin=origin)

    # DB primary
    try:
        from lab.promotion import db as _pdb
        for fname, ev in fields.items():
            _pdb.upsert_ops_field(
                field_name=fname,
                value=ev.value,
                source=ev.source,
                window=ev.window,
                ts=ev.ts,
                write_origin=origin,
            )
        known = sum(1 for v in fields.values() if v.value is not None)
        unknown = sum(1 for v in fields.values() if v.value is None)
        logger.info(
            f"[OPS_METRICS] DB snapshot saved: {known} known, {unknown} UNKNOWN"
        )
        return True
    except Exception as e:
        logger.warning(f"[OPS_METRICS] DB save failed, falling back to file: {e}")
        return _save_ops_snapshot_file(fields, path=DEFAULT_SNAPSHOT_PATH, origin=origin)


def _save_ops_snapshot_file(
    fields: Dict[str, FieldEvidence],
    *,
    path: Path,
    origin: str,
) -> bool:
    """File-based snapshot write (atomic via tmp+replace)."""
    _ensure_dir(path)

    payload: Dict[str, Any] = {
        "_write_ts": _now_iso(),
        "_write_origin": origin,
    }
    for name, ev in fields.items():
        payload[name] = ev.to_dict()

    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        _ = json.loads(tmp.read_text(encoding="utf-8"))
        import os
        os.replace(str(tmp), str(path))
        logger.info(
            f"[OPS_METRICS] file snapshot saved: "
            f"{len([v for v in fields.values() if v.value is not None])} known, "
            f"{len([v for v in fields.values() if v.value is None])} UNKNOWN"
        )
        return True
    except Exception as e:
        logger.error(f"[OPS_METRICS] file save failed: {e}")
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
        return False


# ─── Event log (append-only, optional) ───────────────────────────────

def append_ops_event(
    event_type: str,
    payload: Dict[str, Any],
    *,
    path: Optional[Path] = None,
) -> bool:
    """Append one incident event.

    Storage: **PG primary** (promotion_ops_events) / 파일 fallback (JSONL).
    Event types: RECON_OK / RECON_FAIL / DUPLICATE_FILL / ORDER_TIMEOUT /
    DIRTY_EXIT_RECOVERY / PENDING_STALE_DISCARD / STATE_UNCERTAIN / ...
    """
    # 명시 파일 경로 → 파일 모드
    if path is not None:
        return _append_ops_event_file(event_type, payload, path)

    # DB primary
    try:
        from lab.promotion import db as _pdb
        _pdb.append_ops_event(event_type, payload or {})
        return True
    except Exception as e:
        logger.warning(f"[OPS_METRICS] DB event append failed, falling back to file ({event_type}): {e}")
        return _append_ops_event_file(event_type, payload, DEFAULT_EVENTS_PATH)


def _append_ops_event_file(event_type: str, payload: Dict[str, Any], path: Path) -> bool:
    _ensure_dir(path)
    row = {
        "ts": _now_iso(),
        "event_type": event_type,
        **(payload or {}),
    }
    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        return True
    except Exception as e:
        logger.warning(f"[OPS_METRICS] file event append failed ({event_type}): {e}")
        return False


# ─── Aggregation helpers ─────────────────────────────────────────────

def aggregate_from_tracker(tracker) -> Dict[str, FieldEvidence]:
    """OrderTracker.ops_snapshot() → structured FieldEvidence dict.

    Tracker 가 None 이거나 ops_snapshot 이 없으면 빈 dict (UNKNOWN 으로 남김).
    """
    if tracker is None or not hasattr(tracker, "ops_snapshot"):
        return {}
    try:
        snap = tracker.ops_snapshot() or {}
    except Exception as e:
        logger.warning(f"[OPS_METRICS] tracker.ops_snapshot failed: {e}")
        return {}

    out: Dict[str, FieldEvidence] = {}
    ts = _now_iso()

    if "duplicate_execution_incident_count_total" in snap:
        out["duplicate_execution_incident_count"] = FieldEvidence(
            value=int(snap["duplicate_execution_incident_count_total"]),
            source="order_tracker",
            window="session",
            ts=ts,
        )
    if "order_timeout_events_total" in snap:
        out["order_timeout_24h"] = FieldEvidence(
            value=int(snap["order_timeout_events_total"]),
            source="order_tracker",
            window="session",
            ts=ts,
        )
    # pending_external_unresolved_count — spec §3 recent/unresolved 분리 요구사항
    if "pending_external_unresolved_count" in snap:
        out["pending_external_unresolved_count"] = FieldEvidence(
            value=int(snap["pending_external_unresolved_count"]),
            source="order_tracker",
            window="open",
            ts=ts,
        )
    return out


def aggregate_from_reconcile(
    *,
    recon_ok_streak_days: Optional[int] = None,
    broker_mismatch_unresolved_count: Optional[int] = None,
    recon_unreliable_events_24h: Optional[int] = None,
) -> Dict[str, FieldEvidence]:
    """RECON phase → FieldEvidence. 호출자가 unknown 여부를 명시해야 함.

    모든 인자가 None 이면 해당 필드는 emit 되지 않고 UNKNOWN 유지.
    """
    ts = _now_iso()
    out: Dict[str, FieldEvidence] = {}
    if recon_ok_streak_days is not None:
        out["recon_ok_streak_days"] = FieldEvidence(
            value=int(recon_ok_streak_days),
            source="reconcile",
            window="total",
            ts=ts,
        )
    if broker_mismatch_unresolved_count is not None:
        out["broker_mismatch_unresolved_count"] = FieldEvidence(
            value=int(broker_mismatch_unresolved_count),
            source="reconcile",
            window="open",
            ts=ts,
        )
    if recon_unreliable_events_24h is not None:
        out["recon_unreliable_24h"] = FieldEvidence(
            value=int(recon_unreliable_events_24h),
            source="reconcile",
            window="24h",
            ts=ts,
        )
    return out


def aggregate_from_startup(
    *,
    dirty_exit_recovery_fail_count: Optional[int] = None,
    pending_external_stale_cleanup_fail_count: Optional[int] = None,
    state_uncertain_days_recent: Optional[int] = None,
) -> Dict[str, FieldEvidence]:
    """Startup phase → FieldEvidence."""
    ts = _now_iso()
    out: Dict[str, FieldEvidence] = {}
    if dirty_exit_recovery_fail_count is not None:
        out["dirty_exit_recovery_fail_count"] = FieldEvidence(
            value=int(dirty_exit_recovery_fail_count),
            source="startup",
            window="session",
            ts=ts,
        )
    if pending_external_stale_cleanup_fail_count is not None:
        out["pending_external_stale_cleanup_fail_count"] = FieldEvidence(
            value=int(pending_external_stale_cleanup_fail_count),
            source="startup",
            window="session",
            ts=ts,
        )
    if state_uncertain_days_recent is not None:
        out["state_uncertain_days_recent"] = FieldEvidence(
            value=int(state_uncertain_days_recent),
            source="startup",
            window="7d",
            ts=ts,
        )
    return out


def merge_and_save(
    *parts: Iterable[Dict[str, FieldEvidence]],
    origin: str = "eod_finalize",
    path: Optional[Path] = None,
) -> bool:
    """여러 aggregate_* 결과를 병합해서 snapshot 으로 저장.

    Storage: **PG primary** (save_ops_snapshot 경유) / 파일 fallback.
    우선순위: 나중에 온 part가 같은 필드를 덮어씀 (호출자가 순서 책임).
    병합 중 누락된 필드는 prior snapshot 유지 — 완전 덮어쓰기 대신 merge.
    """
    merged_fields: Dict[str, FieldEvidence] = {}

    # 1) prior snapshot 불러와서 baseline (DB primary, 파일 fallback 자동)
    prior = load_ops_snapshot(path)  # path None → DB, 명시 → file
    for k, v in prior.items():
        if k.startswith("_"):
            continue
        if isinstance(v, dict) and "value" in v:
            merged_fields[k] = FieldEvidence(
                value=v.get("value"),
                source=str(v.get("source", "unknown")),
                window=str(v.get("window", "session")),
                ts=str(v.get("ts", "")),
            )

    # 2) incoming parts 적용
    for part in parts:
        if not part:
            continue
        for k, ev in part.items():
            merged_fields[k] = ev

    return save_ops_snapshot(merged_fields, path=path, origin=origin)
