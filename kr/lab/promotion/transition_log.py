"""
transition_log.py — Promotion Status Transition Log
=====================================================
Per spec §4.

저장 스키마 (jsonl, append-only):
  {
    "strategy": "breakout_trend",
    "evaluated_at": ISO8601 UTC,
    "old_status": "BLOCKED" | null,   # 최초 기록 시 null
    "new_status": "CANDIDATE",
    "reason": "sample reached 60 days, readiness 52",
    "blockers": [str, ...],
    "score": int or null,
    "versions": {cost_model, fill_model, slippage, metrics}
  }

**중복 방지 규칙**:
- 동일 전략의 직전 기록과 new_status가 같으면 append 안 함 (no-op)
- 단, blockers 내용이 바뀌면 별도 snapshot 기록 가능 (옵션)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("promotion.transition_log")

DEFAULT_LOG_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "promotion" / "transition_log.jsonl"
)


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _last_status_file(strategy: str, log_path: Path) -> Optional[str]:
    """해당 전략의 가장 최근 new_status 반환 (파일 기반)."""
    if not log_path.exists():
        return None
    last: Optional[str] = None
    try:
        for line in log_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if row.get("strategy") == strategy:
                    last = row.get("new_status")
            except Exception:
                continue
    except Exception:
        return None
    return last


def record_transition(
    *,
    strategy: str,
    new_status: str,
    score: Optional[int] = None,
    blockers: Optional[List[str]] = None,
    versions: Optional[Dict] = None,
    reason: str = "",
    log_path: Optional[Path] = None,
    force: bool = False,
) -> bool:
    """Status 변화 시 append. 직전 상태와 동일하면 skip (중복 방지).

    Storage: **PG primary** (promotion_transition_log) / 파일 fallback.
    - ``log_path`` 명시 호출은 파일 모드 (테스트 호환).
    - force=True면 status 동일해도 강제 기록.

    Returns: True (기록됨) / False (skip)
    """
    # 명시 파일 경로 → 파일 모드
    if log_path is not None:
        return _record_transition_file(
            strategy=strategy, new_status=new_status, score=score,
            blockers=blockers, versions=versions, reason=reason,
            log_path=log_path, force=force,
        )

    # DB primary
    try:
        from . import db as _pdb
        old_status = _pdb.last_transition_status(strategy)
        if not force and old_status == new_status:
            return False
        _pdb.insert_transition(
            strategy=strategy,
            old_status=old_status,
            new_status=new_status,
            reason=reason,
            blockers=list(blockers or []),
            score=score,
            versions=dict(versions or {}),
        )
        logger.info(
            f"[PROMO_TRANSITION] {strategy}: {old_status} -> {new_status} "
            f"(score={score}, blockers={len(blockers or [])})"
        )
        return True
    except Exception as e:
        logger.warning(f"[PROMO_TRANSITION] DB write failed, falling back to file: {e}")
        return _record_transition_file(
            strategy=strategy, new_status=new_status, score=score,
            blockers=blockers, versions=versions, reason=reason,
            log_path=DEFAULT_LOG_PATH, force=force,
        )


def _record_transition_file(
    *,
    strategy: str,
    new_status: str,
    score: Optional[int],
    blockers: Optional[List[str]],
    versions: Optional[Dict],
    reason: str,
    log_path: Path,
    force: bool,
) -> bool:
    """File-based record (DB 실패 또는 테스트 경로)."""
    _ensure_dir(log_path)
    old_status = _last_status_file(strategy, log_path)

    if not force and old_status == new_status:
        return False

    row = {
        "strategy": strategy,
        "evaluated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "old_status": old_status,
        "new_status": new_status,
        "reason": reason,
        "blockers": list(blockers or []),
        "score": score,
        "versions": dict(versions or {}),
    }
    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        logger.info(
            f"[PROMO_TRANSITION] {strategy}: {old_status} -> {new_status} "
            f"(file, score={score}, blockers={len(row['blockers'])})"
        )
        return True
    except Exception as e:
        logger.warning(f"[PROMO_TRANSITION] file write failed: {e}")
        return False


def load_transitions(
    strategy: Optional[str] = None,
    log_path: Optional[Path] = None,
    limit: int = 100,
) -> List[Dict]:
    """전체 또는 특정 전략의 transition 이력 로드.

    Storage: **PG primary** / 파일 fallback.
    """
    # 명시 파일 경로 → 파일 모드
    if log_path is not None:
        return _load_transitions_file(strategy, log_path, limit)

    # DB primary
    try:
        from . import db as _pdb
        return _pdb.load_transitions(strategy, limit=limit)
    except Exception as e:
        logger.warning(f"[PROMO_TRANSITION] DB load failed, falling back to file: {e}")
        return _load_transitions_file(strategy, DEFAULT_LOG_PATH, limit)


def _load_transitions_file(
    strategy: Optional[str],
    path: Path,
    limit: int,
) -> List[Dict]:
    if not path.exists():
        return []
    rows: List[Dict] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if strategy is None or row.get("strategy") == strategy:
                    rows.append(row)
            except Exception:
                continue
    except Exception:
        return []
    return rows[-limit:] if limit > 0 else rows
