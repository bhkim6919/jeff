"""
regime_history.py — EOD 확정 기준 레짐 이력 저장 + 집계
==========================================================
Per spec §3.

저장 스키마 (jsonl, append-only):
  {
    "trade_date": "YYYY-MM-DD",
    "strategy_name": "breakout_trend",
    "regime_label": "BEAR" | "BULL" | "SIDEWAYS" | "UNKNOWN",
    "regime_source_version": "REGIME_V1",
    "confidence": 0.0 ~ 1.0,
    "snapshot_version": "<EOD snapshot_version>",  # idempotency key
    "recorded_at": ISO8601 UTC
  }

**중요 규칙**:
- EOD 확정 시점에만 append (intraday noisy flip 금지)
- 동일 (trade_date, strategy_name) 중복 기록 방지
- snapshot_version 까지 동일하면 skip; 다르면 강제 rerun 을 나타내므로
  새 record append (EOD 재실행 flow 에 대응)
- 필드 중 trade_date/regime_label은 필수

집계:
  - observed_regimes_count: 고유 regime_label 수 (UNKNOWN 제외)
  - days_in_{bull,bear,sideways}: 각 레짐 일수
  - regime_flip_count: 연속된 서로 다른 레짐 전환 횟수
  - false_flip_rate: 1일만 유지된 flip 비율 (noise)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("promotion.regime_history")

REGIME_SOURCE_VERSION = "REGIME_V1"
_VALID_REGIMES = {"BULL", "BEAR", "SIDEWAYS", "UNKNOWN"}

# Default 저장 경로
DEFAULT_HISTORY_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "promotion" / "regime_history.jsonl"
)


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def record_regime(
    *,
    trade_date: str,
    strategy_name: str,
    regime_label: str,
    confidence: float = 0.0,
    regime_source_version: str = REGIME_SOURCE_VERSION,
    snapshot_version: str = "",
    history_path: Optional[Path] = None,
) -> bool:
    """EOD 확정 시점에 regime 이력 append.

    Storage: **PG primary** (promotion_regime_history 테이블) / 파일 fallback.
    - DB 사용 가능 시: INSERT ... ON CONFLICT DO NOTHING — idempotent
    - DB 실패 시: 기존 JSONL 파일 경로 (재해 복구용 fallback)
    - ``history_path`` 명시 호출은 파일 모드 강제 (테스트 호환)

    Idempotency:
      - 동일 (trade_date, strategy_name, snapshot_version) 이미 존재 → skip (False)
      - 동일 trade_date + **다른 snapshot_version** → 새 record append (True)
      - snapshot_version 이 비어있는 경우에는 기존 동작 (date+strategy 만으로 dedup)

    Returns: True (appended) / False (skipped or failed)
    """
    if regime_label not in _VALID_REGIMES:
        logger.warning(f"[REGIME_REC] invalid regime: {regime_label}")
        return False

    # 명시 파일 경로가 오면 파일 모드 (pytest 호환)
    if history_path is not None:
        return _record_regime_file(
            trade_date=trade_date,
            strategy_name=strategy_name,
            regime_label=regime_label,
            confidence=confidence,
            regime_source_version=regime_source_version,
            snapshot_version=snapshot_version,
            history_path=history_path,
        )

    # DB primary
    try:
        from . import db as _pdb
        return _pdb.insert_regime_record(
            trade_date=trade_date,
            strategy_name=strategy_name,
            regime_label=regime_label,
            confidence=float(confidence),
            regime_source_version=regime_source_version,
            snapshot_version=snapshot_version,
        )
    except Exception as e:
        logger.warning(f"[REGIME_REC] DB write failed, falling back to file: {e}")
        return _record_regime_file(
            trade_date=trade_date,
            strategy_name=strategy_name,
            regime_label=regime_label,
            confidence=confidence,
            regime_source_version=regime_source_version,
            snapshot_version=snapshot_version,
            history_path=DEFAULT_HISTORY_PATH,
        )


def _record_regime_file(
    *,
    trade_date: str,
    strategy_name: str,
    regime_label: str,
    confidence: float,
    regime_source_version: str,
    snapshot_version: str,
    history_path: Path,
) -> bool:
    """File-based record (DB 실패 또는 테스트 경로)."""
    _ensure_dir(history_path)
    existing = _load_history_file(strategy_name, history_path)
    for rec in existing:
        if rec.get("trade_date") != trade_date:
            continue
        if snapshot_version:
            if rec.get("snapshot_version") == snapshot_version:
                return False
            continue
        return False

    row = {
        "trade_date": trade_date,
        "strategy_name": strategy_name,
        "regime_label": regime_label,
        "regime_source_version": regime_source_version,
        "confidence": float(confidence),
        "snapshot_version": snapshot_version,
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        with history_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return True
    except Exception as e:
        logger.warning(f"[REGIME_REC] file write failed: {e}")
        return False


def load_history(
    strategy_name: Optional[str] = None,
    history_path: Optional[Path] = None,
) -> List[Dict]:
    """전체 또는 특정 전략의 regime 이력 로드.

    Storage: **PG primary** / 파일 fallback. trade_date 오름차순 반환.
    """
    # 명시 파일 경로 → 파일 모드 (테스트 호환)
    if history_path is not None:
        return _load_history_file(strategy_name, history_path)

    # DB primary
    try:
        from . import db as _pdb
        return _pdb.load_regime_records(strategy_name)
    except Exception as e:
        logger.warning(f"[REGIME_REC] DB load failed, falling back to file: {e}")
        return _load_history_file(strategy_name, DEFAULT_HISTORY_PATH)


def _load_history_file(
    strategy_name: Optional[str],
    path: Path,
) -> List[Dict]:
    """File-based load (DB 실패 또는 테스트 경로)."""
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
                if strategy_name is None or row.get("strategy_name") == strategy_name:
                    rows.append(row)
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"[REGIME_REC] file load failed: {e}")
        return []

    rows.sort(key=lambda r: r.get("trade_date", ""))
    return rows


def summarize(strategy_name: str,
              history_path: Optional[Path] = None) -> Dict:
    """단일 전략의 regime 이력 집계.

    Returns:
      {
        "observed_regimes_count": int,   # 고유 regime 개수 (UNKNOWN 제외)
        "regimes_observed": [str, ...],
        "days_in": {"BULL": N, "BEAR": N, "SIDEWAYS": N},
        "regime_flip_count": int,
        "false_flip_rate": float,         # 1일짜리 flip 비율
        "total_days": int,
      }
    """
    rows = load_history(strategy_name, history_path=history_path)
    if not rows:
        return {
            "observed_regimes_count": 0,
            "regimes_observed": [],
            "days_in": {"BULL": 0, "BEAR": 0, "SIDEWAYS": 0},
            "regime_flip_count": 0,
            "false_flip_rate": 0.0,
            "total_days": 0,
        }

    labels = [r.get("regime_label", "UNKNOWN") for r in rows]
    observed = [l for l in labels if l != "UNKNOWN"]

    regimes_set = sorted(set(observed))

    days_in = {"BULL": 0, "BEAR": 0, "SIDEWAYS": 0}
    for l in labels:
        if l in days_in:
            days_in[l] += 1

    # Flip 분석 (연속 동일 레짐 → 1회 flip)
    # segments: [(label, run_length), ...]
    segments: List[tuple] = []
    for l in labels:
        if not segments or segments[-1][0] != l:
            segments.append([l, 1])
        else:
            segments[-1][1] += 1
    # flip_count = segment count − 1 (UNKNOWN 세그먼트 포함)
    flip_count = max(0, len([s for s in segments if s[0] != "UNKNOWN"]) - 1)

    # False flip rate — 1일만 유지된 non-UNKNOWN 세그먼트 비율
    short_segments = sum(1 for s in segments if s[0] != "UNKNOWN" and s[1] == 1)
    total_non_unknown_segments = sum(1 for s in segments if s[0] != "UNKNOWN")
    false_flip_rate = (
        short_segments / total_non_unknown_segments
        if total_non_unknown_segments > 0 else 0.0
    )

    return {
        "observed_regimes_count": len(regimes_set),
        "regimes_observed": regimes_set,
        "days_in": days_in,
        "regime_flip_count": flip_count,
        "false_flip_rate": round(false_flip_rate, 3),
        "total_days": len(rows),
    }


def coverage_from_history(strategy_name: str,
                          history_path: Optional[Path] = None) -> Dict:
    """DataQualityMetrics에 주입할 축약 형태.

    Returns: {regime_coverage, regime_flip_observed, total_days}
      - regime_coverage: Optional[int] — None = history 없음 (UNKNOWN).
        0 = history 있으나 모든 label UNKNOWN.
        >=1 = 관측된 고유 regime 개수.
      - regime_flip_observed: Optional[int] — history 없으면 None.
      - total_days: int — 기록된 EOD 일수.

    **중요**: history 없음 (total_days == 0) 을 coverage=1 등으로 대체하지 말 것.
    """
    s = summarize(strategy_name, history_path=history_path)
    total = s["total_days"]
    if total == 0:
        return {
            "regime_coverage": None,
            "regime_flip_observed": None,
            "total_days": 0,
        }
    return {
        "regime_coverage": s["observed_regimes_count"],
        "regime_flip_observed": s["regime_flip_count"],
        "total_days": total,
    }
