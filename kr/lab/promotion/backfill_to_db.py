"""
backfill_to_db.py — One-shot JSONL → PG 백필 유틸리티
======================================================
Phase C 전환 후, 기존에 축적된 JSONL/JSON 파일 데이터를 PG로 이전한다.

대상 파일:
  - kr/data/promotion/regime_history.jsonl  → promotion_regime_history
  - kr/data/promotion/transition_log.jsonl  → promotion_transition_log
  - kr/data/ops/ops_metrics.json            → promotion_ops_snapshot

실행:
  .venv/Scripts/python.exe -m lab.promotion.backfill_to_db [--dry-run]

멱등성:
  - regime: (trade_date, strategy, snapshot_version) UNIQUE → 중복은 ON CONFLICT skip
  - transition: append only. 중복 row 생성 가능성 있음 (evaluated_at 기준으로 구분).
    **run 한 번만 권장**. run 후 JSONL 파일은 archive/로 이동 권장.
  - ops_snapshot: 현재 file 에 있는 field 를 PG 에 UPSERT (기존 DB 값은 덮어씀)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger("promotion.backfill")


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception as e:
            logger.warning(f"[BACKFILL] parse err: {e}")


def backfill_regime_history(dry_run: bool = False) -> int:
    """regime_history.jsonl → promotion_regime_history."""
    from lab.promotion.regime_history import DEFAULT_HISTORY_PATH
    from lab.promotion import db as _pdb

    count = 0
    for row in _iter_jsonl(DEFAULT_HISTORY_PATH):
        if dry_run:
            count += 1
            continue
        try:
            ok = _pdb.insert_regime_record(
                trade_date=row["trade_date"],
                strategy_name=row["strategy_name"],
                regime_label=row.get("regime_label", "UNKNOWN"),
                confidence=float(row.get("confidence", 0.0) or 0.0),
                regime_source_version=row.get("regime_source_version", "REGIME_V1"),
                snapshot_version=row.get("snapshot_version", ""),
            )
            if ok:
                count += 1
        except Exception as e:
            logger.warning(f"[BACKFILL_REGIME] skip row: {e}")
    return count


def backfill_transition_log(dry_run: bool = False) -> int:
    """transition_log.jsonl → promotion_transition_log.

    **주의**: append-only라 재실행 시 중복 위험. 한 번만 실행하고 파일 archive 권장.
    """
    from lab.promotion.transition_log import DEFAULT_LOG_PATH
    from lab.promotion import db as _pdb

    count = 0
    for row in _iter_jsonl(DEFAULT_LOG_PATH):
        if dry_run:
            count += 1
            continue
        try:
            _pdb.insert_transition(
                strategy=row["strategy"],
                old_status=row.get("old_status"),
                new_status=row["new_status"],
                reason=row.get("reason", ""),
                blockers=list(row.get("blockers", []) or []),
                score=row.get("score"),
                versions=dict(row.get("versions", {}) or {}),
            )
            count += 1
        except Exception as e:
            logger.warning(f"[BACKFILL_TRANSITION] skip row: {e}")
    return count


def backfill_ops_snapshot(dry_run: bool = False) -> int:
    """ops_metrics.json → promotion_ops_snapshot (UPSERT)."""
    from runtime.ops_metrics import DEFAULT_SNAPSHOT_PATH
    from lab.promotion import db as _pdb

    if not DEFAULT_SNAPSHOT_PATH.exists():
        return 0
    try:
        data = json.loads(DEFAULT_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[BACKFILL_OPS] parse err: {e}")
        return 0

    count = 0
    for field_name, ev in data.items():
        if field_name.startswith("_"):
            continue
        if not isinstance(ev, dict):
            continue
        if dry_run:
            count += 1
            continue
        try:
            _pdb.upsert_ops_field(
                field_name=field_name,
                value=ev.get("value"),
                source=str(ev.get("source", "unknown")),
                window=str(ev.get("window", "session")),
                ts=str(ev.get("ts") or data.get("_write_ts") or ""),
                write_origin="backfill",
            )
            count += 1
        except Exception as e:
            logger.warning(f"[BACKFILL_OPS] field={field_name}: {e}")
    return count


def main():
    # Bootstrap path so imports work
    kr_root = Path(__file__).resolve().parent.parent.parent
    project_root = kr_root.parent
    for p in (str(kr_root), str(project_root)):
        if p not in sys.path:
            sys.path.insert(0, p)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Backfill promotion JSONL/JSON → PG")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count rows without writing to DB")
    parser.add_argument("--skip-transition", action="store_true",
                        help="Skip transition log (append-only, dup risk)")
    args = parser.parse_args()

    logger.info(f"=== Promotion Backfill {'(DRY RUN)' if args.dry_run else ''} ===")

    n1 = backfill_regime_history(dry_run=args.dry_run)
    logger.info(f"regime_history: {n1} rows {'would be' if args.dry_run else ''} inserted")

    if args.skip_transition:
        logger.info("transition_log: SKIPPED")
    else:
        n2 = backfill_transition_log(dry_run=args.dry_run)
        logger.info(f"transition_log: {n2} rows {'would be' if args.dry_run else ''} inserted")

    n3 = backfill_ops_snapshot(dry_run=args.dry_run)
    logger.info(f"ops_snapshot:   {n3} fields {'would be' if args.dry_run else ''} upserted")

    logger.info("=== Done ===")


if __name__ == "__main__":
    main()
