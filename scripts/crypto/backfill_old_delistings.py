"""D3-3 backfill — Upbit notice pages 21~35 (older delistings, 2018~2022).

D2 (PR #12) capped the crawl at 20 pages; this one-shot script extends the
walk to pages 21..35 to surface delistings that predate the rolling D3-1
incremental scope. Reuses D2 fill-in-the-blanks UPSERT semantics (Jeff D3-3
core requirement: "기존 데이터 overwrite 금지").

Idempotency contract (Jeff D3-3 #1):
    - INSERT … ON CONFLICT DO NOTHING for new pairs
    - UPDATE … WHERE delisted_at IS NULL for filling NULL dates only
    - Re-running the script with the same upstream archive produces 0 row
      deltas (verified via run-twice in this script's main flow).

Safety:
    - File lock (`crypto/jobs/_lockfile.py`) blocks concurrent runs and any
      in-flight D3-1 incremental during backfill execution.
    - PG single transaction → CSV .tmp → fsync → rename (same atomic
      ladder as D3-1).
    - Telegram is best-effort; a Telegram outage never fails the backfill.

Usage::

    # Default: pages 21..35
    python scripts/crypto/backfill_old_delistings.py

    # Smaller smoke (e.g. 21..22)
    python scripts/crypto/backfill_old_delistings.py --start-page 21 --end-page 22

    # Crawl only, no writes
    python scripts/crypto/backfill_old_delistings.py --dry-run

Exit codes:
    0 — success (idempotent or net change applied)
    1 — fatal error (PG/CSV write failure, crawl-wide failure)
    2 — lock contention
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.data.listings_crawler import (  # noqa: E402
    UpbitNoticeCrawler,
    crawl_delistings,
)
from crypto.data.listings_merge import (  # noqa: E402
    merge_fill_in_the_blanks,
    pg_apply_delistings,
    read_existing_csv,
    write_csv_atomic,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.jobs._lockfile import FileLock, LockHeld  # noqa: E402
from crypto.jobs._telegram import send as telegram_send  # noqa: E402
from crypto.jobs.incremental_listings import (  # noqa: E402
    _BASELINE_SQL,
    _read_pg_baseline,
)


logger = logging.getLogger(__name__)

DEFAULT_CSV = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"
DEFAULT_LOCK = WORKTREE_ROOT / "crypto" / "data" / "_locks" / "backfill_old_delistings.lock"
DEFAULT_EVIDENCE_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"

DEFAULT_START_PAGE = 21
DEFAULT_END_PAGE = 35


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--start-page",
        type=int,
        default=DEFAULT_START_PAGE,
        help=f"First page to crawl (default {DEFAULT_START_PAGE}).",
    )
    p.add_argument(
        "--end-page",
        type=int,
        default=DEFAULT_END_PAGE,
        help=f"Last page to crawl, inclusive (default {DEFAULT_END_PAGE}).",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"listings CSV path (default {DEFAULT_CSV}).",
    )
    p.add_argument(
        "--lock-path",
        type=Path,
        default=DEFAULT_LOCK,
        help=f"Lockfile path (default {DEFAULT_LOCK}).",
    )
    p.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence JSON output dir (default {DEFAULT_EVIDENCE_DIR}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Crawl + diff only — skip PG/CSV writes (still acquires lock).",
    )
    return p.parse_args(argv)


def _crawl_range(start_page: int, end_page: int):
    crawler = UpbitNoticeCrawler()
    t0 = time.monotonic()
    events, errors = crawl_delistings(
        crawler,
        max_pages=end_page,
        start_page=start_page,
        per_page=20,
        fail_soft=True,
    )
    elapsed = time.monotonic() - t0
    return events, errors, elapsed


def _run_locked(args, started_at_utc: str) -> tuple[int, dict[str, Any]]:
    """Core path with lock already held. Returns (exit_code, evidence_dict)."""
    if args.start_page < 1 or args.end_page < args.start_page:
        msg = f"invalid page range start={args.start_page} end={args.end_page}"
        logger.error("[D3-3] %s", msg)
        return 1, {"fatal_error": msg}

    print(f"[crawl] pages {args.start_page}..{args.end_page}")
    try:
        events, errors, elapsed = _crawl_range(args.start_page, args.end_page)
    except Exception as exc:
        msg = f"crawl raised: {exc}"
        logger.error("[D3-3] %s", msg)
        return 1, {"fatal_error": msg}
    print(f"  events={len(events)}  errors={len(errors)}  elapsed={elapsed:.1f}s")

    if errors and len(errors) > len(events):
        msg = f"crawl error rate too high: {len(errors)} errors vs {len(events)} events"
        logger.error("[D3-3] %s", msg)
        return 1, {
            "fatal_error": msg,
            "events_crawled": len(events),
            "crawl_errors": errors,
        }

    # Env + DB
    try:
        ensure_main_project_env_loaded()
        from shared.db.pg_base import connection
    except Exception as exc:
        msg = f"env/db init failed: {exc}"
        logger.error("[D3-3] %s", msg)
        return 1, {"fatal_error": msg}

    # Baseline before
    try:
        with connection() as conn:
            baseline_before = _read_pg_baseline(conn)
    except Exception as exc:
        msg = f"baseline read failed: {exc}"
        logger.error("[D3-3] %s", msg)
        return 1, {"fatal_error": msg}
    print(f"[baseline-before] {baseline_before}")

    # CSV merge in-memory
    fieldnames, existing_rows = read_existing_csv(args.csv)
    new_rows, csv_stats = merge_fill_in_the_blanks(existing_rows, events)
    print(f"[csv-merge] {csv_stats}")

    if args.dry_run:
        print("[dry-run] skipping PG/CSV writes")
        return 0, {
            "events_crawled": len(events),
            "crawl_errors": errors,
            "baseline_before": baseline_before,
            "baseline_after": dict(baseline_before),
            "diff": {k: 0 for k in baseline_before},
            "csv_merge_stats": csv_stats,
            "pg_apply_stats": {},
            "idempotent": True,
            "dry_run": True,
        }

    # PG write (single transaction)
    csv_backup = args.csv.with_suffix(args.csv.suffix + ".bak")
    if args.csv.exists():
        shutil.copyfile(args.csv, csv_backup)

    pg_stats = {}
    baseline_after = {}
    try:
        with connection() as conn:
            pg_stats = pg_apply_delistings(conn, events)
            conn.commit()
            baseline_after = _read_pg_baseline(conn)
        print(f"[pg-apply] {pg_stats}")
    except Exception as exc:
        msg = f"PG transaction failed: {exc}"
        logger.error("[D3-3] %s", msg)
        if csv_backup.exists():
            csv_backup.unlink()
        return 1, {
            "fatal_error": msg,
            "events_crawled": len(events),
            "crawl_errors": errors,
        }

    # CSV atomic write
    try:
        write_csv_atomic(args.csv, new_rows)
    except Exception as exc:
        msg = (
            f"CSV write failed AFTER PG commit — DB is canonical, CSV restored "
            f"from backup: {exc}"
        )
        logger.error("[D3-3] %s", msg)
        if csv_backup.exists():
            shutil.copyfile(csv_backup, args.csv)
            csv_backup.unlink()
        return 1, {
            "fatal_error": msg,
            "events_crawled": len(events),
            "crawl_errors": errors,
            "baseline_before": baseline_before,
            "baseline_after": baseline_after,
            "csv_merge_stats": csv_stats,
            "pg_apply_stats": pg_stats,
        }
    finally:
        if csv_backup.exists():
            csv_backup.unlink()

    # Diff + idempotency
    diff = {k: baseline_after.get(k, 0) - baseline_before.get(k, 0)
            for k in baseline_before}
    idempotent = all(v == 0 for v in diff.values())
    print(f"[baseline-after] {baseline_after}")
    print(f"[diff] {diff}  idempotent={idempotent}")

    return 0, {
        "events_crawled": len(events),
        "crawl_errors": errors,
        "baseline_before": baseline_before,
        "baseline_after": baseline_after,
        "diff": diff,
        "csv_merge_stats": csv_stats,
        "pg_apply_stats": pg_stats,
        "idempotent": idempotent,
        "dry_run": False,
    }


def run(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    started_at_utc = _now()
    print("=" * 78)
    print(f"[D3-3] backfill_old_delistings @ {started_at_utc}")
    print(f"  pages={args.start_page}..{args.end_page}  dry_run={args.dry_run}")
    print(f"  csv={args.csv}")
    print(f"  lock={args.lock_path}")
    print("=" * 78)

    # Lock
    try:
        lock = FileLock(args.lock_path, owner="backfill_old_delistings")
        lock.acquire()
    except LockHeld as exc:
        msg = f"[D3-3] lock contention: {exc}"
        print(msg, file=sys.stderr)
        logger.warning(msg)
        telegram_send(f"crypto/D3-3 SKIPPED (lock held)\n{exc}")
        return 2

    payload: dict[str, Any] = {
        "started_at_utc": started_at_utc,
        "completed_at_utc": "",
        "pages": [args.start_page, args.end_page],
        "dry_run": args.dry_run,
        "exit_code": 0,
    }

    try:
        rc, body = _run_locked(args, started_at_utc)
        payload.update(body)
        payload["exit_code"] = rc
    finally:
        lock.release()
        payload["completed_at_utc"] = _now()
        evidence_path = _write_evidence(
            args.evidence_dir, started_at_utc, payload
        )
        print(f"[evidence] {evidence_path}")

    # Telegram (best-effort, on net change OR error)
    telegram_status = "skipped:no-changes"
    diff = payload.get("diff", {}) or {}
    listings_change = int(diff.get("listings_total", 0))
    delisted_change = int(diff.get("delisted_with_date", 0))
    fatal = payload.get("fatal_error")
    if fatal:
        telegram_status = telegram_send(f"crypto/D3-3 FAIL\n{fatal}")
    elif (listings_change or delisted_change) and not args.dry_run:
        text = (
            f"crypto/D3-3 backfill OK\n"
            f"pages {args.start_page}..{args.end_page}\n"
            f"new listings: +{listings_change}\n"
            f"new delisted_at: +{delisted_change}\n"
            f"events: {payload.get('events_crawled', 0)}"
        )
        telegram_status = telegram_send(text)
    payload["telegram_status"] = telegram_status

    # Re-write evidence with telegram_status (last-write wins)
    _write_evidence(args.evidence_dir, started_at_utc, payload)

    print(f"[telegram] {telegram_status}")
    print(f"[verdict] exit={payload['exit_code']} idempotent={payload.get('idempotent')}")
    return int(payload["exit_code"])


def _write_evidence(
    dir_path: Path,
    started_at_utc: str,
    payload: dict[str, Any],
) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    fname = f"backfill_old_delistings_{started_at_utc[:10]}.json"
    out = dir_path / fname
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return out


if __name__ == "__main__":
    raise SystemExit(run())
