"""D3-1 daily incremental listings job (Crypto Lab).

Crawls the first ``--max-pages`` of Upbit's notice list (default 3 — recent
only), applies fill-in-the-blanks UPSERT to PG + listings.csv inside a single
transaction + atomic CSV rename, and emits a JSON evidence file. Designed for
Windows Task Scheduler at UTC 00:30 / KST 09:30 (Jeff D3 Q5).

Safety contract (Jeff D3 보완):
    1. Idempotency — re-running with no upstream changes leaves PG row counts
       and CSV bytes unchanged. UPSERT uses ``ON CONFLICT DO NOTHING`` and
       fill is gated on ``delisted_at IS NULL``.
    2. Lockfile — ``crypto/data/_locks/incremental_listings.lock`` blocks
       concurrent runs (FileLock with stale auto-reclaim).
    3. Partial-write forbidden — PG transaction commits before CSV rename;
       any failure mid-write rolls back PG and restores CSV from backup.
    4. Drift report stub — counts are emitted in the evidence JSON. Full DB↔CSV
       reconcile lives in PR #2.
    5. Telegram best-effort — logger always wins; Telegram failure never
       impacts job exit code.
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from crypto.data.listings_crawler import (
    UpbitNoticeCrawler,
    crawl_delistings,
)
from crypto.data.listings_merge import (
    merge_fill_in_the_blanks,
    pg_apply_delistings,
    read_existing_csv,
    write_csv_atomic,
)
from crypto.db.env import ensure_main_project_env_loaded
from crypto.jobs._lockfile import FileLock, LockHeld
from crypto.jobs._telegram import send as telegram_send

logger = logging.getLogger(__name__)


# --- Paths --------------------------------------------------------------

# Resolve worktree root from this file's location: crypto/jobs/this.py
_HERE = Path(__file__).resolve()
WORKTREE_ROOT = _HERE.parents[2]
DEFAULT_CSV = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"
DEFAULT_LOCK = WORKTREE_ROOT / "crypto" / "data" / "_locks" / "incremental_listings.lock"
DEFAULT_EVIDENCE_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"

DEFAULT_MAX_PAGES = 3  # incremental scope — D2 backfill covered pages 1..20
TELEGRAM_NOTIFY_THRESHOLD_CHANGES = 1  # any net change → notify
TELEGRAM_NOTIFY_ON_ERROR = True


# --- Result types -------------------------------------------------------


@dataclass
class IncrementalResult:
    started_at_utc: str
    completed_at_utc: str
    max_pages: int
    events_crawled: int
    crawl_errors: list[dict[str, Any]] = field(default_factory=list)
    baseline_before: dict[str, int] = field(default_factory=dict)
    baseline_after: dict[str, int] = field(default_factory=dict)
    diff: dict[str, int] = field(default_factory=dict)
    csv_merge_stats: dict[str, int] = field(default_factory=dict)
    pg_apply_stats: dict[str, int] = field(default_factory=dict)
    idempotent: bool = False
    telegram_status: str = "not-attempted"
    fatal_error: Optional[str] = None
    exit_code: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "max_pages": self.max_pages,
            "events_crawled": self.events_crawled,
            "crawl_errors": self.crawl_errors,
            "baseline_before": self.baseline_before,
            "baseline_after": self.baseline_after,
            "diff": self.diff,
            "csv_merge_stats": self.csv_merge_stats,
            "pg_apply_stats": self.pg_apply_stats,
            "idempotent": self.idempotent,
            "telegram_status": self.telegram_status,
            "fatal_error": self.fatal_error,
            "exit_code": self.exit_code,
        }


# --- Baseline / diff ----------------------------------------------------


_BASELINE_SQL = {
    "listings_total":           "SELECT COUNT(*) FROM crypto_listings",
    "delisted_with_date":       "SELECT COUNT(*) FROM crypto_listings WHERE delisted_at IS NOT NULL",
    "source_upbit_notice":      "SELECT COUNT(*) FROM crypto_listings WHERE source = 'upbit_notice'",
    "source_manual_v0":         "SELECT COUNT(*) FROM crypto_listings WHERE source = 'manual_v0'",
}


def _read_pg_baseline(conn) -> dict[str, int]:
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        for key, sql in _BASELINE_SQL.items():
            cur.execute(sql)
            row = cur.fetchone()
            out[key] = int(row[0]) if row else 0
    return out


def _diff_baselines(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    return {k: after.get(k, 0) - before.get(k, 0) for k in before}


# --- Args ---------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Notice pages to crawl (default {DEFAULT_MAX_PAGES} — recent only).",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"listings.csv path (default {DEFAULT_CSV}).",
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


# --- Core run -----------------------------------------------------------


def run(argv: Optional[list[str]] = None) -> int:
    """Daily incremental entry point. Returns shell exit code.

    Exit codes:
        0  — success (idempotent or net change applied)
        1  — fatal error (PG/CSV write failure, crawl-wide failure)
        2  — lock contention (another instance is already running)
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    started_at_utc = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    print("=" * 78)
    print(f"[D3-1] crypto.jobs.incremental_listings @ {started_at_utc}")
    print(f"  max_pages={args.max_pages}  dry_run={args.dry_run}")
    print(f"  csv={args.csv}")
    print(f"  lock={args.lock_path}")
    print("=" * 78)

    # --- Lock first (Jeff D3 #2) ---
    try:
        lock = FileLock(args.lock_path, owner="incremental_listings")
        lock.acquire()
    except LockHeld as exc:
        msg = f"[D3-1] lock contention — another run is in progress: {exc}"
        print(msg, file=sys.stderr)
        logger.warning(msg)
        # Telegram opt-in: lock contention is a recurring-job concern.
        telegram_send(f"crypto/D3-1 SKIPPED (lock held)\n{exc}")
        return 2

    result = IncrementalResult(
        started_at_utc=started_at_utc,
        completed_at_utc="",
        max_pages=args.max_pages,
        events_crawled=0,
    )

    try:
        return _run_locked(args, result)
    finally:
        lock.release()
        # Always persist evidence — even on fatal error — so the next run
        # has context.
        result.completed_at_utc = (
            datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        )
        try:
            _write_evidence(args.evidence_dir, started_at_utc, result)
        except Exception as exc:  # pragma: no cover (defensive)
            logger.error("[D3-1] failed to write evidence: %s", exc)


def _run_locked(args: argparse.Namespace, result: IncrementalResult) -> int:
    """Core path with lock already held."""
    # Env + DB connection (deferred so missing creds don't bubble up before
    # we've acquired the lock).
    try:
        ensure_main_project_env_loaded()
        from shared.db.pg_base import connection
    except Exception as exc:
        msg = f"env/db init failed: {exc}"
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[fail] {msg}", file=sys.stderr)
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1

    # 1) Crawl
    print(f"[crawl] pages 1..{args.max_pages}")
    crawler = UpbitNoticeCrawler()
    t0 = time.monotonic()
    try:
        events, errors = crawl_delistings(
            crawler, max_pages=args.max_pages, per_page=20, fail_soft=True
        )
    except Exception as exc:
        msg = f"crawl_delistings raised: {exc}"
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[fail] {msg}", file=sys.stderr)
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1
    elapsed = time.monotonic() - t0
    result.events_crawled = len(events)
    result.crawl_errors = errors
    print(f"  events={len(events)}  errors={len(errors)}  elapsed={elapsed:.1f}s")

    if errors and len(errors) > len(events):
        # More errors than successful events → treat as crawl-wide failure.
        msg = f"crawl error rate too high: {len(errors)} errors vs {len(events)} events"
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[fail] {msg}", file=sys.stderr)
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1

    # 2) Baseline before
    try:
        with connection() as conn:
            result.baseline_before = _read_pg_baseline(conn)
    except Exception as exc:
        msg = f"baseline read failed: {exc}"
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[fail] {msg}", file=sys.stderr)
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1
    print(f"[baseline-before] {result.baseline_before}")

    # 3) Compute CSV merge in-memory
    fieldnames, existing_rows = read_existing_csv(args.csv)
    new_rows, csv_stats = merge_fill_in_the_blanks(existing_rows, events)
    result.csv_merge_stats = csv_stats
    print(f"[csv-merge] {csv_stats}")

    if args.dry_run:
        print("[dry-run] skipping PG/CSV writes")
        result.baseline_after = dict(result.baseline_before)
        result.diff = _diff_baselines(result.baseline_before, result.baseline_after)
        result.idempotent = True
        result.exit_code = 0
        return 0

    # 4) PG write (single transaction)
    csv_backup = args.csv.with_suffix(args.csv.suffix + ".bak")
    if args.csv.exists():
        shutil.copyfile(args.csv, csv_backup)

    try:
        with connection() as conn:
            pg_stats = pg_apply_delistings(conn, events)
            conn.commit()
            # Read post-commit baseline on the same connection for consistency.
            result.baseline_after = _read_pg_baseline(conn)
        result.pg_apply_stats = pg_stats
        print(f"[pg-apply] {pg_stats}")
    except Exception as exc:
        msg = f"PG transaction failed: {exc}"
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[fail] {msg}", file=sys.stderr)
        # CSV is untouched — backup was created but we have not written.
        if csv_backup.exists():
            csv_backup.unlink()
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1

    # 5) CSV atomic write
    try:
        write_csv_atomic(args.csv, new_rows)
    except Exception as exc:
        msg = (
            f"CSV write failed AFTER PG commit — DB is canonical, CSV restored "
            f"from backup: {exc}"
        )
        result.fatal_error = msg
        result.exit_code = 1
        print(f"[warn] {msg}", file=sys.stderr)
        if csv_backup.exists():
            shutil.copyfile(csv_backup, args.csv)
        if TELEGRAM_NOTIFY_ON_ERROR:
            result.telegram_status = telegram_send(f"crypto/D3-1 FAIL\n{msg}")
        return 1
    finally:
        if csv_backup.exists():
            csv_backup.unlink()

    # 6) Diff + idempotency check
    result.diff = _diff_baselines(result.baseline_before, result.baseline_after)
    net_change = sum(abs(v) for v in result.diff.values())
    result.idempotent = (net_change == 0)
    print(f"[baseline-after] {result.baseline_after}")
    print(f"[diff] {result.diff}  idempotent={result.idempotent}")

    # 7) Telegram (best-effort, only on net change)
    listings_change = result.diff.get("listings_total", 0)
    delisted_change = result.diff.get("delisted_with_date", 0)
    if listings_change >= TELEGRAM_NOTIFY_THRESHOLD_CHANGES or delisted_change >= TELEGRAM_NOTIFY_THRESHOLD_CHANGES:
        text = (
            f"crypto/D3-1 OK\n"
            f"new listings: +{listings_change}\n"
            f"new delisted_at: +{delisted_change}\n"
            f"events_crawled: {result.events_crawled}"
        )
        result.telegram_status = telegram_send(text)
    else:
        result.telegram_status = "skipped:no-changes"

    result.exit_code = 0
    return 0


def _write_evidence(
    dir_path: Path,
    started_at_utc: str,
    result: IncrementalResult,
) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    # Evidence filename uses UTC date — multiple runs in the same day overwrite
    # (latest wins), which matches Jeff's "daily incremental" cadence.
    fname = f"incremental_listings_{started_at_utc[:10]}.json"
    out = dir_path / fname
    out.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"[evidence] {out}")
    return out


if __name__ == "__main__":
    raise SystemExit(run())
