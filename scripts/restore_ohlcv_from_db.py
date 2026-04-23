#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/restore_ohlcv_from_db.py — OHLCV CSV restoration from PostgreSQL.

Rebuilds `backtest/data_full/ohlcv/{code}.csv` from the authoritative
`ohlcv` table in PostgreSQL. Designed to recover from CSV truncation
incidents (see RCA 20260423) in ~15 seconds.

Context:
    On 2026-04-22 batch failed with "Empty universe" because CSVs had
    only 30 days of history (all filtered out by min_history=260).
    DB still had full 2019~2026-04-22 history for 2770 codes.
    This script is the permanent recovery tool.

Usage:
    # Verification (no writes):
    python scripts/restore_ohlcv_from_db.py --dry-run --verify-universe

    # Full restore with backup:
    python scripts/restore_ohlcv_from_db.py --backup-existing --verify-universe

    # Sample mode (specific codes only):
    python scripts/restore_ohlcv_from_db.py --codes 005930,000660 --dry-run

Safety:
    - --dry-run: read from DB, compute targets, but no file writes
    - --backup-existing: copy current CSVs to backup/ohlcv_snapshot_{date}/
      BEFORE overwriting
    - Atomic writes: tmp file → os.replace (no half-written CSVs)
    - --force required to overwrite without --backup-existing

Exit codes:
    0 — success (or dry-run completed)
    1 — invalid arguments
    2 — DB connection failed
    3 — verification failed (universe below threshold after restore)
    4 — other error
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

# Bootstrap path so `shared.*` is importable regardless of cwd
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

logger = logging.getLogger("qtron.restore_ohlcv")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore OHLCV CSVs from PostgreSQL (see RCA 20260423).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # dry-run preview\n"
            "  python scripts/restore_ohlcv_from_db.py --dry-run --verify-universe\n\n"
            "  # full restore with backup\n"
            "  python scripts/restore_ohlcv_from_db.py --backup-existing --verify-universe\n\n"
            "  # sample mode\n"
            "  python scripts/restore_ohlcv_from_db.py --codes 005930,000660 --dry-run\n"
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Query DB + report what would be written. No file changes.",
    )
    parser.add_argument(
        "--backup-existing", action="store_true",
        help="Copy current CSVs to backup/ohlcv_snapshot_{date}/ before overwriting.",
    )
    parser.add_argument(
        "--codes", type=str, default=None,
        help="Comma-separated codes to restore (sample mode). Default: all codes.",
    )
    parser.add_argument(
        "--verify-universe", action="store_true",
        help="After restore, run build_universe_from_ohlcv and print count.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Allow overwriting without --backup-existing (dangerous).",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Override output dir. Default: <repo>/backtest/data_full/ohlcv/",
    )
    return parser.parse_args(argv)


def _resolve_output_dir(override: str | None) -> Path:
    if override:
        return Path(override).resolve()
    return (_REPO_ROOT / "backtest" / "data_full" / "ohlcv").resolve()


def _backup_existing(out_dir: Path) -> Path | None:
    """Copy current CSVs to backup/ohlcv_snapshot_{YYYYMMDD_HHMM}/.

    Returns backup path, or None if no CSVs to backup.
    """
    csvs = list(out_dir.glob("*.csv"))
    if not csvs:
        logger.info("[BACKUP] no existing CSVs in %s — skipping backup", out_dir)
        return None

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    backup_dir = _REPO_ROOT / "backup" / f"ohlcv_snapshot_{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for src in csvs:
        dst = backup_dir / src.name
        try:
            shutil.copy2(src, dst)
            copied += 1
        except OSError as e:
            logger.warning("[BACKUP_SKIP] %s: %s", src.name, e)

    logger.info(
        "[BACKUP] copied %d/%d CSVs → %s", copied, len(csvs), backup_dir,
    )
    return backup_dir


def _write_csv_atomic(
    path: Path, df: "pd.DataFrame",  # noqa: F821 — pandas imported lazily
) -> None:
    """Write DataFrame to CSV atomically (tmp → os.replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            df[["date", "open", "high", "low", "close", "volume"]].to_csv(
                f, index=False, date_format="%Y-%m-%d",
            )
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _verify_universe(ohlcv_dir: Path) -> tuple[int, dict]:
    """Run universe_builder and return (count, filter_stats)."""
    # Lazy import to avoid heavy deps unless requested
    from config import Gen4Config  # noqa: WPS433
    from data.universe_builder import build_universe_from_ohlcv  # noqa: WPS433

    cfg = Gen4Config()
    universe = build_universe_from_ohlcv(
        ohlcv_dir,
        min_close=cfg.UNIV_MIN_CLOSE,
        min_amount=cfg.UNIV_MIN_AMOUNT,
        min_history=cfg.UNIV_MIN_HISTORY,
        min_count=cfg.UNIV_MIN_COUNT,
        allowed_markets=cfg.MARKETS,
        sector_map={},
    )
    return len(universe), {"min_count": cfg.UNIV_MIN_COUNT}


def _query_ohlcv(codes_filter: list[str] | None):
    """Return DataFrame of ohlcv rows. Codes_filter None = all."""
    import pandas as pd  # noqa: WPS433
    from shared.db.pg_base import connection  # noqa: WPS433

    with connection() as conn:
        if codes_filter:
            placeholders = ",".join(["%s"] * len(codes_filter))
            query = (
                f"SELECT code, date, open, high, low, close, volume "
                f"FROM ohlcv WHERE code IN ({placeholders}) ORDER BY code, date"
            )
            df = pd.read_sql(query, conn, params=tuple(codes_filter))
        else:
            df = pd.read_sql(
                "SELECT code, date, open, high, low, close, volume "
                "FROM ohlcv ORDER BY code, date",
                conn,
            )
    return df


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Sanity: if not dry-run and not backup-existing, require --force
    if not args.dry_run and not args.backup_existing and not args.force:
        logger.error(
            "[SAFETY] overwriting CSVs without --backup-existing requires --force. "
            "Use --dry-run to preview, or --backup-existing to be safe."
        )
        return 1

    out_dir = _resolve_output_dir(args.output_dir)
    logger.info("[CONFIG] output_dir=%s dry_run=%s backup=%s verify=%s",
                out_dir, args.dry_run, args.backup_existing, args.verify_universe)

    codes_filter: list[str] | None = None
    if args.codes:
        codes_filter = [c.strip() for c in args.codes.split(",") if c.strip()]
        logger.info("[CODES] restricted to %d codes: %s",
                    len(codes_filter), codes_filter)

    # ---- DB query ----
    t0 = datetime.now()
    try:
        df = _query_ohlcv(codes_filter)
    except Exception as e:  # noqa: BLE001
        logger.error("[DB_FAIL] %r", e)
        return 2

    if df.empty:
        logger.error("[DB_EMPTY] ohlcv table returned 0 rows for filter=%s",
                     codes_filter)
        return 2

    elapsed_query = (datetime.now() - t0).total_seconds()
    unique_codes = df["code"].nunique()
    date_min = df["date"].min()
    date_max = df["date"].max()
    logger.info(
        "[DB_LOADED] rows=%d codes=%d date=%s~%s elapsed=%.1fs",
        len(df), unique_codes, date_min, date_max, elapsed_query,
    )

    # ---- Dry run — report only ----
    if args.dry_run:
        logger.info(
            "[DRY_RUN] would write %d CSVs to %s. "
            "Run without --dry-run to apply.",
            unique_codes, out_dir,
        )
        if args.verify_universe:
            existing_count = len(list(out_dir.glob("*.csv")))
            if existing_count > 0:
                try:
                    u_count, u_meta = _verify_universe(out_dir)
                    logger.info(
                        "[VERIFY_CURRENT] current universe_count=%d (min=%d) "
                        "from existing %d CSVs",
                        u_count, u_meta["min_count"], existing_count,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("[VERIFY_CURRENT_FAIL] %r", e)
        return 0

    # ---- Backup existing ----
    if args.backup_existing:
        _backup_existing(out_dir)

    # ---- Write CSVs per code ----
    out_dir.mkdir(parents=True, exist_ok=True)
    t1 = datetime.now()
    written = 0
    failed = 0
    for code, group in df.groupby("code", sort=False):
        target = out_dir / f"{code}.csv"
        try:
            _write_csv_atomic(target, group)
            written += 1
        except Exception as e:  # noqa: BLE001
            logger.error("[WRITE_FAIL] %s: %r", code, e)
            failed += 1

        if written % 500 == 0 and written > 0:
            logger.info("[PROGRESS] written %d/%d", written, unique_codes)

    elapsed_write = (datetime.now() - t1).total_seconds()
    logger.info(
        "[WRITE_DONE] written=%d failed=%d elapsed=%.1fs total=%.1fs",
        written, failed, elapsed_write,
        (datetime.now() - t0).total_seconds(),
    )

    if failed > 0:
        logger.warning("[PARTIAL_FAIL] %d codes failed to write", failed)

    # ---- Verify universe ----
    if args.verify_universe:
        try:
            u_count, u_meta = _verify_universe(out_dir)
            if u_count < u_meta["min_count"]:
                logger.error(
                    "[VERIFY_FAIL] universe_count=%d < min=%d",
                    u_count, u_meta["min_count"],
                )
                return 3
            logger.info(
                "[VERIFY_OK] universe_count=%d (min=%d)",
                u_count, u_meta["min_count"],
            )
        except Exception as e:  # noqa: BLE001
            logger.error("[VERIFY_CRASH] %r", e)
            return 4

    return 0


if __name__ == "__main__":
    sys.exit(main())
