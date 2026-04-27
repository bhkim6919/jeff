"""S7 driver: sync existing crypto_ohlcv rows from PG → per-pair parquet files.

Per Jeff S7 instruction (2026-04-27):
    - PG = canonical truth, parquet = cache/fallback.
    - Atomic write (DESIGN.md §4.4): tmp → fsync → rename.
    - PG ↔ parquet row count mismatch = 0.
    - PG ↔ parquet checksum mismatch = 0.
    - tmp parquet residual = 0 after run.
    - Touch zero kr/us code or tables.

Design choice (S7 vs Phase 2):
    S6 already populated PG with 100 pairs (83 910 rows). Re-fetching from
    Upbit just to write parquet would waste API quota and add 80+ seconds.
    This script reads from PG and writes to parquet — the parity check that
    follows (verify_pg_parquet_parity.py) proves the two stores match.

    For future incremental fetches (Phase 2), the dual-write path lives in
    bulk_fetch_d1.py via the same parquet_io primitives.

Usage (worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/sync_pg_to_parquet.py \
        --pairs-from crypto/data/universe_top100.csv \
        --limit 2     # 2-pair smoke test (BTC + ZBT per universe order)

    # Full run:
    --pairs-from crypto/data/universe_top100.csv

    # Or single-pair:
    --pair KRW-BTC
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.db.parquet_io import (  # noqa: E402
    aggregate_checksum_from_parquet,
    count_rows_parquet,
    list_tmp_residuals,
    write_pair_parquet_atomic,
)
from crypto.db.repository import (  # noqa: E402
    aggregate_checksum_for_pair,
    count_rows_for_pair,
    read_pair_rows_from_pg,
)


PARQUET_OUT_DIR = WORKTREE_ROOT / "crypto" / "data" / "ohlcv"
DEFAULT_UNIVERSE_PATH = WORKTREE_ROOT / "crypto" / "data" / "universe_top100.csv"


def _load_pairs_from_csv(path: Path) -> list[str]:
    pairs: list[str] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p = row.get("pair", "").strip()
            if p:
                pairs.append(p)
    return pairs


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--pair", help="Single market e.g. KRW-BTC")
    src.add_argument(
        "--pairs-from",
        type=Path,
        help=f"CSV with a 'pair' column. e.g. {DEFAULT_UNIVERSE_PATH}",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="When using --pairs-from, sync only the first N pairs.",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=PARQUET_OUT_DIR,
        help=f"Parquet output directory. Default: {PARQUET_OUT_DIR}",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.pair:
        pairs = [args.pair]
    else:
        if not args.pairs_from.exists():
            print(f"[fail] CSV not found: {args.pairs_from}", file=sys.stderr)
            return 1
        pairs = _load_pairs_from_csv(args.pairs_from)
        if args.limit is not None:
            pairs = pairs[: args.limit]

    print("=" * 78)
    print("S7 sync_pg_to_parquet — PG (canonical) → parquet (cache)")
    print("=" * 78)
    print(f"pairs   : {len(pairs)} ({pairs[:5]}{'...' if len(pairs)>5 else ''})")
    print(f"out_dir : {args.out_dir}")
    print()

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection  # noqa: E402

    started = time.monotonic()
    fetched = 0
    skipped_empty = 0
    failed = 0

    with connection() as conn:
        for i, pair in enumerate(pairs, start=1):
            print(f"[{i:>3}/{len(pairs)}] {pair}")
            try:
                rows = read_pair_rows_from_pg(conn, pair)
                if not rows:
                    print(f"  [skip]  no rows in PG for {pair}")
                    skipped_empty += 1
                    continue

                pg_count = count_rows_for_pair(conn, pair)
                pg_checksum = aggregate_checksum_for_pair(conn, pair)

                # rows already sorted by candle_dt_kst per repository.read_*
                path = write_pair_parquet_atomic(pair, rows, args.out_dir)

                pq_count = count_rows_parquet(path)
                pq_checksum = aggregate_checksum_from_parquet(path)

                row_match = pg_count == pq_count
                chk_match = pg_checksum == pq_checksum
                tag = "ok" if (row_match and chk_match) else "FAIL"
                print(
                    f"  [{tag}]  rows: pg={pg_count} pq={pq_count} "
                    f"(match={row_match})  "
                    f"chk: pg={pg_checksum[:12]}… pq={pq_checksum[:12]}… "
                    f"(match={chk_match})"
                )
                if not (row_match and chk_match):
                    failed += 1
                    continue

                fetched += 1
            except Exception as exc:  # noqa: BLE001 — fail-soft per pair
                print(f"  [fail]  {type(exc).__name__}: {exc}", file=sys.stderr)
                failed += 1

    elapsed = time.monotonic() - started

    # Tmp residual scan (D1 PASS guard) ------------------------------------
    residuals = list_tmp_residuals(args.out_dir)

    print()
    print("=" * 78)
    print(f"S7 sync summary — {elapsed:.2f}s elapsed")
    print(f"  written       : {fetched}")
    print(f"  skipped empty : {skipped_empty}")
    print(f"  failed        : {failed}")
    print(f"  tmp residuals : {len(residuals)} {residuals if residuals else ''}")
    print("=" * 78)

    if failed > 0 or residuals:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
