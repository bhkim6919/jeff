"""S7 verifier: independently check PG ↔ parquet parity.

Runs in addition to (not in place of) sync_pg_to_parquet.py's per-pair check —
this script reads both stores fresh and is the canonical D1 PASS evidence
producer for criteria #4 and #11.

Output:
    crypto/data/_verification/pg_parquet_parity_<utc_date>.json
        Per-pair counts/checksums + overall PASS/FAIL.

Exit code:
    0 — all pairs match (or all skipped because no parquet exists yet)
    1 — at least one mismatch / missing parquet for an existing PG pair /
        tmp residual found.

Usage (worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/verify_pg_parquet_parity.py \
        [--pairs-from crypto/data/universe_top100.csv] [--limit N]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.db.parquet_io import (  # noqa: E402
    aggregate_checksum_from_parquet,
    count_rows_parquet,
    list_tmp_residuals,
)
from crypto.db.repository import (  # noqa: E402
    aggregate_checksum_for_pair,
    count_rows_for_pair,
)


PARQUET_OUT_DIR = WORKTREE_ROOT / "crypto" / "data" / "ohlcv"
VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
DEFAULT_UNIVERSE_PATH = WORKTREE_ROOT / "crypto" / "data" / "universe_top100.csv"


def _load_pairs_from_csv(path: Path) -> list[str]:
    out: list[str] = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            p = row.get("pair", "").strip()
            if p:
                out.append(p)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = p.add_mutually_exclusive_group()
    src.add_argument("--pair", help="Single market.")
    src.add_argument(
        "--pairs-from",
        type=Path,
        default=DEFAULT_UNIVERSE_PATH,
        help=f"CSV with a 'pair' column. Default: {DEFAULT_UNIVERSE_PATH}",
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--out-dir", type=Path, default=PARQUET_OUT_DIR)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.pair:
        pairs = [args.pair]
    else:
        pairs = _load_pairs_from_csv(args.pairs_from)
        if args.limit is not None:
            pairs = pairs[: args.limit]

    print("=" * 78)
    print("S7 verify_pg_parquet_parity — D1 PASS #4 + #11 evidence")
    print("=" * 78)
    print(f"pairs   : {len(pairs)}")
    print(f"out_dir : {args.out_dir}")
    print()

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection  # noqa: E402

    started = time.monotonic()
    per_pair: list[dict] = []
    mismatches = 0
    missing_parquet_for_pg = 0

    with connection() as conn:
        for i, pair in enumerate(pairs, start=1):
            parquet_path = args.out_dir / f"{pair}.parquet"
            pg_count = count_rows_for_pair(conn, pair)
            pg_checksum = aggregate_checksum_for_pair(conn, pair) if pg_count else ""
            pq_count = count_rows_parquet(parquet_path)
            pq_checksum = aggregate_checksum_from_parquet(parquet_path)

            row_match = pg_count == pq_count
            chk_match = pg_checksum == pq_checksum
            both_match = row_match and chk_match

            # Note: pg_count==0 (no PG data) and pq_count==0 (no parquet) → match by definition.
            if pg_count > 0 and pq_count == 0:
                missing_parquet_for_pg += 1
            if not both_match:
                mismatches += 1

            verdict = "ok" if both_match else "FAIL"
            print(
                f"[{i:>3}/{len(pairs)}] [{verdict}] {pair:<14}  "
                f"rows pg/pq={pg_count}/{pq_count}  "
                f"chk pg/pq={pg_checksum[:10]}/{pq_checksum[:10]}"
            )

            per_pair.append(
                {
                    "pair": pair,
                    "pg_row_count": pg_count,
                    "parquet_row_count": pq_count,
                    "pg_checksum": pg_checksum,
                    "parquet_checksum": pq_checksum,
                    "row_match": row_match,
                    "checksum_match": chk_match,
                }
            )

    residuals = list_tmp_residuals(args.out_dir)

    elapsed = time.monotonic() - started
    pass_overall = mismatches == 0 and missing_parquet_for_pg == 0 and not residuals

    print()
    print("=" * 78)
    print(f"S7 parity summary — {elapsed:.2f}s elapsed")
    print(f"  pairs evaluated      : {len(pairs)}")
    print(f"  mismatches           : {mismatches}")
    print(f"  missing parquet for "
          f"PG-populated pair    : {missing_parquet_for_pg}")
    print(f"  tmp residuals        : {len(residuals)} {residuals if residuals else ''}")
    print(f"  D1 PASS #4 + #11    : {'PASS' if pass_overall else 'FAIL'}")
    print("=" * 78)

    # Persist evidence ----------------------------------------------------
    VERIF_DIR.mkdir(parents=True, exist_ok=True)
    verified_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    out_path = VERIF_DIR / f"pg_parquet_parity_{verified_at[:10]}.json"
    out_path.write_text(
        json.dumps(
            {
                "verified_at_utc": verified_at,
                "pairs_evaluated": len(pairs),
                "mismatches": mismatches,
                "missing_parquet_for_pg": missing_parquet_for_pg,
                "tmp_residuals": [str(r) for r in residuals],
                "pass": pass_overall,
                "per_pair": per_pair,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"[ok] evidence saved → {out_path.relative_to(WORKTREE_ROOT)}")

    return 0 if pass_overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
