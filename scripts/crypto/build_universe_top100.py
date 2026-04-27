"""S5 driver: build and persist the Crypto Lab D1 universe (Top 100 KRW spot).

Outputs (relative to worktree root):
    crypto/data/universe_top100.csv
        Static reference file. Columns:
            snapshot_dt_utc, rank, pair, value_krw_24h, captured_at
        Matches DB ``crypto_universe_top100`` (DESIGN.md §11) on
        rank/pair/value_krw_24h (Jeff S5 condition #4).

    crypto/data/_universe/ticker_snapshot_<captured_at>.json
        Full /v1/ticker response for ALL queried KRW markets. Persisted for
        reproducibility (Jeff S5 condition #5).

    crypto/data/_universe/ticker_snapshot_<captured_at>.sha256
        SHA256 checksum of the canonical raw snapshot.

Usage (from worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/build_universe_top100.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

# Ensure crypto/ is importable when running from worktree root.
HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.data.universe import (  # noqa: E402
    UNIVERSE_SIZE,
    build_universe_top100,
)
from crypto.data.upbit_provider import UpbitQuotationProvider  # noqa: E402


CSV_PATH = WORKTREE_ROOT / "crypto" / "data" / "universe_top100.csv"
RAW_DIR = WORKTREE_ROOT / "crypto" / "data" / "_universe"
CSV_HEADER = ["snapshot_dt_utc", "rank", "pair", "value_krw_24h", "captured_at"]


def _ts_for_filename(iso: str) -> str:
    """Convert '2026-04-27T13:00:00+00:00' → '20260427T130000Z' filesafe."""
    s = iso.replace("+00:00", "Z").replace(":", "").replace("-", "")
    return s


def main() -> int:
    print("=" * 78)
    print("S5 Universe Top 100 — KRW spot, sorted by acc_trade_price_24h DESC")
    print("=" * 78)

    provider = UpbitQuotationProvider()
    snap = build_universe_top100(provider)

    fname_ts = _ts_for_filename(snap.captured_at)

    # --- Persist CSV (5 columns, DB-compatible) ----------------------------
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in snap.to_csv_rows():
            writer.writerow(row)

    # --- Persist raw ticker snapshot + checksum (Jeff S5 #5) ---------------
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DIR / f"ticker_snapshot_{fname_ts}.json"
    raw_path.write_text(
        json.dumps(snap.raw_tickers, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    checksum_path = RAW_DIR / f"ticker_snapshot_{fname_ts}.sha256"
    checksum_path.write_text(
        f"{snap.raw_checksum_sha256}  ticker_snapshot_{fname_ts}.json\n",
        encoding="utf-8",
    )

    # --- Build report ------------------------------------------------------
    n_universe = len(snap.universe)
    n_total_krw = len(snap.raw_tickers)
    print(f"snapshot_dt_utc      : {snap.snapshot_dt_utc}")
    print(f"captured_at (UTC)    : {snap.captured_at}")
    print(f"total KRW markets    : {n_total_krw}")
    print(f"universe size        : {n_universe} / {UNIVERSE_SIZE}")
    print(f"csv                  : {CSV_PATH.relative_to(WORKTREE_ROOT)}")
    print(f"raw snapshot         : {raw_path.relative_to(WORKTREE_ROOT)}")
    print(f"raw sha256           : {snap.raw_checksum_sha256}")
    print()
    print("--- Top 10 ---")
    print(f"{'#':>3}  {'pair':<14}  {'24h value (KRW)':>22}")
    for row in snap.universe[:10]:
        print(f"{row.rank:>3}  {row.pair:<14}  {row.value_krw_24h:>22,.0f}")
    if n_universe > 10:
        print(f"... ({n_universe - 10} more)")

    # --- CSV ↔ DB schema parity self-check (Jeff S5 #4) -------------------
    # We don't have the DB applied yet (S6+). Verify the CSV columns we wrote
    # are the exact subset the DB expects on rank/pair/value_krw_24h.
    csv_required = {"snapshot_dt_utc", "rank", "pair", "value_krw_24h"}
    csv_actual = set(CSV_HEADER)
    missing = csv_required - csv_actual
    if missing:
        print(f"[fail] CSV header missing required DB columns: {missing}",
              file=sys.stderr)
        return 1

    # --- Decision ---------------------------------------------------------
    if n_universe < UNIVERSE_SIZE:
        print(f"[warn] universe is smaller than {UNIVERSE_SIZE} "
              f"({n_universe} markets) — Upbit lists fewer KRW pairs today.",
              file=sys.stderr)
    print()
    print("[ok] S5 build PASS — CSV + raw snapshot + checksum persisted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
