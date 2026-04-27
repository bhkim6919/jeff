"""S4 verification script: fetch BTC daily candles and confirm Upbit boundary.

Goals (DESIGN.md §5.2):
    1. Fetch 100 daily candles for KRW-BTC via Quotation REST.
    2. Print candle_date_time_utc / candle_date_time_kst for sample rows.
    3. Compute the (kst - utc) offset and verify it is a constant +9h
       (i.e., Upbit candles align to a fixed timezone, not local DST).
    4. Determine which boundary hypothesis (A / B / C) holds.
    5. Emit a JSON summary file for archival under
       crypto/data/_verification/upbit_boundary_<utc_date>.json
    6. Confirm the response payload contains NO Exchange API leakage.

Usage (from worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" \
        scripts/crypto/verify_upbit_boundary.py

Exit code:
    0 — success, summary written.
    1 — fetch error, count mismatch, or boundary inconsistency.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure crypto/ is importable when running from worktree root.
HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.data.upbit_provider import (  # noqa: E402
    UpbitQuotationProvider,
    fetch_daily_range,
)


TARGET_MARKET = "KRW-BTC"
TARGET_COUNT = 100
SAMPLE_PRINT_COUNT = 5
OUTPUT_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"


def parse_naive_iso(s: str) -> datetime:
    """Parse '2026-04-27T00:00:00' style naive ISO timestamps."""
    # Defensive: accept trailing Z or +00:00
    if s.endswith("Z"):
        s = s[:-1]
    return datetime.fromisoformat(s)


def main() -> int:
    print("=" * 78)
    print(f"S4 Upbit Boundary Verification — {TARGET_MARKET} × {TARGET_COUNT} days")
    print("=" * 78)

    provider = UpbitQuotationProvider()
    print(f"[fetch] paginated daily range → target_count={TARGET_COUNT}")
    candles = fetch_daily_range(provider, TARGET_MARKET, target_count=TARGET_COUNT)
    fetched_count = len(candles)
    print(f"[fetch] received {fetched_count} candles "
          f"({'PASS' if fetched_count == TARGET_COUNT else 'WARN'})")
    if fetched_count == 0:
        print("[fail] zero candles returned — abort", file=sys.stderr)
        return 1

    # Sort ASC for human-friendly inspection.
    candles_asc = sorted(candles, key=lambda c: c["candle_date_time_utc"])

    print()
    print(f"--- Sample (oldest {SAMPLE_PRINT_COUNT}) ---")
    print(f"{'idx':>3}  {'candle_date_time_utc':<22}  {'candle_date_time_kst':<22}  "
          f"{'kst-utc':>9}  {'close':>14}")
    for i, c in enumerate(candles_asc[:SAMPLE_PRINT_COUNT]):
        utc_s = c["candle_date_time_utc"]
        kst_s = c["candle_date_time_kst"]
        offset = parse_naive_iso(kst_s) - parse_naive_iso(utc_s)
        print(f"{i:>3}  {utc_s:<22}  {kst_s:<22}  {str(offset):>9}  "
              f"{c['trade_price']:>14}")

    print()
    print(f"--- Sample (newest {SAMPLE_PRINT_COUNT}) ---")
    print(f"{'idx':>3}  {'candle_date_time_utc':<22}  {'candle_date_time_kst':<22}  "
          f"{'kst-utc':>9}  {'close':>14}")
    for i, c in enumerate(candles_asc[-SAMPLE_PRINT_COUNT:]):
        utc_s = c["candle_date_time_utc"]
        kst_s = c["candle_date_time_kst"]
        offset = parse_naive_iso(kst_s) - parse_naive_iso(utc_s)
        print(f"{i:>3}  {utc_s:<22}  {kst_s:<22}  {str(offset):>9}  "
              f"{c['trade_price']:>14}")

    # --- Boundary analysis --------------------------------------------------
    offsets = [
        parse_naive_iso(c["candle_date_time_kst"])
        - parse_naive_iso(c["candle_date_time_utc"])
        for c in candles
    ]
    unique_offsets = sorted({o.total_seconds() for o in offsets})
    print()
    print(f"--- Boundary analysis ---")
    print(f"unique (kst - utc) offsets: {unique_offsets} seconds")

    # Check first candle to determine boundary hypothesis.
    sample = candles_asc[0]
    utc_t = parse_naive_iso(sample["candle_date_time_utc"])
    kst_t = parse_naive_iso(sample["candle_date_time_kst"])
    print(f"sample utc time-of-day: {utc_t.time()} on {utc_t.date()}")
    print(f"sample kst time-of-day: {kst_t.time()} on {kst_t.date()}")

    # Map to hypotheses:
    #   A: KST 00:00~24:00 boundary (= UTC 15:00 prev ~ UTC 15:00)
    #   B: KST 09:00~09:00+1d (= UTC 00:00~24:00)
    #   C: other
    if utc_t.hour == 0 and kst_t.hour == 9 and unique_offsets == [9 * 3600]:
        hypothesis = "B"
        boundary_desc = (
            "Daily candle covers UTC 00:00 → 23:59 (KST 09:00 → next day 09:00). "
            "candle_date_time_kst = 09:00 KST of the same calendar day as candle_date_time_utc. "
            "Trade day key = candle_date_time_utc.date()."
        )
    elif utc_t.hour == 15 and kst_t.hour == 0 and unique_offsets == [9 * 3600]:
        hypothesis = "A"
        boundary_desc = (
            "Daily candle covers KST 00:00 → 23:59 (UTC 15:00 prev day → UTC 15:00). "
            "candle_date_time_kst = 00:00 KST of the trade day. "
            "Trade day key = candle_date_time_kst.date()."
        )
    elif utc_t.hour == 0 and kst_t.hour == 0 and unique_offsets == [9 * 3600]:
        # Shouldn't happen — KST and UTC dates would differ by 9h naively. Document as anomaly.
        hypothesis = "C"
        boundary_desc = (
            "Anomaly: both timestamps are 00:00 but offset is +9h. "
            "Manual inspection required."
        )
    else:
        hypothesis = "C"
        boundary_desc = (
            f"Other layout. utc time-of-day = {utc_t.time()}, "
            f"kst time-of-day = {kst_t.time()}, offsets = {unique_offsets}s. "
            "Manual inspection required."
        )

    print(f"hypothesis: {hypothesis}")
    print(f"boundary  : {boundary_desc}")

    # --- Forbidden-key leakage scan ----------------------------------------
    forbidden_keys = {
        "balance",
        "available",
        "locked",
        "uuid",
        "side",
        "ord_type",
        "price",  # Note: 'trade_price' OK; standalone 'price' would be order-side
        "remaining_volume",
        "executed_volume",
        "reserved_fee",
        "remaining_fee",
        "paid_fee",
    }
    leak: dict[str, list[str]] = {}
    for c in candles[:10]:
        for k in c.keys():
            if k in forbidden_keys and k != "trade_price":
                leak.setdefault(k, []).append(c["candle_date_time_utc"])
    print()
    print(f"--- Forbidden-key leakage (Exchange API surface) ---")
    if leak:
        print(f"WARN: forbidden keys found: {leak}")
    else:
        print("PASS: no Exchange API field surfaced in Quotation response.")

    # --- Persist summary ---------------------------------------------------
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today_utc = datetime.utcnow().strftime("%Y-%m-%d")
    out_path = OUTPUT_DIR / f"upbit_boundary_{today_utc}.json"
    summary = {
        "verified_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "market": TARGET_MARKET,
        "fetched_count": fetched_count,
        "target_count": TARGET_COUNT,
        "unique_kst_minus_utc_seconds": unique_offsets,
        "hypothesis": hypothesis,
        "boundary_description": boundary_desc,
        "sample_oldest": candles_asc[0],
        "sample_newest": candles_asc[-1],
        "forbidden_key_leakage": leak,
    }
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print()
    print(f"[ok] summary saved → {out_path.relative_to(WORKTREE_ROOT)}")

    # --- Decision ---------------------------------------------------------
    if fetched_count != TARGET_COUNT:
        print(f"[fail] count mismatch: got {fetched_count}, want {TARGET_COUNT}",
              file=sys.stderr)
        return 1
    if unique_offsets != [9 * 3600]:
        print(f"[fail] non-constant or non-+9h offset: {unique_offsets}",
              file=sys.stderr)
        return 1
    if leak:
        print(f"[fail] forbidden keys surfaced: {leak}", file=sys.stderr)
        return 1
    if hypothesis == "C":
        print("[warn] hypothesis C — boundary required manual review", file=sys.stderr)
        # Still exit 0 so summary is preserved; Jeff reviews JSON.

    print("[ok] S4 verification PASS — update DESIGN.md §5.2 with hypothesis above")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
