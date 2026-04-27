"""S8 driver: build crypto/data/listings.csv v0 (manual curation).

Per Jeff S8 instruction (2026-04-27):
    - listings.csv v0 ≥ 30 entries
    - source: 'manual_v0' or 'upbit_notice' (per-row, recorded)
    - active/delisted distinction explicit (delisted_at NULL = active)
    - listed_at NULL allowed when unclear
    - delisted_at populated only when confidently known
    - automated crawling stays D2

Methodology:
    1. Active pairs: extracted from the S5 raw ticker snapshot
       (crypto/data/_universe/ticker_snapshot_*.json). All currently-listed
       KRW-* pairs are written with delisted_at = NULL, source = 'manual_v0',
       notes describing the snapshot source.
    2. Delisted pairs: hand-curated from public knowledge of well-known
       Upbit delistings. delisted_at populated only for events with public
       date confirmation (Terra ecosystem 2022-05, FTX 2022-12, etc.);
       all other delistings carry NULL date and are flagged for D2 audit.

D1 PASS #5 = ≥ 30 entries total (we expect ≈ 252 active + ≈ 20 delisted).

Usage (worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/build_listings_v0.py
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))


CSV_PATH = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"
RAW_SNAPSHOT_GLOB = "ticker_snapshot_*.json"
RAW_SNAPSHOT_DIR = WORKTREE_ROOT / "crypto" / "data" / "_universe"


CSV_HEADER = [
    "pair",
    "symbol",
    "listed_at",
    "delisted_at",
    "delisting_reason",
    "source",
    "notes",
]


# -----------------------------------------------------------------------------
# Curated delisted pairs (manual_v0)
#
# Confidence policy:
#   - delisted_at populated ONLY when the event is broadly public knowledge
#     with a verifiable Upbit-side delisting window. Anything fuzzy stays
#     NULL and is flagged for D2 official-notice audit.
#   - These rows do NOT claim Upbit was the only exchange — they claim the
#     pair WAS available on Upbit at some point and is no longer in
#     /v1/market/all today.
#   - When in doubt: leave NULL. Survivorship-bias prevention only needs the
#     PAIR to be recorded; precise dates are nice-to-have.
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class DelistedEntry:
    symbol: str
    delisted_at: Optional[str]      # YYYY-MM-DD or None
    reason: str
    notes: str


CURATED_DELISTED: tuple[DelistedEntry, ...] = (
    # --- Terra ecosystem collapse (May 2022) ---------------------------------
    DelistedEntry("LUNA", None,
                  "Terra ecosystem collapse",
                  "Delisted following Terra/UST collapse 2022-05; exact Upbit date pending D2 audit"),
    DelistedEntry("UST", None,
                  "Terra ecosystem collapse (TerraUSD)",
                  "Delisted following Terra collapse 2022-05; exact Upbit date pending D2 audit"),
    DelistedEntry("MIR", None,
                  "Terra ecosystem (Mirror Protocol)",
                  "Mirror Protocol delisting; date pending D2 audit"),
    DelistedEntry("ANC", None,
                  "Terra ecosystem (Anchor Protocol)",
                  "Anchor Protocol delisting; date pending D2 audit"),

    # --- FTX collapse (Nov 2022) ---------------------------------------------
    DelistedEntry("FTT", None,
                  "FTX exchange collapse",
                  "Delisted after FTX bankruptcy 2022-11; exact Upbit date pending D2 audit"),

    # --- Project rebrand / migration -----------------------------------------
    DelistedEntry("NPXS", None,
                  "Pundi X rebrand to PUNDIX",
                  "Delisting tied to NPXS->PUNDIX migration; date pending D2 audit"),

    # --- Generic '유의종목 → 거래지원종료' (delisting reason public; date uncertain) ----
    # Each entry below was on Upbit's KRW market at some point and is no
    # longer in /v1/market/all today. delisted_at left NULL because the
    # exact Upbit date requires reading their notice archive (D2 work).
    DelistedEntry("DAWN", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("SOLVE", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("MARO", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("LBC", None, "유의종목 → 거래지원종료", "LBRY Credits; date pending D2 audit"),
    DelistedEntry("MFT", None, "유의종목 → 거래지원종료", "Mainframe; date pending D2 audit"),
    DelistedEntry("UPP", None, "유의종목 → 거래지원종료", "Sentinel Protocol; date pending D2 audit"),
    DelistedEntry("FCT", None, "유의종목 → 거래지원종료", "Factom; date pending D2 audit"),
    DelistedEntry("IGNIS", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("PIVX", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("VTC", None, "유의종목 → 거래지원종료", "Vertcoin; date pending D2 audit"),
    DelistedEntry("XEM", None, "유의종목 → 거래지원종료", "NEM; date pending D2 audit"),
    DelistedEntry("AERGO", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("LOOM", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("ELF", None, "유의종목 → 거래지원종료", "aelf; date pending D2 audit"),
    DelistedEntry("CON", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("OMG", None, "유의종목 → 거래지원종료", "OMG Network; date pending D2 audit"),
    DelistedEntry("BAT", None, "유의종목 → 거래지원종료", "Basic Attention Token; date pending D2 audit"),
    DelistedEntry("GRS", None, "유의종목 → 거래지원종료", "Groestlcoin; date pending D2 audit"),
    DelistedEntry("BSV", None, "유의종목 → 거래지원종료", "Bitcoin SV; date pending D2 audit"),
    DelistedEntry("DASH", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("OST", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
    DelistedEntry("ARN", None, "유의종목 → 거래지원종료", "date pending D2 audit"),
)


# --- Helpers -----------------------------------------------------------------


def _latest_raw_snapshot() -> Path:
    """Return the most recent ticker snapshot file from S5."""
    snapshots = sorted(RAW_SNAPSHOT_DIR.glob(RAW_SNAPSHOT_GLOB))
    if not snapshots:
        raise FileNotFoundError(
            f"No ticker snapshot found in {RAW_SNAPSHOT_DIR} — run "
            f"build_universe_top100.py first (S5)."
        )
    return snapshots[-1]


def _active_pairs_from_snapshot(snapshot_path: Path) -> list[str]:
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    pairs = sorted(
        p["market"] for p in payload
        if p.get("market", "").startswith("KRW-")
    )
    return pairs


def main() -> int:
    print("=" * 78)
    print("S8 build_listings_v0 — manual curation v0")
    print("=" * 78)

    snapshot_path = _latest_raw_snapshot()
    active_pairs = _active_pairs_from_snapshot(snapshot_path)
    print(f"active source : {snapshot_path.name}")
    print(f"active pairs  : {len(active_pairs)}")
    print(f"curated delisted: {len(CURATED_DELISTED)}")

    active_pair_set = set(active_pairs)
    delisted_pairs = [f"KRW-{e.symbol}" for e in CURATED_DELISTED]

    # Dedupe: a curated delisted pair must NOT also be in current active set.
    overlap = active_pair_set & set(delisted_pairs)
    if overlap:
        print(f"[warn] curated delisted entries are still active: "
              f"{sorted(overlap)}", file=sys.stderr)
        # Keep only those that truly are absent.
        keep_delisted = [
            e for e in CURATED_DELISTED
            if f"KRW-{e.symbol}" not in active_pair_set
        ]
        print(f"[info] dropped {len(CURATED_DELISTED) - len(keep_delisted)} "
              f"still-active 'delisted' entries")
    else:
        keep_delisted = list(CURATED_DELISTED)

    # Build CSV rows ----------------------------------------------------
    rows: list[dict[str, str]] = []
    snapshot_date = datetime.now(timezone.utc).date().isoformat()

    for pair in active_pairs:
        symbol = pair.split("-", 1)[1]
        rows.append({
            "pair": pair,
            "symbol": symbol,
            "listed_at": "",  # Upbit /v1/market/all does not expose listing date
            "delisted_at": "",
            "delisting_reason": "",
            "source": "manual_v0",
            "notes": f"Active per Upbit /v1/market/all on {snapshot_date}",
        })

    for entry in keep_delisted:
        rows.append({
            "pair": f"KRW-{entry.symbol}",
            "symbol": entry.symbol,
            "listed_at": "",
            "delisted_at": entry.delisted_at or "",
            "delisting_reason": entry.reason,
            "source": "manual_v0",
            "notes": entry.notes,
        })

    # Write CSV ---------------------------------------------------------
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    n_total = len(rows)
    n_active = sum(1 for r in rows if not r["delisted_at"] and not r["delisting_reason"])
    n_delisted = n_total - n_active
    n_dated = sum(1 for r in rows if r["delisted_at"])

    print()
    print(f"csv path       : {CSV_PATH.relative_to(WORKTREE_ROOT)}")
    print(f"total entries  : {n_total}")
    print(f"  active       : {n_active}")
    print(f"  delisted     : {n_delisted}")
    print(f"    with date  : {n_dated}")
    print(f"    NULL date  : {n_delisted - n_dated}")
    print()

    if n_total < 30:
        print(f"[fail] D1 PASS #5 requires ≥ 30 entries, got {n_total}",
              file=sys.stderr)
        return 1
    if n_delisted == 0:
        print("[fail] no delisted entries — survivorship tracking would be empty",
              file=sys.stderr)
        return 1

    print(f"[ok] D1 PASS #5 satisfied: {n_total} ≥ 30 entries "
          f"({n_active} active + {n_delisted} delisted)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
