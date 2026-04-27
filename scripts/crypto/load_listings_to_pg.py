"""S8 driver: UPSERT crypto/data/listings.csv into PG ``crypto_listings``.

Schema reference: crypto/db/schema.sql (DESIGN.md §11).
CSV columns: pair, symbol, listed_at, delisted_at, delisting_reason, source, notes.
Empty date cells map to SQL NULL.

Idempotent — re-running with same CSV produces same DB state.

Usage (worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/load_listings_to_pg.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402


CSV_PATH = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"


_UPSERT_SQL = """
INSERT INTO crypto_listings
    (pair, symbol, listed_at, delisted_at, delisting_reason, source, notes, updated_at)
VALUES
    (%(pair)s, %(symbol)s,
     %(listed_at)s, %(delisted_at)s, %(delisting_reason)s,
     %(source)s, %(notes)s, NOW())
ON CONFLICT (pair) DO UPDATE SET
    symbol           = EXCLUDED.symbol,
    listed_at        = EXCLUDED.listed_at,
    delisted_at      = EXCLUDED.delisted_at,
    delisting_reason = EXCLUDED.delisting_reason,
    source           = EXCLUDED.source,
    notes            = EXCLUDED.notes,
    updated_at       = NOW()
"""


def _csv_row_to_db_row(r: dict[str, str]) -> dict[str, object]:
    """Map CSV cells to DB-binding dict; '' → None."""
    def _opt(s: str) -> object:
        v = (s or "").strip()
        return v if v else None
    return {
        "pair": r["pair"].strip(),
        "symbol": r["symbol"].strip(),
        "listed_at": _opt(r.get("listed_at", "")),
        "delisted_at": _opt(r.get("delisted_at", "")),
        "delisting_reason": _opt(r.get("delisting_reason", "")),
        "source": (r.get("source") or "manual_v0").strip(),
        "notes": _opt(r.get("notes", "")),
    }


def main() -> int:
    if not CSV_PATH.exists():
        print(f"[fail] CSV missing: {CSV_PATH}. Run build_listings_v0.py first.",
              file=sys.stderr)
        return 1

    rows: list[dict[str, object]] = []
    with CSV_PATH.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(_csv_row_to_db_row(r))

    print("=" * 78)
    print(f"S8 load_listings_to_pg — {len(rows)} rows from {CSV_PATH.name}")
    print("=" * 78)

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection  # noqa: E402

    with connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, rows)
        conn.commit()
        # Post-load census. NOTE on classification:
        #   "Truly active" = both delisted_at AND delisting_reason are NULL.
        #   "Delisted, date pending" = delisting_reason set but delisted_at NULL.
        #   The naive ``delisted_at IS NULL`` filter would conflate these
        #   categories — we count them separately to match Jeff's S8 spec.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM crypto_listings")
            (total,) = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM crypto_listings "
                "WHERE delisted_at IS NULL AND delisting_reason IS NULL"
            )
            (truly_active,) = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM crypto_listings "
                "WHERE delisting_reason IS NOT NULL AND delisted_at IS NOT NULL"
            )
            (delisted_with_date,) = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*) FROM crypto_listings "
                "WHERE delisting_reason IS NOT NULL AND delisted_at IS NULL"
            )
            (delisted_no_date,) = cur.fetchone()

    print(f"[ok] UPSERT committed")
    print(f"  PG crypto_listings total           : {total}")
    print(f"    truly active (no reason, no date): {truly_active}")
    print(f"    delisted with date               : {delisted_with_date}")
    print(f"    delisted, date pending D2 audit  : {delisted_no_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
