"""Shared fill-in-the-blanks merge helpers for listings.csv + crypto_listings.

Extracted from scripts/crypto/crawl_upbit_notices.py (D2) so D3 incremental
cron can reuse the same atomic semantics without code duplication.

Contract (Jeff D2 #5, repeated for D3):
    - Source priority: ``upbit_notice > manual_v0`` (only when filling NULL).
    - Already-populated fields are NEVER overwritten.
    - PG runs in a single transaction (caller commits / rolls back).
    - CSV is written .tmp → fsync → rename (atomic rename on same FS).

Idempotency (Jeff D3 #1):
    Re-running with the same crawled events against the same baseline produces
    zero row changes — INSERT uses ``ON CONFLICT DO NOTHING`` and UPDATE is
    gated on ``delisted_at IS NULL``. CSV merge picks ``preserved_existing_date``
    instead of overwriting.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

from crypto.data.listings_crawler import DelistingNotice


CSV_HEADER: list[str] = [
    "pair",
    "symbol",
    "listed_at",
    "delisted_at",
    "delisting_reason",
    "source",
    "notes",
]


# --- CSV ----------------------------------------------------------------


def read_existing_csv(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not csv_path.exists():
        return list(CSV_HEADER), []
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or CSV_HEADER), [dict(r) for r in reader]


def write_csv_atomic(csv_path: Path, rows: list[dict[str, str]]) -> None:
    """Write rows to ``csv_path.tmp`` → fsync → rename.

    The rename is atomic on the same filesystem, so a mid-write crash leaves
    either the old file intact or the new one in place — never a partial.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in CSV_HEADER})
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, csv_path)


def merge_fill_in_the_blanks(
    existing_rows: list[dict[str, str]],
    delistings: list[DelistingNotice],
) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Merge crawled delistings into existing CSV rows.

    Rules:
      - If pair NOT in existing → append (source='upbit_notice')
      - If pair exists, ``delisted_at`` empty, crawled date present
        → fill date + reason + notes; promote source to 'upbit_notice'
      - If pair exists with ``delisted_at`` already set → preserve
      - Non-KRW notices are skipped

    Returns: (merged_rows_sorted, stats).
    """
    by_pair: dict[str, dict[str, str]] = {r["pair"]: dict(r) for r in existing_rows}
    stats = {
        "upserted_filled_date": 0,
        "inserted_new_pair": 0,
        "preserved_existing_date": 0,
        "skipped_non_krw": 0,
    }

    for ev in delistings:
        if not ev.affects_krw:
            stats["skipped_non_krw"] += 1
            continue
        pair = ev.pair
        new_delisted_at = (
            ev.delisted_at_kst.isoformat() if ev.delisted_at_kst else ""
        )
        new_reason = f"Upbit notice #{ev.notice_id}: {ev.title[:200]}"
        new_notes = (
            f"crawled_from {ev.source_url}; "
            f"affects_krw={ev.affects_krw}; "
            f"date_format={ev.date_format_used or 'unparsed'}"
        )

        if pair not in by_pair:
            by_pair[pair] = {
                "pair": pair,
                "symbol": ev.symbol,
                "listed_at": "",
                "delisted_at": new_delisted_at,
                "delisting_reason": new_reason,
                "source": "upbit_notice",
                "notes": new_notes,
            }
            stats["inserted_new_pair"] += 1
            continue

        existing = by_pair[pair]
        existing_date = (existing.get("delisted_at") or "").strip()

        if existing_date:
            stats["preserved_existing_date"] += 1
            continue

        if new_delisted_at:
            existing["delisted_at"] = new_delisted_at
            existing["delisting_reason"] = new_reason
            existing["source"] = "upbit_notice"
            existing["notes"] = new_notes
            stats["upserted_filled_date"] += 1
        else:
            stats["preserved_existing_date"] += 1

    out_rows = sorted(by_pair.values(), key=lambda r: r["pair"])
    return out_rows, stats


# --- PG -----------------------------------------------------------------


_INSERT_NEW_SQL = """
INSERT INTO crypto_listings
    (pair, symbol, listed_at, delisted_at, delisting_reason, source, notes, updated_at)
VALUES
    (%(pair)s, %(symbol)s, NULL, %(delisted_at)s, %(delisting_reason)s,
     'upbit_notice', %(notes)s, NOW())
ON CONFLICT (pair) DO NOTHING
"""

_FILL_DATE_SQL = """
UPDATE crypto_listings
SET
    delisted_at      = %(delisted_at)s,
    delisting_reason = %(delisting_reason)s,
    source           = 'upbit_notice',
    notes            = %(notes)s,
    updated_at       = NOW()
WHERE pair = %(pair)s
  AND delisted_at IS NULL
"""


def pg_apply_delistings(conn, delistings: list[DelistingNotice]) -> dict[str, int]:
    """Apply crawled delistings to PG inside the caller's transaction.

    Two passes:
        1. INSERT ... ON CONFLICT DO NOTHING → adds previously-unknown pairs.
        2. UPDATE ... WHERE delisted_at IS NULL → fills NULL dates only.

    The caller is responsible for ``commit()`` / ``rollback()``. Any exception
    here propagates so the ``with connection() as conn`` block triggers a
    transaction-wide rollback (no partial writes).
    """
    stats = {"pg_inserted_new": 0, "pg_filled_date": 0, "pg_skipped_non_krw": 0}

    rows_to_insert: list[dict[str, Any]] = []
    rows_to_fill: list[dict[str, Any]] = []
    for ev in delistings:
        if not ev.affects_krw:
            stats["pg_skipped_non_krw"] += 1
            continue
        new_delisted_at = ev.delisted_at_kst.isoformat() if ev.delisted_at_kst else None
        params = {
            "pair": ev.pair,
            "symbol": ev.symbol,
            "delisted_at": new_delisted_at,
            "delisting_reason": f"Upbit notice #{ev.notice_id}: {ev.title[:200]}",
            "notes": (
                f"crawled_from {ev.source_url}; "
                f"affects_krw={ev.affects_krw}; "
                f"date_format={ev.date_format_used or 'unparsed'}"
            ),
        }
        rows_to_insert.append(params)
        if new_delisted_at:
            rows_to_fill.append(params)

    with conn.cursor() as cur:
        if rows_to_insert:
            cur.executemany(_INSERT_NEW_SQL, rows_to_insert)
            stats["pg_inserted_new"] = max(cur.rowcount or 0, 0)
        if rows_to_fill:
            cur.executemany(_FILL_DATE_SQL, rows_to_fill)
            stats["pg_filled_date"] = max(cur.rowcount or 0, 0)

    return stats
