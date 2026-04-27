"""S6 driver: bulk-fetch Upbit KRW daily OHLCV into PostgreSQL.

Per Jeff S6 instruction (2026-04-27):
    - PG only first (parquet write is S7).
    - Existing KR/US PostgreSQL instance, crypto_* tables only.
    - BTC/ETH validated before Top 98.
    - Checkpoint applied (DESIGN.md §13).
    - row_checksum stored in PG.

PASS conditions (S6 — DB-side only, full PASS in G3):
    1. BTC/ETH 100-day or full-range fetch success
    2. crypto_ohlcv UPSERT success (no errors)
    3. row_checksum deterministic (re-run → same digest)
    4. Re-run row count identical (UPSERT idempotency)
    5. 0 Exchange API / order / balance code (static + runtime)

Usage:
    # Single pair, default range (2018-01-01 → yesterday UTC)
    python -X utf8 scripts/crypto/bulk_fetch_d1.py --pair KRW-BTC

    # Multi-pair from universe CSV
    python -X utf8 scripts/crypto/bulk_fetch_d1.py \
        --pairs-from crypto/data/universe_top100.csv \
        --limit 2                           # first 2 = BTC/ETH staging

    # Reset checkpoint and rerun fresh
    python -X utf8 scripts/crypto/bulk_fetch_d1.py --pair KRW-BTC --reset
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.data.upbit_provider import UpbitQuotationProvider  # noqa: E402
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.db.repository import (  # noqa: E402
    aggregate_checksum_for_pair,
    count_rows_for_pair,
    upsert_pair_candles,
)


# --- Defaults ----------------------------------------------------------------

DEFAULT_TARGET_START_KST = date(2018, 1, 1)
DEFAULT_CHECKPOINT_PATH = HERE.parent / "bulk_fetch_checkpoint.json"
DEFAULT_UNIVERSE_PATH = WORKTREE_ROOT / "crypto" / "data" / "universe_top100.csv"
DEFAULT_PAGE_SIZE = 200  # Upbit hard limit


# --- Date helpers ------------------------------------------------------------


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _kst_to_utc_iso_z(d: date) -> str:
    """``2026-04-27`` (KST trade day) → ``2026-04-27T00:00:00Z`` (UTC).

    Per S4 hypothesis B, candle_date_time_utc for a KST trade day == that date
    at 00:00 UTC. So to fetch candles strictly BEFORE end_kst+1 we pass
    ``(end_kst + 1day).isoformat() + 'T00:00:00Z'``.
    """
    return f"{d.isoformat()}T00:00:00Z"


# --- Date-bounded paginated fetch -------------------------------------------


def fetch_kst_range(
    provider: UpbitQuotationProvider,
    market: str,
    start_kst: date,
    end_kst: date,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> list[dict[str, Any]]:
    """Fetch all daily candles for ``market`` where candle_dt_kst ∈ [start, end].

    Returns candles deduplicated by candle_date_time_utc, ascending by date.
    """
    if end_kst < start_kst:
        return []

    seen_utc: set[str] = set()
    out: list[dict[str, Any]] = []

    # Upbit `to` is exclusive: a candle is returned only if its
    # candle_date_time_utc < `to`. Setting to = (end_kst + 1day)T00:00:00Z
    # includes end_kst's candle.
    to_utc: Optional[str] = _kst_to_utc_iso_z(end_kst + timedelta(days=1))

    while True:
        page = provider.fetch_daily_candles(market, count=page_size, to_utc=to_utc)
        if not page:
            break

        added = 0
        oldest_kst_in_page: Optional[date] = None
        for c in page:
            utc_iso = c.get("candle_date_time_utc", "")
            kst_iso = c.get("candle_date_time_kst", "")
            if not utc_iso or not kst_iso:
                continue
            kst_dt = datetime.fromisoformat(kst_iso).date()
            if kst_dt < start_kst or kst_dt > end_kst:
                continue
            if utc_iso in seen_utc:
                continue
            seen_utc.add(utc_iso)
            out.append(c)
            added += 1
            if oldest_kst_in_page is None or kst_dt < oldest_kst_in_page:
                oldest_kst_in_page = kst_dt

        # Stop conditions
        if added == 0:
            break
        if oldest_kst_in_page is not None and oldest_kst_in_page <= start_kst:
            break
        if len(page) < page_size:
            break

        # Continue: page back further
        oldest_utc_in_page = page[-1].get("candle_date_time_utc", "")
        if not oldest_utc_in_page:
            break
        to_utc = (
            oldest_utc_in_page
            if oldest_utc_in_page.endswith("Z")
            else f"{oldest_utc_in_page}Z"
        )

    out.sort(key=lambda c: c["candle_date_time_utc"])
    return out


# --- Checkpoint --------------------------------------------------------------


@dataclass
class Checkpoint:
    path: Path
    data: dict[str, Any]

    @classmethod
    def load_or_init(
        cls,
        path: Path,
        target_start_kst: date,
        target_end_kst: date,
        universe_source_hash: str,
    ) -> "Checkpoint":
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                # Drift detection — universe source change requires --reset.
                stored_hash = data.get("universe_source_hash")
                if stored_hash and stored_hash != universe_source_hash:
                    raise RuntimeError(
                        "[CHECKPOINT_UNIVERSE_DRIFT] universe source changed. "
                        "Re-run with --reset to start fresh, or restore "
                        "previous universe."
                    )
                return cls(path=path, data=data)
            except json.JSONDecodeError:
                # Backup corrupted file and start fresh.
                bak = path.with_suffix(
                    f".bak.{int(time.time())}.json"
                )
                path.rename(bak)
        return cls(
            path=path,
            data={
                "started_at": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
                "target_start_kst": target_start_kst.isoformat(),
                "target_end_kst": target_end_kst.isoformat(),
                "universe_source_hash": universe_source_hash,
                "pairs": {},
            },
        )

    def status(self, pair: str) -> str:
        return self.data["pairs"].get(pair, {}).get("status", "pending")

    def mark_completed(
        self,
        pair: str,
        last_completed_kst: date,
        row_count: int,
        row_checksum_hex: str,
    ) -> None:
        self.data["pairs"][pair] = {
            "status": "completed",
            "last_completed_kst": last_completed_kst.isoformat(),
            "row_count": row_count,
            "row_checksum": row_checksum_hex,
            "fetched_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        }
        self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self.data, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        # Best-effort fsync via OS-level sync of the file before rename.
        with tmp.open("rb+") as f:
            os.fsync(f.fileno())
        os.replace(tmp, self.path)


# --- Universe source hashing -------------------------------------------------


def _hash_universe_source(path: Path) -> str:
    """SHA256 of the universe CSV's contents — drift detector for checkpoint."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_pairs_from_csv(path: Path) -> list[str]:
    pairs: list[str] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p = row.get("pair", "").strip()
            if p:
                pairs.append(p)
    return pairs


# --- Argparse ----------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--pair", help="Single market e.g. KRW-BTC")
    src.add_argument(
        "--pairs-from",
        type=Path,
        help=f"CSV with a 'pair' column. Default location: {DEFAULT_UNIVERSE_PATH}",
    )

    p.add_argument(
        "--target-start-kst",
        type=lambda s: date.fromisoformat(s),
        default=DEFAULT_TARGET_START_KST,
        help="Earliest KST trade day to fetch (default: 2018-01-01).",
    )
    p.add_argument(
        "--target-end-kst",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Latest KST trade day to fetch (default: yesterday UTC).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="When using --pairs-from, fetch only the first N pairs.",
    )
    p.add_argument(
        "--checkpoint",
        type=Path,
        default=DEFAULT_CHECKPOINT_PATH,
        help="Checkpoint JSON path.",
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Wipe checkpoint before starting (forces full re-fetch).",
    )
    p.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Upbit page size (1..200). Default 200.",
    )
    return p.parse_args()


# --- Main --------------------------------------------------------------------


def _fetch_one_pair(
    provider: UpbitQuotationProvider,
    conn,
    pair: str,
    target_start_kst: date,
    target_end_kst: date,
    page_size: int,
) -> tuple[int, str, Optional[date]]:
    """Fetch + UPSERT a single pair. Returns (row_count, agg_checksum_hex, latest_kst)."""
    print(f"  [fetch] {pair}: range "
          f"{target_start_kst.isoformat()}..{target_end_kst.isoformat()}")
    candles = fetch_kst_range(
        provider, pair, target_start_kst, target_end_kst, page_size=page_size
    )
    print(f"  [fetch] {pair}: {len(candles)} candles received")
    if not candles:
        return 0, "", None

    print(f"  [pg]    {pair}: UPSERT begin")
    stats = upsert_pair_candles(conn, candles)
    conn.commit()
    print(f"  [pg]    {pair}: UPSERT committed ({stats['rows']} rows)")

    cnt = count_rows_for_pair(conn, pair)
    chk = aggregate_checksum_for_pair(conn, pair)
    latest_kst = max(
        datetime.fromisoformat(c["candle_date_time_kst"]).date()
        for c in candles
    )
    print(f"  [pg]    {pair}: stored row_count={cnt}, agg_sha256={chk[:16]}…, "
          f"latest_kst={latest_kst}")
    return cnt, chk, latest_kst


def main() -> int:
    args = _parse_args()
    target_end_kst = args.target_end_kst or (_today_utc() - timedelta(days=1))

    if args.pair:
        pairs = [args.pair]
        universe_source_path: Optional[Path] = None
    else:
        if not args.pairs_from.exists():
            print(f"[fail] CSV not found: {args.pairs_from}", file=sys.stderr)
            return 1
        pairs = _load_pairs_from_csv(args.pairs_from)
        if args.limit is not None:
            pairs = pairs[: args.limit]
        universe_source_path = args.pairs_from

    if args.reset and args.checkpoint.exists():
        bak = args.checkpoint.with_suffix(
            f".reset.{int(time.time())}.json"
        )
        args.checkpoint.rename(bak)
        print(f"[info] checkpoint reset; backup at {bak.name}")

    universe_hash = (
        _hash_universe_source(universe_source_path)
        if universe_source_path
        else "single-pair-mode"
    )

    print("=" * 78)
    print("S6 Bulk Fetch — Upbit KRW daily OHLCV → PostgreSQL")
    print("=" * 78)
    print(f"pairs                : {len(pairs)} ({pairs[:5]}{'...' if len(pairs)>5 else ''})")
    print(f"target_start_kst     : {args.target_start_kst}")
    print(f"target_end_kst       : {target_end_kst}")
    print(f"checkpoint           : {args.checkpoint}")
    print(f"page_size            : {args.page_size}")
    print()

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection  # noqa: E402

    cp = Checkpoint.load_or_init(
        args.checkpoint,
        args.target_start_kst,
        target_end_kst,
        universe_hash,
    )

    provider = UpbitQuotationProvider()
    started = time.monotonic()
    skipped = 0
    fetched = 0
    failed = 0

    with connection() as conn:
        for i, pair in enumerate(pairs, start=1):
            print(f"[{i:>3}/{len(pairs)}] {pair}")
            if cp.status(pair) == "completed":
                print(f"  [skip]  already completed in checkpoint")
                skipped += 1
                continue
            try:
                cnt, chk, latest_kst = _fetch_one_pair(
                    provider,
                    conn,
                    pair,
                    args.target_start_kst,
                    target_end_kst,
                    args.page_size,
                )
                if cnt == 0:
                    print(f"  [warn]  zero rows — listing too new or upstream gap")
                    # Treat as completed (best-effort), but mark with 0 rows.
                    cp.mark_completed(pair, target_end_kst, 0, "")
                else:
                    cp.mark_completed(pair, latest_kst or target_end_kst, cnt, chk)
                fetched += 1
            except Exception as exc:  # noqa: BLE001 — fail-soft per pair
                conn.rollback()
                print(f"  [fail]  {type(exc).__name__}: {exc}", file=sys.stderr)
                failed += 1

    elapsed = time.monotonic() - started
    print()
    print("=" * 78)
    print(f"S6 bulk fetch summary — {elapsed:.1f}s elapsed")
    print(f"  fetched : {fetched}")
    print(f"  skipped : {skipped}")
    print(f"  failed  : {failed}")
    print(f"  total   : {len(pairs)}")
    print("=" * 78)

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
