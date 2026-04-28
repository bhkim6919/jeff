"""Backtest universe with survivorship enforcement (Jeff D4 G3).

Survivorship rule (DESIGN §4.3 + crypto_listings):
    A pair is in the universe on UTC trade date ``D`` iff
        (listed_at IS NULL OR listed_at <= D)
        AND
        (delisted_at IS NULL OR delisted_at >= D)

    Rationale:
    * ``listed_at IS NULL`` — manual_v0 entries from D2 don't carry listing
      dates. Treating them as "always listed" is conservative because the
      D1 OHLCV bulk only fetched data starting from each pair's actual
      first day, so pre-listing dates simply have no OHLCV rows (the
      data_loader returns shorter index, not NaN).
    * ``delisted_at >= D`` — the delisting day itself is tradable (last
      close print before the pair stops trading); pairs are excluded from
      the day AFTER ``delisted_at``.

D4 universe (Jeff Q4=A, "D4 한정"):
    KRWStaticTop100 — frozen snapshot from ``crypto/data/universe_top100.csv``
    (D1 captured 2026-04-27). This is *engine validation* universe, NOT a
    strategy-quality claim. Dynamic re-ranking is reserved for D5
    robustness work.
"""

from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Optional


_HERE = Path(__file__).resolve()
WORKTREE_ROOT = _HERE.parents[2]
DEFAULT_TOP100_CSV = WORKTREE_ROOT / "crypto" / "data" / "universe_top100.csv"


@dataclass(frozen=True)
class ListingRow:
    """Lean view of a crypto_listings row for universe filtering."""
    pair: str
    listed_at: Optional[date]
    delisted_at: Optional[date]


def _parse_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


class Universe(ABC):
    """Abstract universe — every backtest engine call site must go through
    one of these. Reserving the ABC here so Jeff's D5 robustness work has a
    drop-in point for ``KRWDynamicMonthly`` etc."""

    @abstractmethod
    def active_pairs(self, on_date: date) -> list[str]:
        """Return pairs in the universe on UTC trade date ``on_date``,
        sorted ascending for deterministic order."""

    @abstractmethod
    def name(self) -> str:
        """Short identifier used in evidence JSON / labels."""


class KRWStaticTop100(Universe):
    """D4 universe: static KRW Top 100 snapshot intersected with the
    survivorship-correct active set on each query.

    Construction:
        ``from_csv_and_listings(csv_path, listings)`` — load CSV pair list
        + the listings table (a list/iterable of ``ListingRow``).

    Tradability:
        ``active_pairs(D)`` returns the subset of the static Top 100 that
        was active on ``D`` per the survivorship rule. Pairs without any
        listings row are kept (treated as always-listed) so manual_v0 gaps
        don't silently shrink the universe.
    """

    def __init__(
        self,
        top100_pairs: list[str],
        listings_by_pair: dict[str, ListingRow],
        *,
        snapshot_dt_utc: Optional[date] = None,
    ) -> None:
        self._pairs = list(top100_pairs)
        self._listings = listings_by_pair
        self._snapshot_dt = snapshot_dt_utc

    def name(self) -> str:
        snap = self._snapshot_dt.isoformat() if self._snapshot_dt else "unknown"
        return f"krw_static_top100@{snap}"

    @property
    def all_pairs(self) -> list[str]:
        return list(self._pairs)

    def active_pairs(self, on_date: date) -> list[str]:
        out: list[str] = []
        for pair in self._pairs:
            row = self._listings.get(pair)
            if row is None:
                # No listings entry — treat as always listed (manual_v0 gap).
                out.append(pair)
                continue
            if row.listed_at is not None and row.listed_at > on_date:
                continue
            if row.delisted_at is not None and row.delisted_at < on_date:
                continue
            out.append(pair)
        return sorted(out)

    @classmethod
    def from_csv_and_listings(
        cls,
        csv_path: Path,
        listings: Iterable[ListingRow],
    ) -> "KRWStaticTop100":
        rows = _read_universe_csv(csv_path)
        snapshot_dt = (
            _parse_date(rows[0]["snapshot_dt_utc"]) if rows else None
        )
        pairs = [r["pair"] for r in rows]
        by_pair = {r.pair: r for r in listings}
        return cls(
            top100_pairs=pairs,
            listings_by_pair=by_pair,
            snapshot_dt_utc=snapshot_dt,
        )


def _read_universe_csv(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"universe CSV not found: {csv_path}")
    with csv_path.open(encoding="utf-8") as f:
        return [dict(r) for r in csv.DictReader(f)]


def load_listings_from_pg(conn) -> list[ListingRow]:
    """Read crypto_listings into ListingRow objects (read-only).

    The caller owns the connection; we just SELECT and convert.
    """
    sql = """
        SELECT pair, listed_at, delisted_at
        FROM crypto_listings
    """
    out: list[ListingRow] = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for pair, listed_at, delisted_at in cur.fetchall():
            out.append(
                ListingRow(
                    pair=pair,
                    listed_at=_parse_date(listed_at),
                    delisted_at=_parse_date(delisted_at),
                )
            )
    return out
