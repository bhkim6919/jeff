"""Universe Top 100 builder for Crypto Lab D1.

Selection rule (per Jeff S5 conditions, 2026-04-27):
    1. /v1/market/all → KRW- prefix filter only
    2. /v1/ticker → sort by acc_trade_price_24h DESC, take Top 100
    3. NO arbitrary exclusions (no stable/anomaly filters → avoid
       survivorship + selection bias). cf. DESIGN.md §2.3.
    4. CSV ↔ DB crypto_universe_top100 must agree on (rank, pair, value_krw_24h).
    5. Raw ticker snapshot + SHA256 checksum kept for reproducibility.

This module is pure data assembly. Persistence is the caller's responsibility
(see scripts/crypto/build_universe_top100.py).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from crypto.data.upbit_provider import (
    KRW_MARKET_PREFIX,
    UpbitQuotationProvider,
)


UNIVERSE_SIZE = 100


@dataclass(frozen=True)
class UniverseRow:
    """A single universe entry (rank → pair → 24h trade value)."""

    rank: int
    pair: str
    value_krw_24h: float


@dataclass(frozen=True)
class UniverseSnapshot:
    """Full universe build result for a single capture."""

    snapshot_dt_utc: str            # ISO date (YYYY-MM-DD)
    captured_at: str                # ISO timestamp UTC, seconds precision
    universe: list[UniverseRow]
    raw_tickers: list[dict[str, Any]]
    raw_checksum_sha256: str        # hex digest of canonical raw_tickers JSON

    def to_csv_rows(self) -> list[dict[str, Any]]:
        """Materialize CSV-ready dicts (matches DB schema)."""
        return [
            {
                "snapshot_dt_utc": self.snapshot_dt_utc,
                "rank": row.rank,
                "pair": row.pair,
                "value_krw_24h": f"{row.value_krw_24h:.2f}",
                "captured_at": self.captured_at,
            }
            for row in self.universe
        ]


def _canonical_json_bytes(payload: list[dict[str, Any]]) -> bytes:
    """Deterministic JSON serialization for checksum."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_universe_top100(
    provider: UpbitQuotationProvider,
    *,
    captured_at_utc: datetime | None = None,
) -> UniverseSnapshot:
    """Build the Top 100 KRW spot universe by 24h trade value.

    Args:
        provider: read-only Upbit Quotation client.
        captured_at_utc: optional override for the capture timestamp (UTC).
            Defaults to ``datetime.now(timezone.utc)``. The date portion is
            stored as ``snapshot_dt_utc`` to align with DESIGN.md §5.3
            snapshot_version semantics.

    Returns:
        UniverseSnapshot with up to 100 ranked rows. May contain fewer than
        100 entries if Upbit currently lists fewer KRW markets — caller is
        expected to surface a warning in that case.
    """
    if captured_at_utc is None:
        captured_at_utc = datetime.now(timezone.utc)

    # Step 1 — list active KRW markets only (Jeff condition #1).
    krw_markets = provider.list_krw_markets()
    pair_codes = [m["market"] for m in krw_markets if m.get("market", "").startswith(KRW_MARKET_PREFIX)]

    # Step 2 — fetch tickers for the full KRW universe.
    raw_tickers = provider.fetch_tickers(pair_codes)

    # Step 3 — sort by acc_trade_price_24h DESC (Jeff condition #2).
    # acc_trade_price_24h is the 24h cumulative quote-currency volume in KRW.
    def _sort_key(t: dict[str, Any]) -> float:
        v = t.get("acc_trade_price_24h")
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    sorted_tickers = sorted(raw_tickers, key=_sort_key, reverse=True)

    # Step 4 — take Top N WITHOUT applying additional filters
    # (Jeff condition #3: no stable/anomaly exclusion at universe-build time).
    top_n = sorted_tickers[:UNIVERSE_SIZE]
    universe = [
        UniverseRow(rank=i + 1, pair=t["market"], value_krw_24h=_sort_key(t))
        for i, t in enumerate(top_n)
    ]

    # Step 5 — checksum of canonical raw ticker payload (Jeff condition #5).
    checksum = hashlib.sha256(_canonical_json_bytes(raw_tickers)).hexdigest()

    return UniverseSnapshot(
        snapshot_dt_utc=captured_at_utc.date().isoformat(),
        captured_at=captured_at_utc.replace(microsecond=0).isoformat(),
        universe=universe,
        raw_tickers=raw_tickers,
        raw_checksum_sha256=checksum,
    )
