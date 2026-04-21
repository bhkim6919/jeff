"""
market_context.py — Lab Live market-context resolver
====================================================
Reconstructed after pre-commit stash incident (2026-04-20) wiped this untracked file.
Contract derived from call sites in `engine.py` and `app.py:/api/debug/market_context`.

Responsibilities:
1. `build_stock_dates_from_csv(all_data)` — union date axis across all loaded stocks.
   Replaces the old `dates = idx_df["date"]` (KOSPI-keyed) bug where KOSPI stale
   would drop every stock row that only had today's price.
2. `SupplyStatus` — string enum matching `lifecycle.batch._classify_kospi_supply`.
3. `resolve_effective_trade_date(...)` — returns a `MarketContext` describing the
   effective trade date (stock-union based) plus KOSPI readiness flag. Engine uses
   `run_mode` for logging but does NOT block execution on DEGRADED.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


class SupplyStatus:
    """KOSPI supply status string constants (mirrors batch._classify_kospi_supply)."""
    SUCCESS_TODAY = "SUCCESS_TODAY"
    SUCCESS_STALE = "SUCCESS_STALE"
    EMPTY_NOT_READY = "EMPTY_NOT_READY"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


def build_stock_dates_from_csv(all_data: Dict[str, pd.DataFrame]) -> pd.Series:
    """Union date axis across per-stock OHLCV frames.

    Each value in `all_data` is a DataFrame with a 'date' column (pd.Timestamp).
    Returns a sorted, de-duplicated Series of Timestamps. Empty Series if no data.
    """
    if not all_data:
        return pd.Series(dtype="datetime64[ns]")

    seen: set = set()
    for df in all_data.values():
        if df is None or df.empty or "date" not in df.columns:
            continue
        for ts in pd.to_datetime(df["date"], errors="coerce").dropna():
            seen.add(ts)
    if not seen:
        return pd.Series(dtype="datetime64[ns]")
    return pd.Series(sorted(seen))


@dataclass
class MarketContext:
    run_mode: str = "NORMAL"                # "NORMAL" | "DEGRADED"
    effective_trade_date: Optional[str] = None
    stock_last_date: Optional[str] = None
    kospi_last_date: Optional[str] = None
    kospi_ready: bool = True
    supply_status: Optional[str] = None
    degraded_reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["index_ready"] = self.kospi_ready
        return d


def _series_last_date_str(dates) -> Optional[str]:
    try:
        if dates is None:
            return None
        s = pd.Series(dates)
        s = pd.to_datetime(s, errors="coerce").dropna()
        if s.empty:
            return None
        return str(s.max().date())
    except Exception:
        return None


def resolve_effective_trade_date(
    stock_dates: Optional[Iterable] = None,
    kospi_dates: Optional[Iterable] = None,
    supply_status: Optional[str] = None,
) -> MarketContext:
    """Decide the effective trade date + KOSPI readiness.

    stock-union drives effective_trade_date; KOSPI is advisory only (DEGRADED flag).
    Engine continues to run even when DEGRADED; this mirrors the existing behavior
    (ffill KOSPI onto stock dates so charts still render).
    """
    stock_last = _series_last_date_str(stock_dates)
    kospi_last = _series_last_date_str(kospi_dates)

    reasons: List[str] = []
    if stock_last is None:
        reasons.append("no_stock_dates")
    if kospi_last is None:
        reasons.append("no_kospi_dates")
    if supply_status in (SupplyStatus.ERROR, SupplyStatus.TIMEOUT, SupplyStatus.EMPTY_NOT_READY):
        reasons.append(f"kospi_supply:{supply_status}")
    if stock_last and kospi_last and kospi_last < stock_last:
        reasons.append(f"kospi_stale(kospi={kospi_last},stock={stock_last})")

    kospi_ready = (
        kospi_last is not None
        and stock_last is not None
        and kospi_last >= stock_last
        and supply_status in (SupplyStatus.SUCCESS_TODAY, SupplyStatus.SUCCESS_STALE, None)
    )
    run_mode = "NORMAL" if (kospi_ready and not reasons) else "DEGRADED"
    # supply_status None is treated as "engine didn't classify" — keep NORMAL if dates align.
    if run_mode == "DEGRADED" and not reasons and supply_status is None:
        run_mode = "NORMAL"

    return MarketContext(
        run_mode=run_mode,
        effective_trade_date=stock_last,
        stock_last_date=stock_last,
        kospi_last_date=kospi_last,
        kospi_ready=kospi_ready,
        supply_status=supply_status,
        degraded_reasons=reasons,
    )
