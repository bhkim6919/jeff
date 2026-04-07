"""
kospi_utils.py — KOSPI benchmark comparison utilities
======================================================
Shared by daily/weekly/monthly reports.

Data source: backtest/data_full/index/KOSPI.csv (index, Close)
Fallback: pykrx KODEX 200 ETF (069500)
"""
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger("gen4.kospi_utils")

_cache: Dict[str, pd.Series] = {}


def load_kospi_close(index_file: Path) -> pd.Series:
    """Load KOSPI daily close prices. Returns Series indexed by 'YYYY-MM-DD' strings."""
    cache_key = str(index_file)
    if cache_key in _cache:
        return _cache[cache_key]

    if not index_file.exists():
        logger.warning(f"KOSPI index file not found: {index_file}")
        return pd.Series(dtype=float)

    try:
        df = pd.read_csv(index_file, encoding="utf-8-sig")
        col_date = "index" if "index" in df.columns else df.columns[0]
        col_close = "Close" if "Close" in df.columns else "close"

        df[col_date] = pd.to_datetime(df[col_date]).dt.strftime("%Y-%m-%d")
        series = df.set_index(col_date)[col_close].astype(float)
        series = series[series > 0]
        _cache[cache_key] = series
        return series
    except Exception as e:
        logger.warning(f"Failed to load KOSPI: {e}")
        return pd.Series(dtype=float)


def get_kospi_return(kospi: pd.Series, date_str: str) -> Optional[float]:
    """Get KOSPI daily return for a given date. Returns None if unavailable."""
    if kospi.empty or date_str not in kospi.index:
        return None
    try:
        dates = kospi.index.tolist()
        idx = dates.index(date_str)
        if idx == 0:
            return None
        prev = dates[idx - 1]
        cur = float(kospi[date_str])
        prv = float(kospi[prev])
        if prv <= 0:
            return None
        return (cur / prv - 1)
    except (ValueError, IndexError):
        return None


def get_kospi_period_return(kospi: pd.Series,
                             start_date: str, end_date: str) -> Optional[float]:
    """Get KOSPI return over a period. Finds nearest trading days."""
    if kospi.empty:
        return None

    dates = sorted(kospi.index.tolist())

    # Find start value: exact match or nearest previous trading day
    start_val = None
    for d in reversed(dates):
        if d <= start_date:
            start_val = float(kospi[d])
            break

    # Find nearest date <= end
    end_val = None
    for d in reversed(dates):
        if d <= end_date:
            end_val = float(kospi[d])
            break

    if start_val is None or end_val is None or start_val <= 0:
        return None
    return (end_val / start_val - 1)


def get_kospi_close_on(kospi: pd.Series, date_str: str) -> Optional[float]:
    """Get KOSPI close on a specific date (or nearest previous)."""
    if kospi.empty:
        return None
    if date_str in kospi.index:
        return float(kospi[date_str])
    # Find nearest previous
    dates = sorted(kospi.index.tolist())
    for d in reversed(dates):
        if d <= date_str:
            return float(kospi[d])
    return None


def compute_excess_return(port_return: float,
                           kospi_return: Optional[float]) -> Tuple[float, str]:
    """Compute excess return and label.
    Returns (excess_pct, label).
    """
    if kospi_return is None:
        return 0.0, "N/A"
    excess = port_return - kospi_return
    if abs(excess) < 0.0005:  # < 0.05%
        label = "In-line"
    elif excess > 0:
        label = "Outperform"
    else:
        label = "Underperform"
    return excess, label


def inject_kospi_close(index_file: Path, date_str: str, close: float) -> None:
    """Inject today's KOSPI close into memory cache AND append to CSV file.

    Ensures both live-session reports and manual re-generation use correct data.
    """
    cache_key = str(index_file)
    if cache_key not in _cache:
        load_kospi_close(index_file)
    if cache_key in _cache:
        _cache[cache_key][date_str] = close
        logger.info("KOSPI close injected: %s = %.2f", date_str, close)
    else:
        logger.warning("Cannot inject KOSPI — cache not initialized for %s", index_file)

    # Also append to CSV file (for offline report regeneration)
    try:
        index_path = Path(index_file)
        if index_path.exists():
            # Check if date already exists in file
            with open(index_path, "r") as f:
                last_line = ""
                for line in f:
                    last_line = line.strip()
            if last_line and last_line.startswith(date_str):
                return  # already written
            # Append: date, open=close, high=close, low=close, close, volume=0
            with open(index_path, "a", newline="") as f:
                f.write(f"\n{date_str},{close:.2f},{close:.2f},{close:.2f},{close:.2f},0")
            logger.info("KOSPI close appended to file: %s = %.2f", date_str, close)
    except Exception as e:
        logger.warning("KOSPI file append failed: %s (non-critical)", e)


def count_outperform_days(equity_df: pd.DataFrame,
                           kospi: pd.Series) -> Tuple[int, int]:
    """Count days where portfolio outperformed KOSPI.
    Returns (outperform_days, total_days).
    """
    if equity_df.empty or "date" not in equity_df.columns:
        return 0, 0
    if "daily_pnl_pct" not in equity_df.columns:
        return 0, 0

    out = 0
    total = 0
    for _, row in equity_df.iterrows():
        dt = str(row["date"])
        port_ret = float(row.get("daily_pnl_pct", 0))
        k_ret = get_kospi_return(kospi, dt)
        if k_ret is not None:
            total += 1
            if port_ret > k_ret:
                out += 1
    return out, total
