"""
scoring.py — SHARED factor calculation module
===============================================
Backtest, batch, and live use this IDENTICAL code.
DO NOT duplicate these formulas anywhere else.

Formulas (from validated validate_gen4.py):
  vol_12m  = np.std(daily_returns[-252:])   # raw std, NOT annualized
  mom_12_1 = price[t-22] / price[t-252] - 1  # skip last 22 trading days

Note: raw std is sufficient for cross-sectional ranking (relative ordering).
      Annualization would not change the rank order.
"""
from __future__ import annotations
from math import sqrt
from typing import Dict, Optional

import numpy as np
import pandas as pd


def calc_volatility(close_series: pd.Series, lookback: int = 252) -> float:
    """
    12-month volatility: raw std of daily returns (NOT annualized).

    Formula: np.std(price_returns[-lookback:])
    Cross-sectional ranking only needs relative ordering.

    Returns NaN if insufficient data (<lookback or <10 valid returns).
    """
    if len(close_series) < lookback:
        return float("nan")
    prices = close_series.iloc[-lookback:].values.astype(float)
    rets = np.diff(prices) / prices[:-1]
    if len(rets) < 10:
        return float("nan")
    vol = float(np.std(rets))
    if vol <= 0:
        return float("nan")
    return vol


def calc_momentum(close_series: pd.Series,
                  lookback: int = 252, skip: int = 22) -> float:
    """
    12-1 month momentum (skip last month).

    Args:
        close_series: Daily close prices.
        lookback: Total lookback window (default 252).
        skip: Days to skip from end (default 22 = ~1 month).

    Returns:
        Momentum ratio (float), or NaN if insufficient data.

    Formula: c[-skip] / c[-lookback] - 1
    Matches validate_gen4.py line 135 exactly.
    """
    if len(close_series) < lookback:
        return float("nan")
    c = close_series.values.astype(float)
    c_skip = c[-skip]      # price ~22 days ago
    c_12m = c[-lookback]   # price ~252 days ago
    if c_12m <= 0 or c_skip <= 0:
        return float("nan")
    return c_skip / c_12m - 1


def score_universe(close_dict: Dict[str, pd.Series],
                   vol_lookback: int = 252,
                   mom_lookback: int = 252,
                   mom_skip: int = 22) -> pd.DataFrame:
    """
    Score all stocks in the universe.

    Args:
        close_dict: {ticker: pd.Series of close prices}
        vol_lookback, mom_lookback, mom_skip: scoring parameters.

    Returns:
        DataFrame with columns: ticker, vol_12m, mom_12_1
        Rows with NaN are included (caller decides filtering).
    """
    records = []
    for ticker, close_s in close_dict.items():
        vol = calc_volatility(close_s, vol_lookback)
        mom = calc_momentum(close_s, mom_lookback, mom_skip)
        records.append({"ticker": ticker, "vol_12m": vol, "mom_12_1": mom})
    return pd.DataFrame(records)
