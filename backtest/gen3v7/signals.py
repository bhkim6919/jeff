"""
signals.py — Gen3 v7 signal generation (RS, ATR, breakout, gap, CVaR)
======================================================================
Isolated from Gen04. Uses only pandas/numpy.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict, Optional


# ── RS Composite ─────────────────────────────────────────────────

def calc_rs_raw(close: pd.Series) -> Dict[str, pd.Series]:
    """Calculate raw RS values (20/60/120 day returns)."""
    return {
        "rs20": close / close.shift(20) - 1,
        "rs60": close / close.shift(60) - 1,
        "rs120": close / close.shift(120) - 1,
    }


def rank_rs_universe(
    all_rs: Dict[str, Dict[str, float]],
    weights: tuple = (0.30, 0.50, 0.20),
) -> Dict[str, float]:
    """Rank RS composite across universe. Returns {ticker: rs_composite}.

    Args:
        all_rs: {ticker: {"rs20": val, "rs60": val, "rs120": val}}
        weights: (w20, w60, w120)
    """
    tickers = list(all_rs.keys())
    if not tickers:
        return {}

    w20, w60, w120 = weights

    # Collect raw values
    rs20_vals = {t: all_rs[t].get("rs20") for t in tickers}
    rs60_vals = {t: all_rs[t].get("rs60") for t in tickers}
    rs120_vals = {t: all_rs[t].get("rs120") for t in tickers}

    # Percentile rank (handle NaN: rank only non-NaN, NaN stays NaN)
    def pct_rank(vals: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        valid = {t: v for t, v in vals.items() if v is not None and not np.isnan(v)}
        if not valid:
            return {t: None for t in vals}
        s = pd.Series(valid)
        ranked = s.rank(pct=True)
        result = {}
        for t in vals:
            result[t] = ranked.get(t)
        return result

    r20 = pct_rank(rs20_vals)
    r60 = pct_rank(rs60_vals)
    r120 = pct_rank(rs120_vals)

    # Weighted composite (NaN-aware: normalize by available weights)
    result = {}
    for t in tickers:
        parts = []
        w_sum = 0.0
        if r20.get(t) is not None:
            parts.append(w20 * r20[t])
            w_sum += w20
        if r60.get(t) is not None:
            parts.append(w60 * r60[t])
            w_sum += w60
        if r120.get(t) is not None:
            parts.append(w120 * r120[t])
            w_sum += w120
        if w_sum > 0:
            result[t] = sum(parts) / w_sum
        else:
            result[t] = np.nan
    return result


# ── Technical Indicators ─────────────────────────────────────────

def calc_atr(high: pd.Series, low: pd.Series, close: pd.Series,
             period: int = 20) -> pd.Series:
    """Average True Range."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def calc_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add all signal columns to a ticker's OHLCV DataFrame.

    Input columns: date, open, high, low, close, volume
    Output: adds rs20, rs60, rs120, atr20, atr_pct, ma20, above_ma20,
            volume_ma20, vol_ratio, high_252, breakout, is_52w_high,
            gap_pct, gap_blocked, pb_score, signal_entry, signal_exit
    """
    df = df.copy()
    c = df["close"]

    # RS raw
    df["rs20"] = c / c.shift(20) - 1
    df["rs60"] = c / c.shift(60) - 1
    df["rs120"] = c / c.shift(120) - 1

    # ATR
    df["atr20"] = calc_atr(df["high"], df["low"], c, 20)
    df["atr_pct"] = df["atr20"] / c

    # MA & volume
    df["ma20"] = c.rolling(20).mean()
    df["above_ma20"] = (c > df["ma20"]).astype(int)
    df["volume_ma20"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["volume_ma20"]

    # Breakout & 52-week high
    df["high_252"] = c.shift(1).rolling(252).max()
    df["breakout"] = (c >= c.shift(1).rolling(20).max()).astype(int)
    df["is_52w_high"] = (c >= df["high_252"] * 0.95).astype(int)

    # Gap
    df["prev_close"] = c.shift(1)
    df["gap_pct"] = df["open"] / df["prev_close"] - 1
    df["gap_blocked"] = ((df["gap_pct"] > 0.08) & (df["vol_ratio"] < 1.30)).astype(int)

    # Pullback score
    pct_from_high = (df["high_252"] - c) / df["high_252"]
    df["pb_score"] = np.where((pct_from_high >= 0.03) & (pct_from_high <= 0.07), 5.0, 0.0)

    # Entry/exit signals (before RS ranking — will be refined with rs_composite)
    # These are placeholder; actual entry uses rs_composite from universe ranking
    df["signal_entry"] = ((df["breakout"] == 1)).astype(int)
    df["signal_exit"] = 0  # Set after RS ranking

    return df


# ── CVaR (LTR v2) ───────────────────────────────────────────────

def calc_cvar(
    returns: pd.Series,
    window: int = 252,
    percentile: float = 0.05,
    min_data_ratio: float = 0.80,
) -> Optional[float]:
    """Calculate CVaR (Expected Shortfall) for a return series.

    Args:
        returns: daily returns (not including current day)
        window: lookback window
        percentile: VaR threshold (0.05 = 5%)
        min_data_ratio: minimum data completeness

    Returns:
        CVaR (negative number) or None if insufficient data
    """
    recent = returns.iloc[-window:] if len(returns) >= window else returns
    recent = recent.dropna()

    if len(recent) < int(window * min_data_ratio):
        return None

    threshold = recent.quantile(percentile)
    tail = recent[recent <= threshold]
    if len(tail) == 0:
        return None
    return float(tail.mean())


def rank_cvar_universe(cvar_values: Dict[str, Optional[float]]) -> Dict[str, float]:
    """Rank CVaR across universe. Higher rank = more risky.

    Returns:
        {ticker: cvar_rank (0~1)}
    """
    valid = {t: v for t, v in cvar_values.items() if v is not None}
    if not valid:
        return {t: 0.5 for t in cvar_values}

    s = pd.Series(valid)
    raw_rank = s.rank(ascending=True, pct=True)  # small (negative) = low rank
    cvar_rank = 1.0 - raw_rank  # flip: risky = high rank

    result = {}
    for t in cvar_values:
        if t in cvar_rank.index:
            result[t] = float(cvar_rank[t])
        else:
            result[t] = 0.5  # default for missing
    return result
