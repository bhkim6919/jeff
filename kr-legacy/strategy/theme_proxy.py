"""
theme_proxy.py — Theme Proxy strategy: price/volume pattern-based
==================================================================
Detects "theme-like" movements without news data.
Uses only OHLCV + volume for candidate selection and scoring.

Strategy: simplified theme proxy (volume surge + momentum + high proximity)
This is the "b version" — pure OHLCV-based, no sector_map/cluster.
Original "a version" with sector_map existed in earlier session but was not saved.

NOT integrated into Core Gen4 engine. Backtest comparison only.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple


def compute_theme_score(close: np.ndarray, volume: np.ndarray,
                        high: np.ndarray) -> Optional[dict]:
    """
    Score a single stock for theme-like characteristics.

    Args:
        close: daily close prices (at least 25 elements)
        volume: daily volume
        high: daily high prices

    Returns:
        dict with score components, or None if insufficient data / filtered out.
    """
    n = len(close)
    if n < 25 or close[-1] <= 0:
        return None

    # ── Volume surge ────────────────────────────────────────────
    amount = close * volume  # daily traded value proxy
    amt_20avg = np.mean(amount[-21:-1]) if n >= 21 else np.mean(amount[:-1])
    if amt_20avg <= 0:
        return None
    amt_ratio = amount[-1] / amt_20avg  # today vs 20d avg

    # 5-day average amount ratio
    amt_5avg = np.mean(amount[-6:-1]) if n >= 6 else amt_20avg
    amt_5_ratio = amt_5avg / amt_20avg if amt_20avg > 0 else 0

    # ── Short-term momentum ─────────────────────────────────────
    ret_5d = close[-1] / close[-6] - 1 if n >= 6 else 0
    ret_1d = close[-1] / close[-2] - 1 if n >= 2 else 0

    # ── 20-day high proximity ───────────────────────────────────
    high_20d = np.max(high[-20:]) if n >= 20 else np.max(high)
    pct_from_high = (close[-1] / high_20d - 1) if high_20d > 0 else -1

    # ── Score components ────────────────────────────────────────
    # Volume surge score (0~40)
    vol_score = min(amt_ratio / 3.0, 1.0) * 30 + min(amt_5_ratio / 2.0, 1.0) * 10

    # Momentum score (0~30)
    mom_score = min(max(ret_5d, 0) / 0.15, 1.0) * 30

    # High proximity score (0~30): closer to 20d high = higher score
    if pct_from_high >= 0:
        # At or above 20d high = breakout
        hi_score = 30.0
    elif pct_from_high >= -0.03:
        # Within 3% of high
        hi_score = 20.0 + (pct_from_high + 0.03) / 0.03 * 10
    else:
        hi_score = max(0, 10.0 + pct_from_high * 100)

    total = vol_score + mom_score + hi_score

    return {
        "amt_ratio": round(amt_ratio, 2),
        "amt_5_ratio": round(amt_5_ratio, 2),
        "ret_5d": round(ret_5d, 4),
        "ret_1d": round(ret_1d, 4),
        "pct_from_high": round(pct_from_high, 4),
        "vol_score": round(vol_score, 1),
        "mom_score": round(mom_score, 1),
        "hi_score": round(hi_score, 1),
        "theme_score": round(total, 1),
    }


def select_theme_candidates(
    date_idx: int,
    codes: List[str],
    close_matrix: Dict[str, np.ndarray],
    volume_matrix: Dict[str, np.ndarray],
    high_matrix: Dict[str, np.ndarray],
    *,
    min_price: float = 3000,
    min_amt_ratio: float = 3.0,
    min_ret_5d_pctile: float = 0.90,
    max_stocks: int = 5,
) -> List[dict]:
    """
    Select theme proxy candidates for a given trading day.

    Args:
        date_idx: index into the arrays (today)
        codes: list of stock codes
        close_matrix: {code: np.array of close prices}
        volume_matrix: {code: np.array of volume}
        high_matrix: {code: np.array of high prices}
        min_price: minimum close price filter
        min_amt_ratio: minimum volume surge ratio
        min_ret_5d_pctile: minimum 5d return percentile (0.90 = top 10%)
        max_stocks: maximum number of candidates

    Returns:
        List of candidate dicts sorted by theme_score descending.
    """
    scores = []

    for code in codes:
        c = close_matrix.get(code)
        v = volume_matrix.get(code)
        h = high_matrix.get(code)
        if c is None or v is None or h is None:
            continue
        if date_idx >= len(c):
            continue

        # Slice up to date_idx (inclusive) — no lookahead
        cs = c[:date_idx + 1]
        vs = v[:date_idx + 1]
        hs = h[:date_idx + 1]

        if len(cs) < 25:
            continue

        # Price filter
        if cs[-1] < min_price:
            continue

        # Common stock filter: 6-digit numeric ending in 0
        if not (len(code) == 6 and code.isdigit() and code[-1] == '0'):
            continue

        result = compute_theme_score(cs, vs, hs)
        if result is None:
            continue

        # Volume surge filter
        if result["amt_ratio"] < min_amt_ratio:
            continue

        result["code"] = code
        result["close"] = float(cs[-1])
        scores.append(result)

    if not scores:
        return []

    # 5d return percentile filter
    ret_5ds = [s["ret_5d"] for s in scores]
    if ret_5ds:
        threshold = np.percentile(ret_5ds, min_ret_5d_pctile * 100)
        scores = [s for s in scores if s["ret_5d"] >= threshold]

    # Sort by theme_score descending, take top N
    scores.sort(key=lambda x: x["theme_score"], reverse=True)
    return scores[:max_stocks]
