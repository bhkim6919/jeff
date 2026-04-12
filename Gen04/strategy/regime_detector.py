"""
regime_detector.py -- Market Regime Detection for Gen4 Ver.02
=============================================================
Provides BEAR/SIDE/BULL classification for Emergency Rebalance (Strategy A).

Regime is determined by:
  1. KOSPI vs its 200-day MA
  2. Market breadth: % of stocks trading above their own 200-day MA

Asymmetric rule (default):
  BULL: KOSPI > MA200 AND breadth > 60%
  BEAR: KOSPI < MA200 OR  breadth < 40%
  SIDE: else

This module is intentionally minimal and stateless.
Regime history / transition tracking is done by the caller (main.py).
"""
from __future__ import annotations
import logging
from typing import Optional, Dict

import numpy as np
import pandas as pd

logger = logging.getLogger("gen4.regime")

# ── Constants (match backtester_regime_v3.py exactly) ────────────────────────
EXPOSURE_MAP = {"BULL": 1.0, "SIDE": 0.7, "BEAR": 0.4}
BREADTH_BULL = 0.60
BREADTH_BEAR = 0.40
MA_WINDOW = 200


def calc_regime(kospi_close: float, kospi_ma200: float,
                breadth: float) -> str:
    """
    Classify market regime.

    Args:
        kospi_close: Today's KOSPI closing price
        kospi_ma200: KOSPI 200-day moving average
        breadth: Fraction of stocks above their own 200-day MA (0.0~1.0)

    Returns:
        "BULL", "SIDE", or "BEAR"
    """
    if kospi_ma200 <= 0 or np.isnan(kospi_ma200):
        return "SIDE"

    kospi_above = kospi_close > kospi_ma200
    breadth_healthy = breadth > BREADTH_BULL
    breadth_weak = breadth < BREADTH_BEAR

    if kospi_above and breadth_healthy:
        return "BULL"
    if (not kospi_above) or breadth_weak:
        return "BEAR"
    return "SIDE"


def calc_breadth_from_prices(stock_closes: Dict[str, float],
                              stock_ma200s: Dict[str, float]) -> float:
    """
    Calculate market breadth from current prices and MA200 values.

    Args:
        stock_closes: {ticker: close_price}
        stock_ma200s: {ticker: 200-day MA}

    Returns:
        Fraction of valid stocks trading above their MA200 (0.0~1.0)
    """
    above = 0
    total = 0
    for tk in stock_closes:
        close = stock_closes[tk]
        ma200 = stock_ma200s.get(tk, 0)
        if close > 0 and ma200 > 0:
            total += 1
            if close > ma200:
                above += 1
    if total == 0:
        return 0.5  # default when no data
    return above / total


def get_target_exposure(regime: str) -> float:
    """Get target exposure ratio for given regime."""
    return EXPOSURE_MAP.get(regime, 1.0)
