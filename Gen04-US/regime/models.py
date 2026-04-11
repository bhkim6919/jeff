# -*- coding: utf-8 -*-
"""
models.py — US Regime Constants & Enums
========================================
Single source of truth for regime levels, thresholds, colors.
Adapted from KR regime system for US market characteristics.
"""
from enum import IntEnum


class RegimeLevel(IntEnum):
    STRONG_BEAR = 1
    BEAR = 2
    NEUTRAL = 3
    BULL = 4
    STRONG_BULL = 5


REGIME_LABELS = {
    1: "STRONG BEAR",
    2: "BEAR",
    3: "NEUTRAL",
    4: "BULL",
    5: "STRONG BULL",
}

REGIME_COLORS = {
    1: "#F04452",   # Red
    2: "#FF991F",   # Orange
    3: "#FFD600",   # Yellow
    4: "#36B37E",   # Light Green
    5: "#00C853",   # Dark Green
}

# ── Feature Weights (sum = 1.0) ─────────────────────────
# US market: SPY/QQQ dominant, VIX important, DXY minor
FEATURE_WEIGHTS = {
    "index":  0.40,   # SPY + QQQ
    "vol":    0.25,   # VIX level + spike
    "sector": 0.20,   # Sector breadth (advance/decline across 11 sectors)
    "fx":     0.15,   # DXY (USD strength, inverse)
}

# ── Composite Score → Regime Thresholds ─────────────────
# More conservative for US (lower vol than KR)
REGIME_THRESHOLDS = [
    (-0.40, RegimeLevel.STRONG_BEAR),
    (-0.15, RegimeLevel.BEAR),
    (+0.15, RegimeLevel.NEUTRAL),
    (+0.40, RegimeLevel.BULL),
]
# > +0.40 → STRONG_BULL

# ── Actual Regime Thresholds (SPY daily change %) ───────
ACTUAL_THRESHOLDS = [
    (-1.5, RegimeLevel.STRONG_BEAR),
    (-0.5, RegimeLevel.BEAR),
    (+0.5, RegimeLevel.NEUTRAL),
    (+1.5, RegimeLevel.BULL),
]
# > +1.5% → STRONG_BULL

# ── VIX Level Scoring ──────────────────────────────────
VIX_SCORES = [
    (15, +0.8),   # < 15: very bullish
    (20, +0.3),   # 15-20: mildly bullish
    (25, -0.3),   # 20-25: mildly bearish
    (30, -0.6),   # 25-30: bearish
]
# > 30: -1.0

# ── Sector ETFs (SPDR Select Sector) ───────────────────
SECTOR_ETFS = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLE": "Energy",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLC": "Communication",
    "XLY": "Consumer Disc.",
    "XLP": "Consumer Staples",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLB": "Materials",
}

# ── Market Indices ──────────────────────────────────────
MARKET_SYMBOLS = {
    "spy": "SPY",
    "qqq": "QQQ",
    "vix": "^VIX",       # Yahoo only (Alpaca doesn't have VIX)
    "dxy": "UUP",        # USD Bull ETF as DXY proxy (Alpaca tradeable)
}


def score_to_regime(composite: float) -> RegimeLevel:
    """Map composite score [-1, +1] to regime level."""
    for threshold, level in REGIME_THRESHOLDS:
        if composite <= threshold:
            return level
    return RegimeLevel.STRONG_BULL


def actual_to_regime(spy_change_pct: float) -> RegimeLevel:
    """Map SPY daily change % to actual regime level."""
    for threshold, level in ACTUAL_THRESHOLDS:
        if spy_change_pct <= threshold:
            return level
    return RegimeLevel.STRONG_BULL
