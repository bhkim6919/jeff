# -*- coding: utf-8 -*-
"""
models.py — Regime enums, thresholds, shared constants
========================================================
Single source of truth for regime definitions.
"""
from __future__ import annotations

from enum import IntEnum
from typing import Dict, Tuple

# ── Regime Levels ─────────────────────────────────────────────

class RegimeLevel(IntEnum):
    STRONG_BEAR = 1
    BEAR = 2
    NEUTRAL = 3
    BULL = 4
    STRONG_BULL = 5


REGIME_LABELS = {
    1: "STRONG_BEAR",
    2: "BEAR",
    3: "NEUTRAL",
    4: "BULL",
    5: "STRONG_BULL",
}

REGIME_COLORS = {
    1: "#F04452",   # 빨강
    2: "#FF991F",   # 주황
    3: "#FFD600",   # 노랑
    4: "#36B37E",   # 연두
    5: "#00C853",   # 초록
}

# ── Feature Weights (sum = 1.0) ───────────────────────────────

FEATURE_WEIGHTS: Dict[str, float] = {
    "global": 0.30,
    "vol": 0.20,
    "domestic": 0.30,
    "micro": 0.10,
    "fx": 0.10,
}

# ── Composite → Regime Thresholds ─────────────────────────────
# Walk in order; first match wins. Outside all → STRONG_BULL.
# Source: initial calibration values. Adjust after 20+ data points.

REGIME_THRESHOLDS: list[Tuple[float, RegimeLevel]] = [
    (-0.40, RegimeLevel.STRONG_BEAR),
    (-0.15, RegimeLevel.BEAR),
    (+0.15, RegimeLevel.NEUTRAL),
    (+0.40, RegimeLevel.BULL),
    # > +0.40 → STRONG_BULL
]

# ── Actual Regime Thresholds (KOSPI change %) ─────────────────
# Used by actual.py — deliberately separate from predictor thresholds.

ACTUAL_THRESHOLDS: list[Tuple[float, RegimeLevel]] = [
    (-0.015, RegimeLevel.STRONG_BEAR),
    (-0.005, RegimeLevel.BEAR),
    (+0.005, RegimeLevel.NEUTRAL),
    (+0.015, RegimeLevel.BULL),
    # > +1.5% → STRONG_BULL
]

# ── Confidence Caps ───────────────────────────────────────────

GLOBAL_MISSING_CAP = 0.70          # 글로벌 데이터 없으면 최대 70%
MIN_AVAILABLE_WEIGHT = 0.50        # 이 미만이면 prediction unavailable
DIRECTION_PENALTY = 0.80           # global 없이 극단 예측 시 감점 계수

# ── KOSPI Source (Step 0 검증 완료) ───────────────────────────

INDEX_SOURCE = {
    "name": "코스피",
    "api": "ka20001",
    "params": {"mrkt_tp": "0", "inds_cd": "001"},
    "scale": 1.0,  # raw value = actual value (confirmed: 5817.21 vs yfinance 5818.70)
    "verified": True,
    "verified_date": "2026-04-08",
    "verification_note": "ka20001 vs yfinance ^KS11: 99.97% match",
}

# ── Staleness ─────────────────────────────────────────────────

STALE_THRESHOLD_SEC = 300          # 5분
EXPIRED_THRESHOLD_SEC = 3600       # 1시간

# ── Collector Result Type (documentation) ─────────────────────
# {
#   "ok": bool,
#   "data": {...},
#   "source_ts": float,
#   "read_ts": float,
#   "stale": bool,
#   "expired": bool,
#   "from_cache": bool,
#   "error": str | None,
#   "source": str,
# }


def score_to_regime(composite: float) -> RegimeLevel:
    """Map composite score to regime level."""
    for threshold, level in REGIME_THRESHOLDS:
        if composite <= threshold:
            return level
    return RegimeLevel.STRONG_BULL


def change_to_regime(change_pct: float) -> RegimeLevel:
    """Map KOSPI change % (decimal, e.g. 0.015 = 1.5%) to actual regime."""
    for threshold, level in ACTUAL_THRESHOLDS:
        if change_pct <= threshold:
            return level
    return RegimeLevel.STRONG_BULL
