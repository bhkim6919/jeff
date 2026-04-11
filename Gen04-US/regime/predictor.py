# -*- coding: utf-8 -*-
"""
predictor.py — US Market Regime Prediction
============================================
Alpaca snapshots → normalized scores → composite → regime level.
Broker TR 최대 활용, 계산 최소화.
"""
from __future__ import annotations

import logging
from typing import Optional

from .models import (
    FEATURE_WEIGHTS, VIX_SCORES, RegimeLevel, REGIME_LABELS,
    score_to_regime,
)

logger = logging.getLogger("qtron.us.regime.predictor")


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _map_change_to_score(change_pct: float, scale: float = 2.0) -> float:
    """Map change% to [-1, +1]. scale=2.0 means ±2% → ±1.0."""
    return _clamp(change_pct / scale)


def _vix_to_score(level: float, change_pct: float = 0) -> float:
    """VIX level + spike penalty → [-1, +1]."""
    score = -1.0  # default: very high VIX
    for threshold, s in VIX_SCORES:
        if level < threshold:
            score = s
            break
    # Spike penalty: VIX up >20% in a day
    if change_pct > 20:
        score = min(score, score - 0.3)
    return _clamp(score)


def predict_regime(market_data: dict) -> dict:
    """
    Build regime prediction from collected market data.

    Returns prediction dict with scores, composite, regime level.
    """
    spy = market_data.get("spy", {})
    qqq = market_data.get("qqq", {})
    vix = market_data.get("vix", {})
    fx = market_data.get("fx", {})
    breadth = market_data.get("breadth", {})

    # ── 1. Index Score (SPY 70% + QQQ 30%) ──────────────
    index_score = 0.0
    index_avail = False

    if spy.get("available"):
        spy_score = _map_change_to_score(spy["change_pct"], scale=1.5)
        index_score = spy_score * 0.7
        index_avail = True

        if qqq.get("available"):
            qqq_score = _map_change_to_score(qqq["change_pct"], scale=2.0)
            index_score += qqq_score * 0.3
        else:
            index_score = spy_score  # SPY only

    # ── 2. Volatility Score (VIX) ───────────────────────
    vol_score = 0.0
    vol_avail = False

    if vix.get("available"):
        vol_score = _vix_to_score(vix["level"], vix.get("change_pct", 0))
        vol_avail = True

    # ── 3. Sector Breadth Score ─────────────────────────
    sector_score = 0.0
    sector_avail = False

    br = breadth.get("ratio", 0.5)
    if breadth.get("total", 0) > 0:
        # breadth_ratio: 0=all declining, 0.5=mixed, 1.0=all advancing
        sector_score = _clamp((br - 0.5) * 4)  # 0.75 → +1.0, 0.25 → -1.0
        sector_avail = True

    # ── 4. FX Score (UUP = USD strength, inverse) ───────
    fx_score = 0.0
    fx_avail = False

    if fx.get("available"):
        # USD strong (UUP up) → bearish for stocks
        fx_score = _clamp(-fx["uup_change_pct"] / 1.0)  # ±1% → ∓1.0
        fx_avail = True

    # ── Composite ───────────────────────────────────────
    scores = {
        "index":  (index_score,  index_avail,  FEATURE_WEIGHTS["index"]),
        "vol":    (vol_score,    vol_avail,    FEATURE_WEIGHTS["vol"]),
        "sector": (sector_score, sector_avail, FEATURE_WEIGHTS["sector"]),
        "fx":     (fx_score,     fx_avail,     FEATURE_WEIGHTS["fx"]),
    }

    weighted_sum = 0.0
    available_weight = 0.0

    for _name, (score, avail, weight) in scores.items():
        if avail:
            weighted_sum += score * weight
            available_weight += weight

    composite = weighted_sum / available_weight if available_weight > 0 else 0.0
    enough_data = available_weight >= 0.50

    if not enough_data:
        regime = RegimeLevel.NEUTRAL
        confidence_flag = "INSUFFICIENT"
    else:
        regime = score_to_regime(composite)
        confidence_flag = "FULL" if available_weight >= 0.80 else "PARTIAL"

    result = {
        "predicted_regime": int(regime),
        "predicted_label": REGIME_LABELS[regime],
        "composite_score": round(composite, 4),
        "index_score": round(index_score, 4),
        "index_avail": index_avail,
        "vol_score": round(vol_score, 4),
        "vol_avail": vol_avail,
        "sector_score": round(sector_score, 4),
        "sector_avail": sector_avail,
        "fx_score": round(fx_score, 4),
        "fx_avail": fx_avail,
        "available_weight": round(available_weight, 2),
        "confidence_flag": confidence_flag,
        "spy_change_pct": spy.get("change_pct", 0),
        "qqq_change_pct": qqq.get("change_pct", 0),
        "vix_level": vix.get("level", 0),
        "breadth_ratio": br,
    }

    logger.info(
        f"[REGIME] {result['predicted_label']} (score={composite:.3f}, "
        f"conf={confidence_flag}, avail={available_weight:.0%})"
    )

    return result


def predict_tomorrow(market_data: dict) -> dict:
    """
    Predict tomorrow's regime using today's closing data.
    Same logic as predict_regime but labeled as "tomorrow".
    """
    result = predict_regime(market_data)
    result["prediction_type"] = "tomorrow"
    return result
