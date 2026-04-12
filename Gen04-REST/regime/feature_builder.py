# -*- coding: utf-8 -*-
"""
feature_builder.py — 5축 점수 빌더
====================================
각 축 [-1, +1] 범위. unavailable 시 (0, False) 반환.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from regime.models import FEATURE_WEIGHTS, MIN_AVAILABLE_WEIGHT


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _safe_change(result: Dict[str, Any]) -> Tuple[float, bool]:
    """Extract change_pct from collector result. Returns (change, available)."""
    if not result.get("ok") or not result.get("data"):
        return 0.0, False
    change = result["data"].get("change_pct")
    if change is None or change == "":
        return 0.0, False
    return float(change), True


def build_global_score(sp500: Dict, nasdaq: Dict) -> Tuple[float, bool]:
    """SPX ±2% → ±1.0 선형, NASDAQ 보조 (0.3 weight)."""
    spx_chg, spx_ok = _safe_change(sp500)
    ndx_chg, ndx_ok = _safe_change(nasdaq)

    if not spx_ok and not ndx_ok:
        return 0.0, False

    score = 0.0
    if spx_ok:
        score += _clamp(spx_chg / 0.02) * 0.7  # ±2% → ±1.0, weight 70%
    if ndx_ok:
        score += _clamp(ndx_chg / 0.02) * 0.3  # weight 30%

    # If only one available, scale up
    if spx_ok and not ndx_ok:
        score = score / 0.7
    elif ndx_ok and not spx_ok:
        score = score / 0.3

    return round(_clamp(score), 4), True


def build_vol_score(vix: Dict) -> Tuple[float, bool]:
    """VIX level + VIX change → score."""
    if not vix.get("ok") or not vix.get("data"):
        return 0.0, False

    data = vix["data"]
    close = data.get("close")
    change_pct = data.get("change_pct")

    if close is None or close <= 0:
        return 0.0, False

    # VIX level score
    if close < 15:
        level_score = 0.8
    elif close < 20:
        level_score = 0.3
    elif close < 25:
        level_score = -0.3
    elif close < 30:
        level_score = -0.6
    else:
        level_score = -1.0

    # VIX spike penalty
    spike_penalty = 0.0
    if change_pct is not None and change_pct > 0.20:
        spike_penalty = -0.3

    score = _clamp(level_score + spike_penalty)
    return round(score, 4), True


def build_domestic_score(kospi: Dict, kosdaq: Dict) -> Tuple[float, bool]:
    """KOSPI change ±2% → ±1.0 선형, KOSDAQ 보조."""
    kospi_chg, kospi_ok = _safe_change(kospi)
    kosdaq_chg, kosdaq_ok = _safe_change(kosdaq)

    if not kospi_ok and not kosdaq_ok:
        return 0.0, False

    score = 0.0
    if kospi_ok:
        score += _clamp(kospi_chg / 0.02) * 0.7
    if kosdaq_ok:
        score += _clamp(kosdaq_chg / 0.02) * 0.3

    if kospi_ok and not kosdaq_ok:
        score = score / 0.7
    elif kosdaq_ok and not kospi_ok:
        score = score / 0.3

    # Breadth bonus (if available)
    if kospi_ok and kospi.get("data"):
        rising = kospi["data"].get("rising", 0)
        falling = kospi["data"].get("falling", 0)
        total = rising + falling
        if total > 0:
            breadth = (rising - falling) / total
            score = _clamp(score + breadth * 0.2)

    return round(_clamp(score), 4), True


def build_micro_score(strength: Dict) -> Tuple[float, bool]:
    """체결강도 100 중심. >120 bullish, <80 bearish."""
    if not strength.get("ok") or not strength.get("data"):
        return 0.0, False

    val = strength["data"].get("strength", 100)
    if val is None or val <= 0:
        return 0.0, False

    # Normalize: 80-120 → -1 to +1
    score = _clamp((val - 100) / 20.0)
    return round(score, 4), True


def build_fx_score(usdkrw: Dict) -> Tuple[float, bool]:
    """원화 강세(USD/KRW 하락)=bullish, 약세(상승)=bearish."""
    chg, ok = _safe_change(usdkrw)
    if not ok:
        return 0.0, False

    # USD/KRW 상승 = 원화 약세 = bearish → 부호 반전
    score = _clamp(-chg / 0.02)
    return round(score, 4), True


def build_composite(scores: Dict[str, Tuple[float, bool]]) -> Tuple[float, float, bool]:
    """
    Compute weighted composite score.
    Returns (composite, available_weight, enough_data).
    available_weight < MIN_AVAILABLE_WEIGHT → enough_data=False.
    """
    available_weight = 0.0
    weighted_sum = 0.0

    for axis, (score, available) in scores.items():
        if available and axis in FEATURE_WEIGHTS:
            w = FEATURE_WEIGHTS[axis]
            available_weight += w
            weighted_sum += score * w

    if available_weight < MIN_AVAILABLE_WEIGHT:
        return 0.0, round(available_weight, 4), False

    composite = weighted_sum / available_weight
    return round(composite, 6), round(available_weight, 4), True
