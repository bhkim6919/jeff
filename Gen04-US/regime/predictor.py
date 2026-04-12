# -*- coding: utf-8 -*-
"""
predictor.py — US Market Regime Prediction v2
===============================================
v2 changes:
  1. VIX spike penalty 제거 (Index 축과 이중 반영 방지)
  2. Breadth scaling ×4 → ×2 (과민 반응 완화)
  3. EMA smoothing (노이즈 제거, 핵심 개선)
  4. Persistence filter (경계 흔들림 방지 + 급변 강제 전환)
  5. 진단 로깅 (raw + ema + 축별 점수)
"""
from __future__ import annotations

import logging
from typing import Optional

from .models import (
    FEATURE_WEIGHTS, VIX_SCORES, RegimeLevel, REGIME_LABELS, REGIME_THRESHOLDS,
    EMA_ALPHA, PERSISTENCE_DEAD_ZONE, PERSISTENCE_FORCE_ZONE,
    score_to_regime,
)

logger = logging.getLogger("qtron.us.regime.predictor")


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


# ── Axis Score Functions ────────────────────────────────


def _index_score(spy: dict, qqq: dict) -> tuple[float, bool]:
    """Index axis: SPY 70% + QQQ 30%. Scale: ±1.5% / ±2.0%."""
    if not spy.get("available"):
        return 0.0, False

    spy_s = _clamp(spy["change_pct"] / 1.5)

    if qqq.get("available"):
        qqq_s = _clamp(qqq["change_pct"] / 2.0)
        return 0.7 * spy_s + 0.3 * qqq_s, True
    return spy_s, True


def _vix_score(vix: dict) -> tuple[float, bool]:
    """VIX axis: level-based only (v2: spike penalty 제거)."""
    if not vix.get("available"):
        return 0.0, False

    level = vix.get("level", 20)
    score = -1.0  # default: very high VIX
    for threshold, s in VIX_SCORES:
        if level < threshold:
            score = s
            break
    # v2: spike penalty 제거 — Index 축에서 이미 반영
    return _clamp(score), True


def _breadth_score(breadth: dict) -> tuple[float, bool]:
    """Sector breadth: (ratio - 0.5) × 2 (v2: ×4에서 완화)."""
    if breadth.get("total", 0) <= 0:
        return 0.0, False

    ratio = breadth.get("ratio", 0.5)
    # v2: ×2 (v1은 ×4로 과민)
    return _clamp((ratio - 0.5) * 2), True


def _fx_score(fx: dict) -> tuple[float, bool]:
    """FX axis: -UUP change (USD 강세 = 주식 약세)."""
    if not fx.get("available"):
        return 0.0, False
    return _clamp(-fx["uup_change_pct"] / 1.0), True


# ── EMA Smoothing ───────────────────────────────────────


def _apply_ema(raw_score: float, prev_ema: Optional[float]) -> float:
    """EMA(span=3). 첫 호출 시 raw 그대로 사용."""
    if prev_ema is None:
        return raw_score
    return EMA_ALPHA * raw_score + (1 - EMA_ALPHA) * prev_ema


# ── Persistence Filter ──────────────────────────────────


def _get_nearest_boundary(score: float) -> float:
    """현재 score에 가장 가까운 레짐 경계값 반환."""
    boundaries = [t[0] for t in REGIME_THRESHOLDS]  # [-0.40, -0.15, +0.15, +0.40]
    if not boundaries:
        return 0.0
    return min(boundaries, key=lambda b: abs(score - b))


def _apply_persistence(
    ema_score: float,
    new_regime: RegimeLevel,
    prev_regime: Optional[int],
) -> RegimeLevel:
    """
    경계 근처 흔들림 방지 + 급변 강제 전환.
    - dead zone (< 0.05): 이전 레짐 유지
    - force zone (> 0.15): 강제 전환
    - 그 외: 일반 전환
    """
    if prev_regime is None or prev_regime == int(new_regime):
        return new_regime

    boundary = _get_nearest_boundary(ema_score)
    dist = abs(ema_score - boundary)

    if dist < PERSISTENCE_DEAD_ZONE:
        # 경계 근처 — 이전 레짐 유지
        logger.debug(
            f"[REGIME_PERSIST] keep {prev_regime} (dist={dist:.3f} < dead_zone)"
        )
        return RegimeLevel(prev_regime)

    if dist > PERSISTENCE_FORCE_ZONE:
        # 충분히 벗어남 — 강제 전환
        logger.debug(
            f"[REGIME_PERSIST] force {prev_regime} → {int(new_regime)} "
            f"(dist={dist:.3f} > force_zone)"
        )

    return new_regime


# ── Main Prediction ─────────────────────────────────────


def predict_regime(
    market_data: dict,
    prev_ema_score: Optional[float] = None,
    prev_regime: Optional[int] = None,
) -> dict:
    """
    Build regime prediction from collected market data (v2).

    Args:
        market_data: collected market snapshots
        prev_ema_score: previous EMA score (from runtime_state)
        prev_regime: previous regime level int (from runtime_state)

    Returns prediction dict with raw/ema scores, regime level, diagnostics.
    """
    spy = market_data.get("spy", {})
    qqq = market_data.get("qqq", {})
    vix = market_data.get("vix", {})
    fx = market_data.get("fx", {})
    breadth = market_data.get("breadth", {})

    # ── 1. Axis scores ──
    idx_score, idx_avail = _index_score(spy, qqq)
    vol_score, vol_avail = _vix_score(vix)
    sec_score, sec_avail = _breadth_score(breadth)
    f_score, f_avail = _fx_score(fx)

    # ── 2. Weighted composite (raw) ──
    axes = {
        "index":  (idx_score, idx_avail, FEATURE_WEIGHTS["index"]),
        "vol":    (vol_score, vol_avail, FEATURE_WEIGHTS["vol"]),
        "sector": (sec_score, sec_avail, FEATURE_WEIGHTS["sector"]),
        "fx":     (f_score,   f_avail,   FEATURE_WEIGHTS["fx"]),
    }

    weighted_sum = 0.0
    available_weight = 0.0
    for _name, (score, avail, weight) in axes.items():
        if avail:
            weighted_sum += score * weight
            available_weight += weight

    raw_score = weighted_sum / available_weight if available_weight > 0 else 0.0
    enough_data = available_weight >= 0.50

    # ── 3. EMA smoothing ──
    ema_score = _apply_ema(raw_score, prev_ema_score)

    # ── 4. Regime mapping ──
    if not enough_data:
        regime = RegimeLevel.NEUTRAL
        confidence_flag = "INSUFFICIENT"
    else:
        new_regime = score_to_regime(ema_score)
        regime = _apply_persistence(ema_score, new_regime, prev_regime)
        confidence_flag = "FULL" if available_weight >= 0.80 else "PARTIAL"

    # ── 5. Diagnostics ──
    result = {
        "predicted_regime": int(regime),
        "predicted_label": REGIME_LABELS[regime],
        # v2: raw + ema 분리
        "raw_score": round(raw_score, 4),
        "ema_score": round(ema_score, 4),
        "composite_score": round(ema_score, 4),  # backward compat
        # Axis scores
        "index_score": round(idx_score, 4),
        "index_avail": idx_avail,
        "vol_score": round(vol_score, 4),
        "vol_avail": vol_avail,
        "sector_score": round(sec_score, 4),
        "sector_avail": sec_avail,
        "fx_score": round(f_score, 4),
        "fx_avail": f_avail,
        # Meta
        "available_weight": round(available_weight, 2),
        "confidence_flag": confidence_flag,
        "prev_regime": prev_regime,
        "prev_ema_score": round(prev_ema_score, 4) if prev_ema_score is not None else None,
        # Raw data (diagnostics)
        "spy_change_pct": spy.get("change_pct", 0),
        "qqq_change_pct": qqq.get("change_pct", 0),
        "vix_level": vix.get("level", 0),
        "breadth_ratio": breadth.get("ratio", 0.5),
    }

    logger.info(
        f"[REGIME_V2] {result['predicted_label']} "
        f"(raw={raw_score:.3f} ema={ema_score:.3f} "
        f"prev={prev_regime} conf={confidence_flag})"
    )

    return result


def predict_tomorrow(market_data: dict, **kwargs) -> dict:
    """Predict tomorrow's regime using today's closing data."""
    result = predict_regime(market_data, **kwargs)
    result["prediction_type"] = "tomorrow"
    return result
