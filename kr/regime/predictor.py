# -*- coding: utf-8 -*-
"""
predictor.py — T-1 기반 레짐 예측 v2
=======================================
v2 changes:
  1. VIX spike penalty 제거
  2. Domestic breadth 분리 → breadth 독립 축
  3. micro 축 제거 (dead factor)
  4. EMA smoothing + persistence filter
  5. 진단 로깅 (raw + ema)
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, Optional

from regime.calendar import next_trading_day, previous_trading_day
from regime.collector_domestic import collect_kospi, collect_kosdaq, collect_trade_strength
from regime.collector_global import fetch_sp500, fetch_nasdaq, fetch_vix, fetch_usdkrw
from regime.feature_builder import (
    build_global_score, build_vol_score, build_domestic_score,
    build_breadth_score, build_fx_score, build_composite,
)
from regime.models import (
    score_to_regime, REGIME_LABELS, REGIME_THRESHOLDS,
    EMA_ALPHA, PERSISTENCE_DEAD_ZONE, PERSISTENCE_FORCE_ZONE,
)
from regime.storage import save_prediction

logger = logging.getLogger("gen4.regime.predictor")


# ── EMA Smoothing ───────────────────────────────────────

def _apply_ema(raw_score: float, prev_ema: Optional[float]) -> float:
    if prev_ema is None:
        return raw_score
    return EMA_ALPHA * raw_score + (1 - EMA_ALPHA) * prev_ema


# ── Persistence Filter ──────────────────────────────────

def _get_nearest_boundary(score: float) -> float:
    boundaries = [t[0] for t in REGIME_THRESHOLDS]
    if not boundaries:
        return 0.0
    return min(boundaries, key=lambda b: abs(score - b))


def _apply_persistence(ema_score, new_regime, prev_regime):
    if prev_regime is None or prev_regime == int(new_regime):
        return new_regime

    boundary = _get_nearest_boundary(ema_score)
    dist = abs(ema_score - boundary)

    if dist < PERSISTENCE_DEAD_ZONE:
        logger.debug(f"[REGIME_PERSIST] keep {prev_regime} (dist={dist:.3f})")
        from regime.models import RegimeLevel
        return RegimeLevel(prev_regime)

    return new_regime


# ── Main Prediction ─────────────────────────────────────

def predict_regime(
    provider: Any = None,
    prev_ema_score: Optional[float] = None,
    prev_regime: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Predict tomorrow's regime using T-1 data (v2).
    """
    today = date.today()
    target_date = next_trading_day(today)
    feature_date = previous_trading_day(target_date)

    logger.info(f"[Predict] today={today}, feature_date={feature_date}, target_date={target_date}")

    # ── Collect ──
    global_data = {
        "sp500": fetch_sp500(feature_date),
        "nasdaq": fetch_nasdaq(feature_date),
        "vix": fetch_vix(feature_date),
        "usdkrw": fetch_usdkrw(feature_date),
    }

    domestic_data = {}
    if provider:
        domestic_data = {
            "kospi": collect_kospi(provider, feature_date),
            "kosdaq": collect_kosdaq(provider, feature_date),
        }
    else:
        domestic_data = {
            "kospi": _fail("no provider"),
            "kosdaq": _fail("no provider"),
        }

    # ── Build scores (v2: micro 제거, breadth 독립) ──
    scores = {
        "global": build_global_score(global_data["sp500"], global_data["nasdaq"]),
        "vol": build_vol_score(global_data["vix"]),
        "domestic": build_domestic_score(domestic_data["kospi"], domestic_data["kosdaq"]),
        "breadth": build_breadth_score(domestic_data["kospi"]),
        "fx": build_fx_score(global_data["usdkrw"]),
    }

    raw_composite, avail_weight, enough = build_composite(scores)

    if not enough:
        logger.warning(f"[Predict] Insufficient data: available_weight={avail_weight}")
        return {
            "unavailable": True,
            "available_weight": avail_weight,
            "feature_date": str(feature_date),
            "target_date": str(target_date),
            "error": f"available_weight {avail_weight} < 0.50",
        }

    # ── EMA smoothing ──
    ema_score = _apply_ema(raw_composite, prev_ema_score)

    # ── Regime mapping + persistence ──
    new_regime = score_to_regime(ema_score)
    regime = _apply_persistence(ema_score, new_regime, prev_regime)
    confidence_flag = "FULL" if avail_weight >= 0.80 else "PARTIAL"

    # ── Source health ──
    source_health = {}
    for name, result in {**global_data, **domestic_data}.items():
        source_health[name] = {
            "ok": result.get("ok", False),
            "stale": result.get("stale", True),
            "from_cache": result.get("from_cache", False),
            "source": result.get("source", ""),
            "error": result.get("error"),
        }

    record = {
        "feature_date": str(feature_date),
        "target_date": str(target_date),
        "predicted_regime": regime.value,
        "predicted_label": regime.name,
        # v2: raw + ema
        "raw_score": raw_composite,
        "ema_score": round(ema_score, 6),
        "composite_score": round(ema_score, 6),  # backward compat
        # Axis scores
        "global_score": scores["global"][0],
        "global_avail": int(scores["global"][1]),
        "vol_score": scores["vol"][0],
        "vol_avail": int(scores["vol"][1]),
        "domestic_score": scores["domestic"][0],
        "domestic_avail": int(scores["domestic"][1]),
        "breadth_score": scores["breadth"][0],
        "breadth_avail": int(scores["breadth"][1]),
        "fx_score": scores["fx"][0],
        "fx_avail": int(scores["fx"][1]),
        # Meta
        "available_weight": avail_weight,
        "confidence_flag": confidence_flag,
        "prev_regime": prev_regime,
        "prev_ema_score": round(prev_ema_score, 4) if prev_ema_score is not None else None,
        "source_health": source_health,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # ── Save ──
    try:
        save_prediction(record)
        logger.info(
            f"[Predict_V2] {target_date}: {regime.name} "
            f"(raw={raw_composite:.4f} ema={ema_score:.4f} prev={prev_regime})"
        )
    except Exception as e:
        logger.error(f"[Predict] Save failed: {e}")
        record["save_error"] = str(e)

    return record


def _fail(error: str) -> Dict:
    return {"ok": False, "data": None, "source_ts": 0, "read_ts": time.time(),
            "stale": True, "expired": True, "from_cache": False, "error": error, "source": ""}
