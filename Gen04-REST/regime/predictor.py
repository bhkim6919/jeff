# -*- coding: utf-8 -*-
"""
predictor.py — T-1 기반 레짐 예측 (시간축 고정)
=================================================
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
    build_micro_score, build_fx_score, build_composite,
)
from regime.models import score_to_regime, REGIME_LABELS
from regime.storage import save_prediction

logger = logging.getLogger("gen4.regime.predictor")


def predict_regime(provider: Any = None) -> Dict[str, Any]:
    """
    Predict tomorrow's regime using T-1 data.
    Returns full prediction record or {"unavailable": True} if insufficient data.
    """
    today = date.today()
    target_date = next_trading_day(today)
    feature_date = previous_trading_day(target_date)

    logger.info(f"[Predict] today={today}, feature_date={feature_date}, target_date={target_date}")

    # ── Collect (date 강제 전달) ──
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
            "strength": collect_trade_strength(provider, feature_date),
        }
    else:
        domestic_data = {
            "kospi": _fail("no provider"),
            "kosdaq": _fail("no provider"),
            "strength": _fail("no provider"),
        }

    # ── Build scores ──
    scores = {
        "global": build_global_score(global_data["sp500"], global_data["nasdaq"]),
        "vol": build_vol_score(global_data["vix"]),
        "domestic": build_domestic_score(domestic_data["kospi"], domestic_data["kosdaq"]),
        "micro": build_micro_score(domestic_data["strength"]),
        "fx": build_fx_score(global_data["usdkrw"]),
    }

    composite, avail_weight, enough = build_composite(scores)

    if not enough:
        logger.warning(f"[Predict] Insufficient data: available_weight={avail_weight}")
        return {
            "unavailable": True,
            "available_weight": avail_weight,
            "feature_date": str(feature_date),
            "target_date": str(target_date),
            "error": f"available_weight {avail_weight} < 0.50",
        }

    regime = score_to_regime(composite)
    confidence_flag = "FULL" if avail_weight >= 0.80 else "PARTIAL"

    # ── Source health summary ──
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
        "composite_score": composite,
        "global_score": scores["global"][0],
        "global_avail": int(scores["global"][1]),
        "vol_score": scores["vol"][0],
        "vol_avail": int(scores["vol"][1]),
        "domestic_score": scores["domestic"][0],
        "domestic_avail": int(scores["domestic"][1]),
        "micro_score": scores["micro"][0],
        "micro_avail": int(scores["micro"][1]),
        "fx_score": scores["fx"][0],
        "fx_avail": int(scores["fx"][1]),
        "available_weight": avail_weight,
        "confidence_flag": confidence_flag,
        "source_health": source_health,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }

    # ── Save ──
    try:
        save_prediction(record)
        logger.info(f"[Predict] {target_date}: {regime.name} (score={composite:.4f}, weight={avail_weight})")
    except Exception as e:
        logger.error(f"[Predict] Save failed: {e}")
        record["save_error"] = str(e)

    return record


def _fail(error: str) -> Dict:
    return {"ok": False, "data": None, "source_ts": 0, "read_ts": time.time(),
            "stale": True, "expired": True, "from_cache": False, "error": error, "source": ""}
