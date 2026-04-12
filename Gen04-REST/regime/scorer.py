# -*- coding: utf-8 -*-
"""
scorer.py — 예측 vs 실제 비교 + adjusted_confidence
=====================================================
confidence = 사후 채점 점수 (사전 확률 아님).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from regime.models import GLOBAL_MISSING_CAP, DIRECTION_PENALTY
from regime.storage import save_score, load_recent_scores

logger = logging.getLogger("gen4.regime.scorer")


def score_prediction(
    predicted: int,
    actual: int,
    available_weight: float = 1.0,
    global_available: bool = True,
    prediction_id: Optional[int] = None,
    actual_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compare predicted vs actual regime.
    Returns score record with adjusted_confidence.
    """
    distance = abs(predicted - actual)
    raw_confidence = (1 - distance / 4) * 100

    # Adjusted confidence = raw × available_weight
    adjusted = raw_confidence * available_weight

    # Global missing cap
    if not global_available:
        adjusted = min(adjusted, GLOBAL_MISSING_CAP * 100)

    # Direction penalty: global 없이 극단 예측
    if not global_available and abs(predicted - 3) > 1:
        adjusted *= DIRECTION_PENALTY

    adjusted = round(max(0, min(100, adjusted)), 1)

    # Confidence flag
    if available_weight >= 0.80:
        flag = "FULL"
    elif available_weight >= 0.50:
        flag = "PARTIAL"
    else:
        flag = "INSUFFICIENT"

    record = {
        "prediction_id": prediction_id,
        "actual_id": actual_id,
        "predicted": predicted,
        "actual": actual,
        "distance": distance,
        "raw_confidence": round(raw_confidence, 1),
        "adjusted_confidence": adjusted,
        "available_weight": available_weight,
        "confidence_flag": flag,
    }

    try:
        save_score(record)
        logger.info(f"[Score] pred={predicted} actual={actual} dist={distance} "
                     f"conf={adjusted}% ({flag})")
    except Exception as e:
        logger.error(f"[Score] Save failed: {e}")
        record["save_error"] = str(e)

    return record


def compute_rolling_stats(window: int = 20) -> Dict[str, Any]:
    """Compute rolling accuracy stats from recent scores."""
    records = load_recent_scores(window)
    if not records:
        return {
            "count": 0,
            "avg_confidence_5d": None,
            "avg_confidence_20d": None,
            "exact_match_rate": None,
            "within_one_step_rate": None,
            "extreme_miss_rate": None,
        }

    n = len(records)

    # All records
    confidences = [r["adjusted_confidence"] for r in records]
    exact = sum(1 for r in records if r["distance"] == 0)
    within_one = sum(1 for r in records if r["distance"] <= 1)
    extreme = sum(1 for r in records if r["distance"] >= 3)

    # 5-day subset
    recent_5 = records[:5]
    conf_5 = [r["adjusted_confidence"] for r in recent_5] if len(recent_5) >= 1 else []

    return {
        "count": n,
        "avg_confidence_5d": round(sum(conf_5) / len(conf_5), 1) if conf_5 else None,
        "avg_confidence_20d": round(sum(confidences) / n, 1) if n else None,
        "exact_match_rate": round(exact / n * 100, 1) if n else None,
        "within_one_step_rate": round(within_one / n * 100, 1) if n else None,
        "extreme_miss_rate": round(extreme / n * 100, 1) if n else None,
    }
