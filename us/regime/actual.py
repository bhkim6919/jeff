# -*- coding: utf-8 -*-
"""
actual.py — Today's Actual Regime (SPY-based)
===============================================
Simple: SPY daily change % → regime level.
No complex calculation — broker data directly.
"""
from __future__ import annotations

import logging

from .models import actual_to_regime, REGIME_LABELS

logger = logging.getLogger("qtron.us.regime.actual")


def calculate_actual(market_data: dict) -> dict:
    """
    Calculate today's actual regime from SPY change %.

    Returns:
        {actual_regime, actual_label, spy_change_pct, spy_price, breadth_ratio}
    """
    spy = market_data.get("spy", {})
    breadth = market_data.get("breadth", {})

    spy_change = spy.get("change_pct", 0)
    spy_price = spy.get("price", 0)

    if not spy.get("available"):
        return {
            "actual_regime": 3,
            "actual_label": "NEUTRAL",
            "spy_change_pct": 0,
            "spy_price": 0,
            "breadth_ratio": 0.5,
            "available": False,
        }

    regime = actual_to_regime(spy_change)
    label = REGIME_LABELS[regime]

    return {
        "actual_regime": int(regime),
        "actual_label": label,
        "spy_change_pct": round(spy_change, 2),
        "spy_price": round(spy_price, 2),
        "breadth_ratio": round(breadth.get("ratio", 0.5), 3),
        "available": True,
    }
