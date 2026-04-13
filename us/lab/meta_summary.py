"""
meta_summary.py -- Gen5 Meta Layer: US UI summary
===================================================
US-specific thresholds and scoring rules.
No DB storage of text — computed on-the-fly from data.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from lab import meta_db

logger = logging.getLogger("lab.meta")

THRESHOLDS = {
    "index_up": 0.005,
    "index_down": -0.005,
    "index_crash": -0.01,
    "adv_ratio_strong": 0.6,
    "adv_ratio_weak": 0.4,
    "adv_ratio_mid": 0.5,
    "breakout_high": 0.05,
    "breakout_mid": 0.03,
    "sector_disp_high": 0.015,
    "sector_disp_low": 0.008,
}

RULES: Dict[str, list] = {
    "breakout_trend": [
        ("breakout_ratio", ">", "breakout_high", +2, "breakout stocks high"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("adv_ratio", "<", "adv_ratio_weak", -1, "ADR weak"),
    ],
    "liquidity_signal": [
        ("breakout_ratio", ">", "breakout_mid", +1, "breakout active"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("index_return", ">", "index_up", +1, "market rising"),
    ],
    "mean_reversion": [
        ("index_return", "<", "index_down", +2, "market dip -> bounce"),
        ("adv_ratio", "<", "adv_ratio_weak", +1, "oversold breadth"),
        ("breakout_ratio", ">", "breakout_high", -1, "trend market unfavorable"),
    ],
    "momentum_base": [
        ("index_return", ">", "index_up", +1, "market rising"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("sector_dispersion", "<", "sector_disp_high", +1, "even sector"),
    ],
    "lowvol_momentum": [
        ("index_return", ">", "index_up", +1, "market rising"),
        ("sector_dispersion", "<", "sector_disp_high", +1, "even sector"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
    ],
    "quality_factor": [
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("sector_dispersion", "<", "sector_disp_high", +1, "even sector"),
        ("index_return", "<", "index_crash", -1, "crash unfavorable"),
    ],
    "hybrid_qscore": [
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("breakout_ratio", ">", "breakout_mid", +1, "breakout active"),
        ("sector_dispersion", ">", "sector_disp_high", +1, "sector differentiation"),
    ],
    "sector_rotation": [
        ("sector_dispersion", ">", "sector_disp_high", +2, "sector rotation clear"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("sector_dispersion", "<", "sector_disp_low", -2, "even sector unfavorable"),
    ],
    "vol_regime": [
        ("index_return", "<", "index_crash", +1, "market drop defensive"),
        ("adv_ratio", ">", "adv_ratio_strong", +1, "broad strength"),
        ("index_return", ">", "index_up", +1, "market rising"),
    ],
    "russell3000_lowvol": [
        ("index_return", ">", "index_up", +1, "market rising"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "broad advance"),
        ("sector_dispersion", "<", "sector_disp_low", +1, "even sector"),
    ],
}


def build_daily_summary_us(trade_date: str) -> Optional[dict]:
    mc = meta_db.get_market_context(trade_date)
    if not mc:
        return None

    sd = meta_db.get_strategy_daily(trade_date)

    strategy_fit = {}
    for row in sd:
        strategy_fit[row["strategy"]] = _score_fit(row["strategy"], mc)

    data_days = _count_data_days()

    return {
        "trade_date": trade_date,
        "market_tags": _market_tags(mc),
        "market_data": {
            "index_return": mc.get("index_return"),
            "adv_ratio": mc.get("adv_ratio"),
            "breakout_ratio": mc.get("breakout_ratio"),
            "sector_dispersion": mc.get("sector_dispersion"),
        },
        "strategy_fit": strategy_fit,
        "confidence": _confidence_level(data_days),
        "data_days": data_days,
    }


def _market_tags(mc: dict) -> List[str]:
    tags = []
    T = THRESHOLDS

    ir = mc.get("index_return")
    if ir is not None:
        if ir > T["index_up"]:
            tags.append("Market Up")
        elif ir < T["index_down"]:
            tags.append("Market Down")
        else:
            tags.append("Market Flat")

    adv = mc.get("adv_ratio")
    if adv is not None:
        if adv > T["adv_ratio_strong"]:
            tags.append("ADR Strong")
        elif adv < T["adv_ratio_weak"]:
            tags.append("ADR Weak")

    br = mc.get("breakout_ratio")
    if br is not None and br > T["breakout_high"]:
        tags.append("Breakout Active")

    sd = mc.get("sector_dispersion")
    if sd is not None and sd > T["sector_disp_high"]:
        tags.append("Sector Dispersion")

    return tags


def _score_fit(strategy: str, mc: dict) -> dict:
    rules = RULES.get(strategy, [])
    score = 0
    reasons = []

    for feat, op, threshold_key, weight, label in rules:
        val = mc.get(feat)
        if val is None:
            continue

        threshold = THRESHOLDS.get(threshold_key, threshold_key)

        hit = False
        if op == ">" and isinstance(val, (int, float)) and val > threshold:
            hit = True
        elif op == "<" and isinstance(val, (int, float)) and val < threshold:
            hit = True
        elif op == "==" and val == threshold:
            hit = True

        if hit:
            score += weight
            sign = "+" if weight > 0 else "-"
            reasons.append({"sign": sign, "text": label})

    level = "HIGH" if score >= 2 else "LOW" if score < 0 else "MID"
    return {"score": level, "score_value": score, "reasons": reasons}


def _confidence_level(data_days: int) -> str:
    if data_days >= 120:
        return "HIGH"
    if data_days >= 60:
        return "MID"
    return "LOW"


def _count_data_days() -> int:
    conn = meta_db._conn()
    try:
        row = conn.execute("SELECT COUNT(*) FROM market_context").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()
