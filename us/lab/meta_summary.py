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
    sd_map = {row["strategy"]: row for row in sd}

    strategy_fit = {}
    for sname, sd_row in sd_map.items():
        strategy_fit[sname] = _score_fit_combined(sname, mc, sd_row)
        fit = strategy_fit[sname]
        logger.debug(
            f"[META_RETURN_DEBUG] {trade_date} {sname}: "
            f"dr={sd_row.get('daily_return')} cr={sd_row.get('cumul_return')} "
            f"pc={sd_row.get('position_count')} ge={sd_row.get('gross_exposure')} "
            f"dq={fit['data_quality']['status']} mf={fit['score']} "
            f"final={fit['final_score']} final_v={fit['final_score_value']} "
            f"rankable={fit['rankable']}"
        )

    data_days = _count_data_days()

    # ── Recommendation log 저장 ──
    try:
        import json as _json
        rankable = {s: f for s, f in strategy_fit.items() if f.get("rankable", True)}
        weights = {}
        if rankable:
            vals = {s: max(f.get("final_score_value", 0), 0) for s, f in rankable.items()}
            total = sum(vals.values()) or 1
            weights = {s: round(v / total, 4) for s, v in vals.items()}

        sorted_by_score = sorted(
            rankable.items(),
            key=lambda x: x[1].get("final_score_value", -999),
            reverse=True,
        )
        top_strategy = sorted_by_score[0][0] if sorted_by_score else None
        top3 = [s for s, _ in sorted_by_score[:3]]

        mf_summary = {s: f.get("market_fit", {}).get("score", "?") for s, f in strategy_fit.items()}
        ph_summary = {s: f.get("perf_health", {}).get("penalty", 0) for s, f in strategy_fit.items()}

        dq_statuses = [f.get("data_quality", {}).get("status", "?") for f in strategy_fit.values()]
        if "BAD" in dq_statuses:
            dq_overall = "BAD"
        elif "UNKNOWN" in dq_statuses:
            dq_overall = "PARTIAL"
        else:
            dq_overall = "OK"

        _sv = mc.get("data_snapshot_id", "")
        meta_db.save_recommendation_log({
            "trade_date": trade_date,
            "snapshot_version": _sv,
            "recommended_weights_json": _json.dumps(weights),
            "top_strategy": top_strategy,
            "top3_strategies_json": _json.dumps(top3),
            "confidence_score": round(data_days / 120, 2) if data_days < 120 else 1.0,
            "regime_label": mc.get("regime_label"),
            "market_fit_summary": _json.dumps(mf_summary),
            "perf_health_summary": _json.dumps(ph_summary),
            "data_quality_status": dq_overall,
            "is_valid": 1 if dq_overall == "OK" else 0,
            "selected_source": mc.get("data_snapshot_id", ""),
        })
        logger.info(f"[META_RECOMMEND] {trade_date}: top={top_strategy}, dq={dq_overall}")
    except Exception as e:
        logger.warning(f"[META_RECOMMEND] save failed (non-fatal): {e}")

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


# ── Data Quality Check ──────────────────────────────────────
# Criteria same as collector (_OUTLIER_THRESHOLD=0.5, mismatch=ge<1e-8).
# gross_exposure==0 is generally equivalent to pos_value==0,
# verified via DQ_EQUIV logs in collector during initial operation.
_OUTLIER_THRESHOLD = 0.5

def _check_data_quality(sd_row: Optional[dict]) -> dict:
    """Validate strategy daily data for sanity."""
    if not sd_row:
        return {"status": "UNKNOWN", "flags": ["NO_DATA"]}

    flags = []
    dr = sd_row.get("daily_return")
    pc = sd_row.get("position_count", 0)
    ge = sd_row.get("gross_exposure")

    if dr is None:
        flags.append("RETURN_NULL")
    elif abs(dr) > _OUTLIER_THRESHOLD:
        flags.append("OUTLIER")

    if pc > 0 and ge is not None and abs(ge) < 1e-8:
        flags.append("SNAPSHOT_MISMATCH")

    if "OUTLIER" in flags or "SNAPSHOT_MISMATCH" in flags:
        status = "BAD"
    elif "RETURN_NULL" in flags or "NO_DATA" in flags:
        status = "UNKNOWN"
    else:
        status = "OK"

    return {"status": status, "flags": flags}


# ── Performance Health ──────────────────────────────────────

def _perf_health(sd_row: Optional[dict], dq: dict) -> dict:
    """Performance-based penalty. Disabled when data_quality != OK."""
    if not sd_row or dq["status"] != "OK":
        return {"penalty": 0, "reasons": []}

    penalty = 0
    reasons = []
    dr = sd_row.get("daily_return")
    cr = sd_row.get("cumul_return")

    if dr is not None:
        if dr <= -0.02:
            penalty = -2
            reasons.append({"sign": "-", "text": f"Daily loss {dr:.1%}"})
        elif dr < 0:
            penalty = -1
            reasons.append({"sign": "-", "text": f"Daily loss {dr:.1%}"})

    if cr is not None and cr <= -0.05:
        reasons.append({"sign": "-", "text": f"Cumul loss {cr:.1%}"})

    return {"penalty": penalty, "reasons": reasons}


# ── Combined Fitness ────────────────────────────────────────

def _score_fit_combined(strategy: str, mc: dict, sd_row: Optional[dict]) -> dict:
    """market_fit + perf_health + data_quality → final_score."""
    mf = _score_fit(strategy, mc)
    dq = _check_data_quality(sd_row)
    ph = _perf_health(sd_row, dq)

    final_value = mf["score_value"] + ph["penalty"]

    # Reasons priority: dq warnings → divergence → perf → market
    reasons = []

    # 1. data_quality flags
    if dq["status"] == "BAD":
        for f in dq["flags"]:
            reasons.append({"sign": "-", "text": f"Data: {f}"})
    elif dq["status"] == "UNKNOWN":
        reasons.append({"sign": "-", "text": "Insufficient data (limited judgment)"})

    # 2. divergence warning (dq OK only)
    if dq["status"] == "OK" and sd_row:
        cr = sd_row.get("cumul_return")
        dr = sd_row.get("daily_return")
        if mf["score"] == "HIGH" and ((dr is not None and dr < 0) or (cr is not None and cr < 0)):
            reasons.append({"sign": "-", "text": "⚠ Market fit vs perf divergence"})
        elif mf["score"] == "LOW" and cr is not None and cr > 0.02:
            reasons.append({"sign": "+", "text": "✓ Market unfavorable but perf positive"})

    # 3. performance reasons
    reasons.extend(ph["reasons"])

    # 4. market reasons
    reasons.extend(mf["reasons"])

    # final_score
    if dq["status"] == "BAD":
        final_score = "WARN"
        final_value = -999
        rankable = False
    elif dq["status"] == "UNKNOWN" and mf["score"] == "HIGH":
        final_score = "MID"
        rankable = True
    else:
        final_score = "HIGH" if final_value >= 2 else "LOW" if final_value < 0 else "MID"
        rankable = True

    return {
        "score": mf["score"],
        "score_value": mf["score_value"],
        "final_score": final_score,
        "final_score_value": final_value if rankable else -999,
        "rankable": rankable,
        "reasons": reasons,
        "market_fit": {"score": mf["score"], "score_value": mf["score_value"]},
        "perf_health": ph,
        "data_quality": dq,
    }


def _confidence_level(data_days: int) -> str:
    if data_days >= 120:
        return "HIGH"
    if data_days >= 60:
        return "MID"
    return "LOW"


def _count_data_days() -> int:
    try:
        from shared.db.pg_base import connection
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM meta_market_context_us")
            row = cur.fetchone()
            cur.close()
            return row[0] if row else 0
    except Exception:
        return 0
