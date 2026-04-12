"""Parameter Recommender - analyze close_log patterns to suggest config changes.

All recommendations are MANUAL_REVIEW only. Never auto-applied.
"""
from __future__ import annotations

from datetime import datetime
from ..ingestion.schema import SnapshotWindow


def recommend_params(window: SnapshotWindow) -> list[dict]:
    """Analyze window data and generate parameter recommendations.

    Returns list of recommendation dicts.
    """
    recommendations = []

    # Collect all closes from valid days
    all_closes = []
    for s, v in zip(window.snapshots, window.valid_mask):
        if not v:
            continue
        for c in s.closes:
            all_closes.append({
                "code": c.get("code", "").strip(),
                "exit_reason": c.get("exit_reason", "").strip(),
                "pnl_pct": _sf(c.get("pnl_pct", "0")),
                "hold_days": _si(c.get("hold_days", "0")),
                "max_hwm_pct": _sf(c.get("max_hwm_pct", "0")),
            })

    if len(all_closes) < 5:
        return []  # Not enough data

    # ── 1. Trail Stop analysis ──
    trail_exits = [c for c in all_closes if c["exit_reason"] == "TRAIL_STOP"]
    if trail_exits:
        premature = [c for c in trail_exits
                     if c["hold_days"] <= 5 and c["pnl_pct"] < 0]
        premature_rate = len(premature) / len(trail_exits)

        if premature_rate > 0.40:
            recommendations.append(_build_rec(
                param="TRAIL_PCT",
                current=0.12,
                suggested=0.15,
                rationale=(f"Trail stop premature exit rate {premature_rate:.0%} "
                           f"({len(premature)}/{len(trail_exits)} within 5 days at loss). "
                           f"Wider stop may reduce whipsaw exits."),
                confidence="LOW",
            ))
        elif premature_rate < 0.10 and len(trail_exits) >= 10:
            avg_hwm = sum(c["max_hwm_pct"] for c in trail_exits) / len(trail_exits)
            if avg_hwm > 0.08:
                recommendations.append(_build_rec(
                    param="TRAIL_PCT",
                    current=0.12,
                    suggested=0.10,
                    rationale=(f"Trail exits rarely premature ({premature_rate:.0%}), "
                               f"avg HWM gain {avg_hwm:.1%} before exit. "
                               f"Tighter stop may capture more profit."),
                    confidence="LOW",
                ))

    # ── 2. Hold days analysis ──
    if all_closes:
        avg_hold = sum(c["hold_days"] for c in all_closes) / len(all_closes)
        short_exits = [c for c in all_closes if c["hold_days"] <= 3]
        short_rate = len(short_exits) / len(all_closes)

        if short_rate > 0.30:
            recommendations.append(_build_rec(
                param="REBAL_DAYS",
                current=21,
                suggested=None,  # observation only
                rationale=(f"{short_rate:.0%} of exits within 3 days "
                           f"(avg hold={avg_hold:.1f}d). "
                           f"Check if entry timing or market conditions are the cause."),
                confidence="LOW",
            ))

    # ── 3. Win rate trend ──
    if len(all_closes) >= 10:
        first_half = all_closes[:len(all_closes)//2]
        second_half = all_closes[len(all_closes)//2:]
        wr1 = sum(1 for c in first_half if c["pnl_pct"] > 0) / len(first_half)
        wr2 = sum(1 for c in second_half if c["pnl_pct"] > 0) / len(second_half)

        if wr2 < wr1 - 0.15:
            recommendations.append(_build_rec(
                param="STRATEGY_REVIEW",
                current=None,
                suggested=None,
                rationale=(f"Win rate declining: first half {wr1:.0%} -> "
                           f"second half {wr2:.0%} ({wr2-wr1:+.0%}). "
                           f"Market regime shift or strategy drift possible."),
                confidence="MEDIUM",
            ))

    # ── 4. DD guard effectiveness ──
    dd_days = 0
    normal_days = 0
    for s, v in zip(window.snapshots, window.valid_mask):
        if not v:
            continue
        risk = s.equity.get("risk_mode", "").strip() if s.equity else ""
        if risk and risk != "NORMAL":
            dd_days += 1
        else:
            normal_days += 1

    total = dd_days + normal_days
    if total > 0 and dd_days / total > 0.50:
        recommendations.append(_build_rec(
            param="DD_LEVELS",
            current="monthly -7%",
            suggested=None,
            rationale=(f"DD guard active {dd_days}/{total} days ({dd_days/total:.0%}). "
                       f"Guard may be too sensitive for current market conditions. "
                       f"Review MONTHLY_DD_LIMIT threshold."),
            confidence="LOW",
        ))

    return recommendations


def _build_rec(param: str, current, suggested, rationale: str,
               confidence: str) -> dict:
    return {
        "id": f"ADV_{datetime.now().strftime('%Y%m%d')}_{param}",
        "timestamp": datetime.now().isoformat(),
        "category": "PARAM",
        "priority": "MEDIUM",
        "parameter": param,
        "current_value": current,
        "suggested_value": suggested,
        "rationale": rationale,
        "confidence": confidence,
        "action_required": "MANUAL_REVIEW",
    }


def _sf(v) -> float:
    try:
        return float(str(v).strip() or "0")
    except (ValueError, TypeError):
        return 0.0


def _si(v) -> int:
    try:
        return int(float(str(v).strip() or "0"))
    except (ValueError, TypeError):
        return 0
