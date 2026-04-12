"""MDD Analyzer — drawdown periods, contributors, DD guard effectiveness."""
from __future__ import annotations

from ..ingestion.schema import DailySnapshot, SnapshotWindow


def analyze_mdd(window: SnapshotWindow) -> dict:
    """Analyze maximum drawdown across window."""
    valid = [(s, v) for s, v in zip(window.snapshots, window.valid_mask) if v]
    if not valid:
        return {"error": "no valid snapshots"}

    # Build equity series
    equity_series = []
    for s, _ in valid:
        eq = _sf(s.equity.get("equity", "0"))
        dd = _sf(s.equity.get("monthly_dd_pct", "0"))
        risk = s.equity.get("risk_mode", "").strip()
        equity_series.append({
            "date": s.trading_day,
            "equity": eq,
            "monthly_dd_pct": dd,
            "risk_mode": risk,
            "regime": s.regime,
        })

    # Find MDD
    peak = 0
    mdd = 0
    mdd_start = ""
    mdd_end = ""
    current_dd_start = ""
    in_dd = False

    for row in equity_series:
        eq = row["equity"]
        if eq <= 0:
            continue
        if eq > peak:
            peak = eq
            in_dd = False
        dd = (eq / peak - 1) if peak > 0 else 0
        if dd < mdd:
            mdd = dd
            mdd_end = row["date"]
            if not in_dd:
                current_dd_start = row["date"]
                in_dd = True
            mdd_start = current_dd_start

    # DD guard activations
    dd_guard_days = [r for r in equity_series
                     if r["risk_mode"] and r["risk_mode"] != "NORMAL"]
    dd_guard_modes = {}
    for d in dd_guard_days:
        mode = d["risk_mode"]
        dd_guard_modes[mode] = dd_guard_modes.get(mode, 0) + 1

    # Current status
    latest = equity_series[-1] if equity_series else {}

    return {
        "window": f"{window.start_date}~{window.end_date}",
        "mdd_pct": mdd,
        "mdd_start": mdd_start,
        "mdd_end": mdd_end,
        "current_dd_pct": latest.get("monthly_dd_pct", 0),
        "current_risk_mode": latest.get("risk_mode", ""),
        "dd_guard_activations": dd_guard_modes,
        "dd_guard_days": len(dd_guard_days),
        "peak_equity": peak,
    }


def analyze_mdd_contributors(snapshot: DailySnapshot) -> dict:
    """Identify worst-performing positions contributing to DD."""
    if not snapshot.positions:
        return {"contributors": []}

    contributors = []
    for code, pos in snapshot.positions.items():
        avg = pos.get("avg_price", 0)
        hwm = pos.get("high_watermark", 0)
        trail = pos.get("trail_stop_price", 0)
        qty = pos.get("quantity", 0)

        # Estimate current value gap from HWM
        if hwm > 0 and avg > 0:
            dd_from_hwm = (trail / hwm - 1) if hwm > 0 else 0
            invested = qty * avg
            contributors.append({
                "code": code,
                "invested": invested,
                "dd_from_hwm_pct": dd_from_hwm,
                "hwm": hwm,
                "trail_stop": trail,
            })

    # Sort by worst DD from HWM
    contributors.sort(key=lambda x: x["dd_from_hwm_pct"])

    return {
        "date": snapshot.trading_day,
        "contributors": contributors[:5],  # Top 5 worst
    }


def _sf(v) -> float:
    try:
        return float(str(v).strip() or "0")
    except (ValueError, TypeError):
        return 0.0
