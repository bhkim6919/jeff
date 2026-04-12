"""Exit Analyzer — exit reason distribution, premature exits, post-exit performance."""
from __future__ import annotations

from collections import Counter
from ..ingestion.schema import SnapshotWindow


def analyze_exits(window: SnapshotWindow) -> dict:
    """Analyze exit reason distribution across window."""
    all_closes = []
    for s, valid in zip(window.snapshots, window.valid_mask):
        if not valid:
            continue
        for c in s.closes:
            all_closes.append({
                "code": c.get("code", "").strip(),
                "date": c.get("date", "").strip(),
                "exit_reason": c.get("exit_reason", "").strip(),
                "pnl_pct": _sf(c.get("pnl_pct", "0")),
                "hold_days": _si(c.get("hold_days", "0")),
                "entry_price": _sf(c.get("entry_price", "0")),
                "exit_price": _sf(c.get("exit_price", "0")),
            })

    if not all_closes:
        return {"total_exits": 0, "distribution": {}}

    # Distribution by reason
    reason_counter = Counter(c["exit_reason"] for c in all_closes)
    distribution = {}
    for reason, count in reason_counter.most_common():
        subset = [c for c in all_closes if c["exit_reason"] == reason]
        pnls = [c["pnl_pct"] for c in subset]
        holds = [c["hold_days"] for c in subset]
        distribution[reason] = {
            "count": count,
            "pct_of_total": count / len(all_closes),
            "avg_pnl_pct": sum(pnls) / len(pnls),
            "avg_hold_days": sum(holds) / len(holds),
            "win_rate": sum(1 for p in pnls if p > 0) / len(pnls),
        }

    # Premature exits: trail stop within 5 days AND loss
    premature = [c for c in all_closes
                 if c["exit_reason"] == "TRAIL_STOP"
                 and c["hold_days"] <= 5
                 and c["pnl_pct"] < 0]
    trail_total = reason_counter.get("TRAIL_STOP", 0)

    return {
        "window": f"{window.start_date}~{window.end_date}",
        "total_exits": len(all_closes),
        "distribution": distribution,
        "premature_trail_exits": len(premature),
        "premature_trail_rate": (len(premature) / trail_total
                                 if trail_total > 0 else 0),
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
