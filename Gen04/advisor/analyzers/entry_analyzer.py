"""Entry Quality Analyzer — post-entry performance, timing, regime mismatch."""
from __future__ import annotations

from ..ingestion.schema import SnapshotWindow


def analyze_entries(window: SnapshotWindow) -> dict:
    """Analyze entry quality across window."""
    # Collect all BUY trades
    all_buys = []
    for s, valid in zip(window.snapshots, window.valid_mask):
        if not valid:
            continue
        for t in s.trades:
            if t.get("side", "").upper() != "BUY":
                continue
            all_buys.append({
                "code": t.get("code", "").strip(),
                "date": t.get("date", "").strip(),
                "price": _sf(t.get("price", "0")),
                "slippage_pct": _sf(t.get("slippage_pct", "0")),
            })

    if not all_buys:
        return {"total_entries": 0}

    # Collect all closes to match entry → exit
    close_map = {}  # code -> list of closes
    for s, valid in zip(window.snapshots, window.valid_mask):
        if not valid:
            continue
        for c in s.closes:
            code = c.get("code", "").strip()
            if code not in close_map:
                close_map[code] = []
            close_map[code].append({
                "exit_date": c.get("date", "").strip(),
                "pnl_pct": _sf(c.get("pnl_pct", "0")),
                "hold_days": _si(c.get("hold_days", "0")),
                "exit_reason": c.get("exit_reason", "").strip(),
                "max_hwm_pct": _sf(c.get("max_hwm_pct", "0")),
            })

    # Match entries to their exits
    matched = []
    for buy in all_buys:
        code = buy["code"]
        closes = close_map.get(code, [])
        # Find the close that happened after this buy
        matching_close = None
        for c in closes:
            if c["exit_date"] >= buy["date"]:
                matching_close = c
                break
        if matching_close:
            matched.append({
                "code": code,
                "entry_date": buy["date"],
                "entry_price": buy["price"],
                "pnl_pct": matching_close["pnl_pct"],
                "hold_days": matching_close["hold_days"],
                "exit_reason": matching_close["exit_reason"],
                "max_hwm_pct": matching_close.get("max_hwm_pct", 0),
            })

    # Entry quality metrics
    if matched:
        failed_entries = [m for m in matched
                         if m["hold_days"] <= 3 and m["pnl_pct"] < -0.03]
        quick_stops = [m for m in matched
                       if m["hold_days"] <= 5 and m["exit_reason"] == "TRAIL_STOP"]

        avg_pnl = sum(m["pnl_pct"] for m in matched) / len(matched)
        avg_hold = sum(m["hold_days"] for m in matched) / len(matched)
    else:
        failed_entries = []
        quick_stops = []
        avg_pnl = 0
        avg_hold = 0

    # Slippage analysis
    slippages = [b["slippage_pct"] for b in all_buys if b["slippage_pct"]]
    avg_slippage = sum(slippages) / len(slippages) if slippages else 0

    return {
        "window": f"{window.start_date}~{window.end_date}",
        "total_entries": len(all_buys),
        "matched_to_exit": len(matched),
        "avg_pnl_pct": avg_pnl,
        "avg_hold_days": avg_hold,
        "failed_entry_count": len(failed_entries),
        "failed_entry_rate": (len(failed_entries) / len(matched)
                              if matched else 0),
        "quick_trail_stop_count": len(quick_stops),
        "quick_trail_stop_rate": (len(quick_stops) / len(matched)
                                  if matched else 0),
        "avg_slippage_pct": avg_slippage,
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
