"""PnL Analyzer — daily/weekly/monthly attribution, cost impact, win rate trend."""
from __future__ import annotations

from ..ingestion.schema import DailySnapshot, SnapshotWindow


def analyze_pnl(snapshot: DailySnapshot) -> dict:
    """Single-day PnL analysis."""
    equity = snapshot.equity
    if not equity:
        return {"error": "no equity data"}

    daily_pnl = _safe_float(equity.get("daily_pnl_pct", "0"))
    monthly_dd = _safe_float(equity.get("monthly_dd_pct", "0"))
    risk_mode = equity.get("risk_mode", "").strip()
    n_positions = _safe_int(equity.get("n_positions", "0"))
    equity_val = _safe_float(equity.get("equity", "0"))
    cash = _safe_float(equity.get("cash", "0"))

    # Trade summary
    buys = [t for t in snapshot.trades if t.get("side", "").upper() == "BUY"]
    sells = [t for t in snapshot.trades if t.get("side", "").upper() == "SELL"]

    # Cost impact from trades
    total_cost = sum(_safe_float(t.get("cost", "0")) for t in snapshot.trades)

    # Close analysis (realized PnL)
    realized = []
    for c in snapshot.closes:
        pnl = _safe_float(c.get("pnl_pct", "0"))
        code = c.get("code", "").strip()
        realized.append({"code": code, "pnl_pct": pnl})

    # Sort by PnL for top/bottom contributors
    realized.sort(key=lambda x: x["pnl_pct"], reverse=True)
    top = realized[:3] if realized else []
    bottom = realized[-3:] if realized else []

    return {
        "date": snapshot.trading_day,
        "daily_pnl_pct": daily_pnl,
        "monthly_dd_pct": monthly_dd,
        "risk_mode": risk_mode,
        "equity": equity_val,
        "cash": cash,
        "n_positions": n_positions,
        "n_buys": len(buys),
        "n_sells": len(sells),
        "n_closes": len(snapshot.closes),
        "total_cost": total_cost,
        "top_contributors": top,
        "bottom_contributors": bottom,
        "realized_trades": realized,
    }


def analyze_pnl_window(window: SnapshotWindow) -> dict:
    """Multi-day PnL analysis."""
    valid_snapshots = [
        s for s, v in zip(window.snapshots, window.valid_mask) if v
    ]
    if not valid_snapshots:
        return {"error": "no valid snapshots"}

    daily_pnls = []
    for s in valid_snapshots:
        pnl = _safe_float(s.equity.get("daily_pnl_pct", "0"))
        daily_pnls.append(pnl)

    # Win/loss days
    win_days = sum(1 for p in daily_pnls if p > 0)
    loss_days = sum(1 for p in daily_pnls if p < 0)
    total_days = len(daily_pnls)

    avg_pnl = sum(daily_pnls) / total_days if total_days else 0
    cumulative = 1.0
    for p in daily_pnls:
        cumulative *= (1 + p)
    total_return = cumulative - 1

    # All closes in window
    all_closes = []
    for s in valid_snapshots:
        for c in s.closes:
            all_closes.append({
                "code": c.get("code", "").strip(),
                "pnl_pct": _safe_float(c.get("pnl_pct", "0")),
                "hold_days": _safe_int(c.get("hold_days", "0")),
                "exit_reason": c.get("exit_reason", "").strip(),
            })

    win_trades = [c for c in all_closes if c["pnl_pct"] > 0]
    loss_trades = [c for c in all_closes if c["pnl_pct"] <= 0]

    return {
        "window": f"{window.start_date}~{window.end_date}",
        "valid_days": total_days,
        "coverage": window.coverage_ratio,
        "total_return_pct": total_return,
        "avg_daily_pnl_pct": avg_pnl,
        "win_days": win_days,
        "loss_days": loss_days,
        "win_day_rate": win_days / total_days if total_days else 0,
        "total_closes": len(all_closes),
        "win_trades": len(win_trades),
        "loss_trades": len(loss_trades),
        "win_rate": len(win_trades) / len(all_closes) if all_closes else 0,
        "avg_win_pct": (sum(c["pnl_pct"] for c in win_trades) / len(win_trades)
                        if win_trades else 0),
        "avg_loss_pct": (sum(c["pnl_pct"] for c in loss_trades) / len(loss_trades)
                         if loss_trades else 0),
    }


def _safe_float(v) -> float:
    try:
        return float(str(v).strip() or "0")
    except (ValueError, TypeError):
        return 0.0


def _safe_int(v) -> int:
    try:
        return int(float(str(v).strip() or "0"))
    except (ValueError, TypeError):
        return 0
