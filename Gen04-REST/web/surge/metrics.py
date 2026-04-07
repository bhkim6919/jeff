# -*- coding: utf-8 -*-
"""
metrics.py -- Trade Statistics & Daily Summary
=================================================
거래 기록 + 17개 지표 일별 요약 + CSV/JSON 저장.
"""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("gen4.rest.surge")

# Output directory base
SURGE_RESULTS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "surge_results"

TRADE_CSV_FIELDS = [
    "trade_id", "code", "name", "entry_time", "exit_time",
    "entry_fill_price", "exit_fill_price", "raw_ask_at_entry", "raw_bid_at_exit",
    "qty", "gross_pnl_pct", "net_pnl_pct", "gross_pnl_krw", "net_pnl_krw",
    "entry_reason", "exit_reason",
    "entry_slippage_pct", "exit_slippage_pct",
    "fee_entry", "fee_exit", "tax",
    "holding_seconds",
]


@dataclass
class TradeRecord:
    trade_id: int = 0
    code: str = ""
    name: str = ""
    entry_time: str = ""
    exit_time: str = ""
    entry_fill_price: int = 0
    exit_fill_price: int = 0
    raw_ask_at_entry: int = 0
    raw_bid_at_exit: int = 0
    qty: int = 0
    gross_pnl_pct: float = 0.0
    net_pnl_pct: float = 0.0
    gross_pnl_krw: float = 0.0
    net_pnl_krw: float = 0.0
    entry_reason: str = ""
    exit_reason: str = ""       # TP / SL / TIME_EXIT / FORCE_EXIT
    entry_slippage_pct: float = 0.0
    exit_slippage_pct: float = 0.0
    fee_entry: float = 0.0
    fee_exit: float = 0.0
    tax: float = 0.0
    holding_seconds: float = 0.0


def compute_summary(trades: List[TradeRecord]) -> dict:
    """Compute daily summary with 17+ metrics."""
    if not trades:
        return {"total_trades": 0}

    wins = [t for t in trades if t.net_pnl_pct > 0]
    losses = [t for t in trades if t.net_pnl_pct <= 0]

    total = len(trades)
    win_count = len(wins)
    loss_count = len(losses)
    win_rate = win_count / total * 100 if total else 0

    avg_win = sum(t.net_pnl_pct for t in wins) / win_count if wins else 0
    avg_loss = sum(t.net_pnl_pct for t in losses) / loss_count if losses else 0

    # Profit factor
    total_win_pnl = sum(t.net_pnl_krw for t in wins)
    total_loss_pnl = abs(sum(t.net_pnl_krw for t in losses))
    profit_factor = total_win_pnl / total_loss_pnl if total_loss_pnl > 0 else float("inf")

    # Expectancy
    expectancy = sum(t.net_pnl_pct for t in trades) / total if total else 0

    # Total PnL
    total_pnl_pct = sum(t.net_pnl_pct for t in trades)
    total_pnl_krw = sum(t.net_pnl_krw for t in trades)

    # Max consecutive losses
    max_consec = _max_consecutive_losses(trades)

    # Time-of-day performance
    time_perf = _time_of_day_performance(trades)

    # Exit reason distribution
    exit_dist: Dict[str, int] = {}
    for t in trades:
        exit_dist[t.exit_reason] = exit_dist.get(t.exit_reason, 0) + 1

    # Slippage before/after
    gross_total = sum(t.gross_pnl_pct for t in trades)
    net_total = sum(t.net_pnl_pct for t in trades)

    # Per-stock top/bottom
    stock_pnl: Dict[str, float] = {}
    for t in trades:
        stock_pnl[t.code] = stock_pnl.get(t.code, 0) + t.net_pnl_pct
    sorted_stocks = sorted(stock_pnl.items(), key=lambda x: x[1], reverse=True)
    top_stocks = sorted_stocks[:5]
    bottom_stocks = sorted_stocks[-5:] if len(sorted_stocks) > 5 else sorted_stocks

    # Overheating chase failures: entered with high change_pct but lost
    # (approximation: short holding time + loss)
    overheat_count = sum(1 for t in trades
                         if t.holding_seconds < 60 and t.net_pnl_pct < 0)

    return {
        "total_trades": total,
        "win_count": win_count,
        "loss_count": loss_count,
        "win_rate": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 3),
        "avg_loss_pct": round(avg_loss, 3),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "Inf",
        "expectancy_pct": round(expectancy, 3),
        "total_pnl_pct": round(total_pnl_pct, 3),
        "total_pnl_krw": round(total_pnl_krw),
        "max_consecutive_losses": max_consec,
        "time_of_day_performance": time_perf,
        "exit_reason_distribution": exit_dist,
        "gross_vs_net": {
            "gross_total_pct": round(gross_total, 3),
            "net_total_pct": round(net_total, 3),
            "cost_impact_pct": round(gross_total - net_total, 3),
        },
        "top_stocks": [{"code": c, "pnl_pct": round(p, 3)} for c, p in top_stocks],
        "bottom_stocks": [{"code": c, "pnl_pct": round(p, 3)} for c, p in bottom_stocks],
        "overheating_chase_failures": overheat_count,
        "avg_holding_seconds": round(
            sum(t.holding_seconds for t in trades) / total, 1
        ) if total else 0,
        "total_fees": round(sum(t.fee_entry + t.fee_exit + t.tax for t in trades)),
    }


def _max_consecutive_losses(trades: List[TradeRecord]) -> int:
    max_c = 0
    current = 0
    for t in trades:
        if t.net_pnl_pct <= 0:
            current += 1
            max_c = max(max_c, current)
        else:
            current = 0
    return max_c


def _time_of_day_performance(trades: List[TradeRecord]) -> Dict[str, dict]:
    """Group trades by hour, compute win_rate and avg_pnl."""
    hourly: Dict[str, List[TradeRecord]] = {}
    for t in trades:
        try:
            hour = t.entry_time[11:13] if len(t.entry_time) > 13 else "??"
            bucket = f"{hour}:00"
        except Exception:
            bucket = "??"
        hourly.setdefault(bucket, []).append(t)

    result = {}
    for bucket, tlist in sorted(hourly.items()):
        wins = sum(1 for t in tlist if t.net_pnl_pct > 0)
        result[bucket] = {
            "count": len(tlist),
            "win_rate": round(wins / len(tlist) * 100, 1) if tlist else 0,
            "avg_pnl_pct": round(sum(t.net_pnl_pct for t in tlist) / len(tlist), 3),
        }
    return result


# ── File I/O ─────────────────────────────────────────────────

def get_output_dir(date_str: Optional[str] = None) -> Path:
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    d = SURGE_RESULTS_DIR / date_str
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_trades_csv(trades: List[TradeRecord], out_dir: Path) -> Path:
    path = out_dir / "trades_sim.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_CSV_FIELDS)
        writer.writeheader()
        for t in trades:
            writer.writerow({k: getattr(t, k, "") for k in TRADE_CSV_FIELDS})
    return path


def save_summary_json(summary: dict, out_dir: Path) -> Path:
    path = out_dir / "daily_summary_sim.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    return path


def save_state_json(state: dict, out_dir: Path) -> Path:
    path = out_dir / "state_sim.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    return path
