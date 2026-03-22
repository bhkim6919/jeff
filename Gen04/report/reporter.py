"""
reporter.py — Trade logging and report generation
===================================================
Simplified from Gen3 (monthly rebalance focus).

Outputs:
  trades.csv    — all buy/sell records
  close_log.csv — closed position details
  equity_log.csv — daily equity snapshots
"""
from __future__ import annotations
import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("gen4.report")


class TradeLogger:
    """Log trades and closed positions to CSV."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._trades_file = log_dir / "trades.csv"
        self._close_file = log_dir / "close_log.csv"
        self._equity_file = log_dir / "equity_log.csv"

        self._ensure_headers()

    def _ensure_headers(self):
        """Create CSV headers if files don't exist."""
        if not self._trades_file.exists():
            self._write_header(self._trades_file, [
                "date", "code", "side", "quantity", "price",
                "cost", "slippage_pct", "mode",
            ])

        if not self._close_file.exists():
            self._write_header(self._close_file, [
                "date", "code", "exit_reason", "quantity",
                "entry_price", "exit_price", "entry_date",
                "hold_days", "pnl_pct", "pnl_amount", "mode",
            ])

        if not self._equity_file.exists():
            self._write_header(self._equity_file, [
                "date", "equity", "cash", "n_positions",
                "daily_pnl_pct", "monthly_dd_pct",
            ])

    def _write_header(self, path: Path, columns: list):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(columns)

    def log_trade(self, code: str, side: str, qty: int, price: float,
                  cost: float = 0, mode: str = "LIVE"):
        """Log a buy or sell trade."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            code, side, qty, f"{price:.2f}",
            f"{cost:.2f}", "0.00", mode,
        ]
        self._append(self._trades_file, row)

    def log_close(self, trade: dict, exit_reason: str, mode: str = "LIVE"):
        """Log a closed position."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            trade.get("code", trade.get("ticker", "")),
            exit_reason,
            trade.get("quantity", 0),
            f"{trade.get('entry_price', 0):.2f}",
            f"{trade.get('exit_price', 0):.2f}",
            trade.get("entry_date", ""),
            trade.get("hold_days", 0),
            f"{trade.get('pnl_pct', 0):.4f}",
            f"{trade.get('pnl_amount', 0):.2f}",
            mode,
        ]
        self._append(self._close_file, row)

    def log_equity(self, equity: float, cash: float, n_positions: int,
                   daily_pnl: float, monthly_dd: float):
        """Log daily equity snapshot."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            f"{equity:.2f}", f"{cash:.2f}", n_positions,
            f"{daily_pnl:.4f}", f"{monthly_dd:.4f}",
        ]
        self._append(self._equity_file, row)

    def log_rebalance_summary(self, sells: int, buys: int, equity: float):
        """Log rebalance event marker."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            "REBALANCE", "SUMMARY", f"sells={sells}", f"buys={buys}",
            f"{equity:.2f}", "0.00", "LIVE",
        ]
        self._append(self._trades_file, row)

    def _append(self, path: Path, row: list):
        try:
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow(row)
        except Exception as e:
            logger.error(f"Failed to write to {path.name}: {e}")


def generate_monthly_summary(trades: List[dict], equity: float,
                              month: str = "") -> str:
    """Generate text summary for monthly report."""
    if not month:
        month = date.today().strftime("%Y-%m")

    n = len(trades)
    if n == 0:
        return f"[{month}] No trades."

    wins = [t for t in trades if t.get("pnl_pct", 0) > 0]
    losses = [t for t in trades if t.get("pnl_pct", 0) <= 0]
    total_pnl = sum(t.get("pnl_amount", 0) for t in trades)

    lines = [
        f"[{month}] Monthly Summary",
        f"  Trades: {n} (wins={len(wins)}, losses={len(losses)})",
        f"  Win Rate: {len(wins)/n*100:.0f}%",
        f"  Total PnL: {total_pnl:+,.0f} KRW",
        f"  Equity: {equity:,.0f} KRW",
    ]

    # Exit reason breakdown
    reasons = {}
    for t in trades:
        r = t.get("exit_reason", "UNKNOWN")
        reasons.setdefault(r, []).append(t.get("pnl_pct", 0))

    for reason, pnls in sorted(reasons.items()):
        avg = sum(pnls) / len(pnls) * 100
        wr = len([p for p in pnls if p > 0]) / len(pnls) * 100
        lines.append(f"  {reason}: {len(pnls)} trades, WR={wr:.0f}%, Avg={avg:+.1f}%")

    return "\n".join(lines)
