"""
reporter.py — Trade logging and report generation
===================================================
Outputs:
  trades.csv       — all buy/sell records
  close_log.csv    — closed position details
  equity_log.csv   — daily equity snapshots
  decision_log.csv — buy/sell decision context (forensic)
  reconcile_log.csv — broker sync diffs (forensic)
"""
from __future__ import annotations
import csv
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("gen4.report")


def make_event_id(code: str, action: str) -> str:
    """Generate a unique event ID: YYYYMMDD_HHMMSS_{code}_{action}."""
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{code}_{action}"


class TradeLogger:
    """Log trades and closed positions to CSV."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._trades_file = log_dir / "trades.csv"
        self._close_file = log_dir / "close_log.csv"
        self._equity_file = log_dir / "equity_log.csv"
        self._decision_file = log_dir / "decision_log.csv"
        self._reconcile_file = log_dir / "reconcile_log.csv"
        self._positions_file = log_dir / "daily_positions.csv"

        self._ensure_headers()

    def _ensure_headers(self):
        """Create CSV headers if files don't exist."""
        if not self._trades_file.exists():
            self._write_header(self._trades_file, [
                "date", "code", "side", "quantity", "price",
                "cost", "slippage_pct", "mode", "event_id",
            ])

        if not self._close_file.exists():
            self._write_header(self._close_file, [
                "date", "code", "exit_reason", "quantity",
                "entry_price", "exit_price", "entry_date",
                "hold_days", "pnl_pct", "pnl_amount", "mode", "event_id",
            ])

        if not self._equity_file.exists():
            self._write_header(self._equity_file, [
                "date", "equity", "cash", "n_positions",
                "daily_pnl_pct", "monthly_dd_pct",
                "risk_mode", "rebalance_executed", "price_fail_count",
                "reconcile_corrections", "monitor_only",
            ])

        if not self._decision_file.exists():
            self._write_header(self._decision_file, [
                "event_id", "date", "code", "side", "reason",
                "score_vol", "score_mom", "rank",
                "target_weight", "price", "cash_before",
                "high_watermark", "trail_stop_price",
                "pnl_pct", "hold_days",
            ])

        if not self._reconcile_file.exists():
            self._write_header(self._reconcile_file, [
                "date", "time", "code", "diff_type",
                "engine_qty", "broker_qty",
                "engine_avg", "broker_avg",
                "resolution",
            ])

        self._ensure_header(self._positions_file, [
            "date", "code", "quantity", "avg_price",
            "current_price", "market_value",
            "pnl_pct", "pnl_amount",
            "est_cost_pct", "net_pnl_pct",
            "high_watermark", "trail_stop_price",
            "entry_date", "hold_days",
        ])

    def _write_header(self, path: Path, columns: list):
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(columns)

    def _ensure_header(self, path: Path, expected_columns: list):
        """Validate CSV header matches expected columns.

        If column count mismatches, the old file is backed up as .mismatch_backup
        and a fresh file with correct header is created. NO silent padding/truncation.
        """
        if not path.exists():
            self._write_header(path, expected_columns)
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                header = f.readline().strip().split(",")
            if header == expected_columns:
                return  # header matches, no action needed

            if len(header) == len(expected_columns):
                # Same column count, just rename header (safe)
                import pandas as pd
                logger.warning(
                    "[CSV_HEADER_FIX] %s: renaming %d cols (count matches)",
                    path.name, len(header))
                df = pd.read_csv(path, encoding="utf-8-sig", header=0)
                df.columns = expected_columns
                df.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                # Column count mismatch — backup old file, start fresh.
                # NO silent padding/truncation to prevent data corruption.
                import shutil
                backup = path.with_suffix(".mismatch_backup")
                shutil.copy2(path, backup)
                logger.error(
                    "[CSV_HEADER_MISMATCH_FATAL] %s: %d cols -> %d cols. "
                    "Old file backed up to %s. Starting fresh CSV.",
                    path.name, len(header), len(expected_columns), backup.name)
                self._write_header(path, expected_columns)
        except Exception as e:
            logger.error("[CSV_HEADER_FIX] failed for %s: %s", path.name, e)

    # ── Trades ────────────────────────────────────────────────────

    def log_trade(self, code: str, side: str, qty: int, price: float,
                  cost: float = 0, mode: str = "LIVE",
                  event_id: str = "", slippage_pct: str = "N/A"):
        """Log a buy or sell trade. slippage_pct='N/A' if unmeasured."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            code, side, qty, f"{price:.2f}",
            f"{cost:.2f}", slippage_pct, mode,
            event_id or make_event_id(code, side),
        ]
        self._append(self._trades_file, row)

    def log_close(self, trade: dict, exit_reason: str, mode: str = "LIVE",
                  event_id: str = ""):
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
            event_id or make_event_id(
                trade.get("code", ""), exit_reason),
        ]
        self._append(self._close_file, row)

    def log_rebalance_summary(self, sells: int, buys: int, equity: float):
        """Log rebalance event marker."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            "REBALANCE", "SUMMARY", f"sells={sells}", f"buys={buys}",
            f"{equity:.2f}", "0.00", "LIVE", "",
        ]
        self._append(self._trades_file, row)

    # ── Equity (extended) ─────────────────────────────────────────

    def log_equity(self, equity: float, cash: float, n_positions: int,
                   daily_pnl: float, monthly_dd: float, *,
                   risk_mode: str = "NORMAL",
                   rebalance_executed: bool = False,
                   price_fail_count: int = 0,
                   reconcile_corrections: int = 0,
                   monitor_only: bool = False):
        """Log daily equity snapshot with context tags."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            f"{equity:.2f}", f"{cash:.2f}", n_positions,
            f"{daily_pnl:.4f}", f"{monthly_dd:.4f}",
            risk_mode,
            "Y" if rebalance_executed else "N",
            price_fail_count,
            reconcile_corrections,
            "Y" if monitor_only else "N",
        ]
        self._append(self._equity_file, row)

    # ── Daily Position Snapshot (EOD) ────────────────────────────

    def log_daily_positions(self, positions: dict, today_str: str = "",
                            buy_cost: float = 0.00115,
                            sell_cost: float = 0.00295):
        """
        Log all open positions at EOD with unrealized P&L.

        Args:
            positions: {code: Position} from PortfolioManager.positions
            today_str: date string override (default: today)
            buy_cost: buy transaction cost rate (for est_cost_pct)
            sell_cost: sell transaction cost rate (for est_cost_pct)
        """
        dt = today_str or date.today().strftime("%Y-%m-%d")
        est_cost_pct = buy_cost + sell_cost  # ~0.41%

        for code, pos in sorted(positions.items()):
            mv = pos.quantity * pos.current_price if pos.current_price > 0 else 0
            cost = pos.quantity * pos.avg_price
            pnl_pct = (pos.current_price / pos.avg_price - 1) if pos.avg_price > 0 else 0
            pnl_amt = mv - cost
            net_pnl_pct = pnl_pct - est_cost_pct

            hold_days = 0
            if pos.entry_date:
                try:
                    fmt = "%Y-%m-%d" if "-" in pos.entry_date else "%Y%m%d"
                    ed = datetime.strptime(pos.entry_date, fmt).date()
                    hold_days = (date.today() - ed).days
                except (ValueError, TypeError):
                    pass

            row = [
                dt, code, pos.quantity,
                f"{pos.avg_price:.2f}", f"{pos.current_price:.2f}",
                f"{mv:.2f}", f"{pnl_pct:.4f}", f"{pnl_amt:.2f}",
                f"{est_cost_pct:.4f}", f"{net_pnl_pct:.4f}",
                f"{pos.high_watermark:.2f}", f"{pos.trail_stop_price:.2f}",
                pos.entry_date, hold_days,
            ]
            self._append(self._positions_file, row)

        logger.info(f"Daily positions logged: {len(positions)} open positions")

    # ── Decision Log (forensic) ───────────────────────────────────

    def log_decision_buy(self, code: str, reason: str,
                         score_vol: float = 0, score_mom: float = 0,
                         rank: int = 0, target_weight: float = 0,
                         price: float = 0, cash_before: float = 0,
                         event_id: str = ""):
        """Log buy decision context."""
        eid = event_id or make_event_id(code, "BUY")
        row = [
            eid, date.today().strftime("%Y-%m-%d"),
            code, "BUY", reason,
            f"{score_vol:.6f}", f"{score_mom:.4f}", rank,
            f"{target_weight:.2f}", f"{price:.2f}", f"{cash_before:.2f}",
            "", "", "", "",
        ]
        self._append(self._decision_file, row)
        return eid

    def log_decision_sell(self, code: str, reason: str,
                          price: float = 0, high_watermark: float = 0,
                          trail_stop_price: float = 0,
                          pnl_pct: float = 0, hold_days: int = 0,
                          event_id: str = ""):
        """Log sell decision context."""
        eid = event_id or make_event_id(code, "SELL")
        row = [
            eid, date.today().strftime("%Y-%m-%d"),
            code, "SELL", reason,
            "", "", "",
            "", f"{price:.2f}", "",
            f"{high_watermark:.2f}", f"{trail_stop_price:.2f}",
            f"{pnl_pct:.4f}", hold_days,
        ]
        self._append(self._decision_file, row)
        return eid

    # ── Reconcile Log (forensic) ──────────────────────────────────

    def log_reconcile(self, code: str, diff_type: str,
                      engine_qty: int = 0, broker_qty: int = 0,
                      engine_avg: float = 0, broker_avg: float = 0,
                      resolution: str = ""):
        """Log broker reconciliation diff."""
        row = [
            date.today().strftime("%Y-%m-%d"),
            datetime.now().strftime("%H:%M:%S"),
            code, diff_type,
            engine_qty, broker_qty,
            f"{engine_avg:.2f}", f"{broker_avg:.2f}",
            resolution,
        ]
        self._append(self._reconcile_file, row)

    # ── Internal ──────────────────────────────────────────────────

    def _append(self, path: Path, row: list):
        try:
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow(row)
        except Exception as e:
            logger.error(f"Failed to write to {path.name}: {e}")


# ── Forensic Snapshot ─────────────────────────────────────────────────────

def save_forensic_snapshot(state_dir: Path, *,
                           portfolio_data: dict = None,
                           runtime_data: dict = None,
                           prices: dict = None,
                           target_date: str = "",
                           pending_orders: list = None,
                           price_fail_codes: list = None,
                           error_msg: str = "",
                           extra: dict = None):
    """
    Dump a forensic snapshot on critical error.
    Saved to state/forensic_YYYYMMDD_HHMMSS.json
    """
    state_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = state_dir / f"forensic_{ts}.json"

    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "error": error_msg,
        "portfolio": portfolio_data or {},
        "runtime": runtime_data or {},
        "prices_used": prices or {},
        "target_date": target_date,
        "pending_orders": pending_orders or [],
        "price_fail_codes": price_fail_codes or [],
    }
    if extra:
        snapshot["extra"] = extra

    try:
        path.write_text(
            json.dumps(snapshot, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")
        logger.warning(f"Forensic snapshot saved: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to save forensic snapshot: {e}")
        return None


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
