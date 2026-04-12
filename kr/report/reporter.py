"""
reporter.py — Trade logging and report generation (PostgreSQL)
===============================================================
All report data is stored in PostgreSQL report_* tables.
DB write failures are suppressed (logged only) — never propagate to callers.
"""
from __future__ import annotations
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
    """Log trades and closed positions to PostgreSQL report_* tables."""

    def __init__(self, log_dir: Path, mode: str = "LIVE"):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._mode = mode

        # Initialize DB provider
        try:
            from data.db_provider import DbProvider
            self._db = DbProvider()
            self._db.ensure_report_tables()
            logger.info("[REPORT] PostgreSQL report_* tables ready (mode=%s)", mode)
        except Exception:
            logger.exception("[REPORT_DB_INIT_FAIL] DB init failed, reporting disabled")
            self._db = None

    # ── Trades ────────────────────────────────────────────────────

    def log_trade(self, code: str, side: str, qty: int, price: float,
                  cost: float = 0, mode: str = "LIVE",
                  event_id: str = "", slippage_pct: str = "N/A"):
        """Log a buy or sell trade."""
        if not self._db:
            return
        eid = event_id or make_event_id(code, side)
        try:
            self._db.insert_report_trade(
                date.today().strftime("%Y-%m-%d"),
                code, side, qty, price, cost, slippage_pct, mode, eid)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_trade %s %s %s", code, side, eid)

    def log_close(self, trade: dict, exit_reason: str, mode: str = "LIVE",
                  event_id: str = "",
                  entry_rank: int = 0, score_mom: float = 0.0,
                  max_hwm_pct: float = 0.0):
        """Log a closed position."""
        if not self._db:
            return
        code = trade.get("code", trade.get("ticker", ""))
        eid = event_id or make_event_id(code, exit_reason)
        try:
            self._db.insert_report_close(
                date.today().strftime("%Y-%m-%d"),
                code, exit_reason,
                trade.get("quantity", 0),
                trade.get("entry_price", 0),
                trade.get("exit_price", 0),
                trade.get("entry_date", ""),
                trade.get("hold_days", 0),
                trade.get("pnl_pct", 0),
                trade.get("pnl_amount", 0),
                mode, eid, entry_rank, score_mom, max_hwm_pct)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_close %s %s", code, eid)

    def log_rebalance_summary(self, sells: int, buys: int, equity: float):
        """Log rebalance event marker."""
        if not self._db:
            return
        eid = make_event_id("REBALANCE", "SUMMARY")
        try:
            self._db.insert_report_trade(
                date.today().strftime("%Y-%m-%d"),
                "REBALANCE", "SUMMARY", 0, equity,
                0, f"sells={sells},buys={buys}", "LIVE", eid)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_rebalance_summary")

    # ── Equity (extended) ─────────────────────────────────────────

    def log_equity(self, equity: float, cash: float, n_positions: int,
                   daily_pnl: float, monthly_dd: float, *,
                   risk_mode: str = "NORMAL",
                   rebalance_executed: bool = False,
                   price_fail_count: int = 0,
                   reconcile_corrections: int = 0,
                   monitor_only: bool = False,
                   kospi_close: float = 0.0,
                   kosdaq_close: float = 0.0,
                   regime: str = "",
                   kospi_ma200: float = 0.0,
                   breadth: float = 0.0):
        """Log daily equity snapshot with context tags."""
        if not self._db:
            return
        try:
            self._db.insert_report_equity(
                date.today().strftime("%Y-%m-%d"),
                equity, cash, n_positions,
                daily_pnl, monthly_dd, risk_mode,
                "Y" if rebalance_executed else "N",
                price_fail_count, reconcile_corrections,
                "Y" if monitor_only else "N",
                kospi_close or 0, kosdaq_close or 0,
                regime, kospi_ma200 or 0, breadth or 0,
                self._mode)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_equity")

    # ── Daily Position Snapshot (EOD) ────────────────────────────

    def log_daily_positions(self, positions: dict, today_str: str = "",
                            buy_cost: float = 0.00115,
                            sell_cost: float = 0.00295):
        """Log all open positions at EOD with unrealized P&L."""
        if not self._db:
            return
        dt = today_str or date.today().strftime("%Y-%m-%d")
        est_cost_pct = buy_cost + sell_cost

        for code, pos in sorted(positions.items()):
            try:
                mv = pos.quantity * pos.current_price if pos.current_price > 0 else 0
                cost_basis = pos.quantity * pos.avg_price
                pnl_pct = (pos.current_price / pos.avg_price - 1) if pos.avg_price > 0 else 0
                pnl_amt = mv - cost_basis
                net_pnl_pct = pnl_pct - est_cost_pct

                hold_days = 0
                if pos.entry_date:
                    try:
                        fmt = "%Y-%m-%d" if "-" in pos.entry_date else "%Y%m%d"
                        ed = datetime.strptime(pos.entry_date, fmt).date()
                        hold_days = (date.today() - ed).days
                    except (ValueError, TypeError):
                        pass

                hwm_pct = (pos.high_watermark / pos.avg_price - 1) if pos.avg_price > 0 else 0

                self._db.insert_report_daily_position(
                    dt, code, pos.quantity,
                    pos.avg_price, pos.current_price, mv,
                    pnl_pct, pnl_amt, est_cost_pct, net_pnl_pct,
                    pos.high_watermark, pos.trail_stop_price,
                    pos.entry_date, hold_days, hwm_pct, self._mode)
            except Exception:
                logger.exception("[REPORT_DB_WRITE_FAIL] log_daily_positions %s", code)

        logger.info(f"Daily positions logged: {len(positions)} open positions")

    # ── Decision Log (forensic) ───────────────────────────────────

    def log_decision_buy(self, code: str, reason: str,
                         score_vol: float = 0, score_mom: float = 0,
                         rank: int = 0, target_weight: float = 0,
                         price: float = 0, cash_before: float = 0,
                         event_id: str = "", regime: str = ""):
        """Log buy decision context."""
        eid = event_id or make_event_id(code, "BUY")
        if not self._db:
            return eid
        try:
            self._db.insert_report_decision(
                eid, date.today().strftime("%Y-%m-%d"),
                code, "BUY", reason,
                score_vol, score_mom, rank,
                target_weight, price, cash_before,
                0, 0, 0, 0, regime)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_decision_buy %s", code)
        return eid

    def log_decision_sell(self, code: str, reason: str,
                          price: float = 0, high_watermark: float = 0,
                          trail_stop_price: float = 0,
                          pnl_pct: float = 0, hold_days: int = 0,
                          event_id: str = "", regime: str = ""):
        """Log sell decision context."""
        eid = event_id or make_event_id(code, "SELL")
        if not self._db:
            return eid
        try:
            self._db.insert_report_decision(
                eid, date.today().strftime("%Y-%m-%d"),
                code, "SELL", reason,
                0, 0, 0, 0, price, 0,
                high_watermark, trail_stop_price,
                pnl_pct, hold_days, regime)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_decision_sell %s", code)
        return eid

    # ── Reconcile Log (forensic) ──────────────────────────────────

    def log_reconcile(self, code: str, diff_type: str,
                      engine_qty: int = 0, broker_qty: int = 0,
                      engine_avg: float = 0, broker_avg: float = 0,
                      resolution: str = ""):
        """Log broker reconciliation diff."""
        if not self._db:
            return
        try:
            self._db.insert_report_reconcile(
                date.today().strftime("%Y-%m-%d"),
                datetime.now().strftime("%H:%M:%S"),
                code, diff_type,
                engine_qty, broker_qty,
                engine_avg, broker_avg, resolution)
        except Exception:
            logger.exception("[REPORT_DB_WRITE_FAIL] log_reconcile %s %s", code, diff_type)


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
