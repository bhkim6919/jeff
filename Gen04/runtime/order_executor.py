"""
order_executor.py — Order execution layer
===========================================
Bridges portfolio_manager and kiwoom_provider.
Handles paper/live mode, slippage validation, fill tracking.
"""
from __future__ import annotations
import logging
import time
from datetime import date
from typing import Dict, Optional

logger = logging.getLogger("gen4.executor")

MAX_SLIPPAGE_CAP = 0.03  # 3% max slippage tolerance
ORDER_INTERVAL = 0.3      # seconds between orders


class OrderExecutor:
    """Execute buy/sell orders with paper/live mode support."""

    def __init__(self, provider, tracker, trade_logger, paper: bool = True):
        """
        Args:
            provider: Gen4KiwoomProvider (or None for pure mock)
            tracker: OrderTracker for fill idempotency
            trade_logger: TradeLogger for CSV logging
            paper: True = simulate fills, False = real Kiwoom orders
        """
        self.provider = provider
        self.tracker = tracker
        self.trade_logger = trade_logger
        self.paper = paper

    def get_live_price(self, code: str) -> float:
        """Get current market price from Kiwoom."""
        if self.provider is None:
            return 0.0
        try:
            return self.provider.get_current_price(code)
        except Exception as e:
            logger.warning(f"Price fetch failed for {code}: {e}")
            return 0.0

    def execute_sell(self, code: str, qty: int,
                     reason: str = "REBALANCE_EXIT") -> Dict:
        """
        Execute sell order.

        Returns: {order_no, exec_price, exec_qty, error}
        """
        if qty <= 0:
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"invalid qty={qty}"}

        # Register order
        rec = self.tracker.register(code, "SELL", qty, 0, reason=reason)

        if self.paper:
            return self._simulate_sell(code, qty, rec, reason)

        return self._live_sell(code, qty, rec, reason)

    def execute_buy(self, code: str, qty: int,
                    reason: str = "REBALANCE_ENTRY") -> Dict:
        """
        Execute buy order.

        Returns: {order_no, exec_price, exec_qty, error}
        """
        if qty <= 0:
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"invalid qty={qty}"}

        rec = self.tracker.register(code, "BUY", qty, 0, reason=reason)

        if self.paper:
            return self._simulate_buy(code, qty, rec, reason)

        return self._live_buy(code, qty, rec, reason)

    # ── Paper Mode ───────────────────────────────────────────────────────────

    def _simulate_sell(self, code, qty, rec, reason) -> Dict:
        price = self.get_live_price(code)
        if price <= 0:
            self.tracker.mark_rejected(rec.order_id, "no price")
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "no price for simulation"}

        order_no = f"PAPER_{rec.order_id}"
        self.tracker.mark_filled(rec.order_id, price, qty)
        self.tracker.record_fill(order_no, "SELL", code, qty, price, qty, "PAPER")

        self.trade_logger.log_trade(code, "SELL", qty, price, mode="PAPER")
        logger.info(f"[PAPER SELL] {code} qty={qty} @ {price:,.0f} ({reason})")

        return {"order_no": order_no, "exec_price": price,
                "exec_qty": qty, "error": ""}

    def _simulate_buy(self, code, qty, rec, reason) -> Dict:
        price = self.get_live_price(code)
        if price <= 0:
            self.tracker.mark_rejected(rec.order_id, "no price")
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "no price for simulation"}

        order_no = f"PAPER_{rec.order_id}"
        self.tracker.mark_filled(rec.order_id, price, qty)
        self.tracker.record_fill(order_no, "BUY", code, qty, price, qty, "PAPER")

        self.trade_logger.log_trade(code, "BUY", qty, price, mode="PAPER")
        logger.info(f"[PAPER BUY] {code} qty={qty} @ {price:,.0f} ({reason})")

        return {"order_no": order_no, "exec_price": price,
                "exec_qty": qty, "error": ""}

    # ── Live Mode ────────────────────────────────────────────────────────────

    def _live_sell(self, code, qty, rec, reason) -> Dict:
        # Pre-check: verify broker holds this stock
        sell_info = self.provider.query_sellable_qty(code)
        if sell_info.get("error"):
            self.tracker.mark_rejected(rec.order_id, sell_info["error"])
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"sellcheck failed: {sell_info['error']}"}

        broker_hold = sell_info.get("hold_qty", 0)
        if broker_hold <= 0:
            self.tracker.mark_rejected(rec.order_id, "broker_hold=0")
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"broker holds 0 shares of {code}"}

        actual_qty = min(qty, broker_hold)
        if actual_qty < qty:
            logger.warning(f"Sell qty adjusted: {qty} -> {actual_qty} (broker_hold={broker_hold})")

        self.tracker.mark_submitted(rec.order_id)
        time.sleep(ORDER_INTERVAL)

        result = self.provider.send_order(code, "SELL", actual_qty, 0, "03")

        if result.get("error"):
            self.tracker.mark_rejected(rec.order_id, result["error"])
            logger.error(f"SELL FAILED {code}: {result['error']}")
        else:
            self.tracker.mark_filled(rec.order_id, result["exec_price"], result["exec_qty"])
            self.tracker.record_fill(
                result["order_no"], "SELL", code,
                result["exec_qty"], result["exec_price"],
                result["exec_qty"], "CHEJAN")
            self.trade_logger.log_trade(
                code, "SELL", result["exec_qty"], result["exec_price"], mode="LIVE")

        return result

    def _live_buy(self, code, qty, rec, reason) -> Dict:
        self.tracker.mark_submitted(rec.order_id)
        time.sleep(ORDER_INTERVAL)

        result = self.provider.send_order(code, "BUY", qty, 0, "03")

        if result.get("error"):
            self.tracker.mark_rejected(rec.order_id, result["error"])
            logger.error(f"BUY FAILED {code}: {result['error']}")
        else:
            # Slippage check
            decision_price = self.get_live_price(code)
            if decision_price > 0 and result["exec_price"] > 0:
                slip = abs(result["exec_price"] - decision_price) / decision_price
                if slip > MAX_SLIPPAGE_CAP:
                    logger.warning(
                        f"HIGH SLIPPAGE {code}: {slip:.1%} "
                        f"(decision={decision_price:,.0f}, fill={result['exec_price']:,.0f})")

            self.tracker.mark_filled(rec.order_id, result["exec_price"], result["exec_qty"])
            self.tracker.record_fill(
                result["order_no"], "BUY", code,
                result["exec_qty"], result["exec_price"],
                result["exec_qty"], "CHEJAN")
            self.trade_logger.log_trade(
                code, "BUY", result["exec_qty"], result["exec_price"], mode="LIVE")

        return result
