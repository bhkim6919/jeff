"""
order_executor.py — Order execution layer
===========================================
Bridges portfolio_manager and kiwoom_provider.
Handles mock/kiwoom mode, slippage validation, fill tracking.

Mode terminology:
  mock     = simulate=True, internal simulation (--mock mode)
  paper    = simulate=False, Kiwoom MOCK server (모의투자)
  live     = simulate=False, Kiwoom REAL server (실거래)
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
    """Execute buy/sell orders with mock/kiwoom mode support.

    TRADING_MODE is the operator's intended mode.
    server_type is the broker's actual connected environment.
    If they do not match, abort immediately.
      mock  = internal simulation only
      paper = broker mock trading
      live  = broker real trading

    simulate=True  → mock mode: internal fill simulation (no broker)
    simulate=False → kiwoom mode: orders sent via Kiwoom API (paper or live)
    """

    def __init__(self, provider, tracker, trade_logger,
                 simulate: bool = True,
                 trading_mode: str = "mock",
                 **kwargs):
        """
        Args:
            provider: Gen4KiwoomProvider (or None for pure mock)
            tracker: OrderTracker for fill idempotency
            trade_logger: TradeLogger for CSV logging
            simulate: True = internal simulation (mock), False = Kiwoom API (paper/live)
            trading_mode: "mock" | "paper" | "live" — for logging and order-level guard
        """
        # Backward compatibility: paper= → simulate=
        if "paper" in kwargs:
            simulate = kwargs.pop("paper")
            logger.warning("[DEPRECATED_PARAM] OrderExecutor.paper is deprecated; "
                           "use simulate instead")

        self.provider = provider
        self.tracker = tracker
        self.trade_logger = trade_logger
        self.simulate = simulate
        self.trading_mode = trading_mode

        # Ghost fill sync (set via set_ghost_fill_context)
        self._portfolio = None
        self._state_mgr = None
        self._buy_cost = 0.00115       # default, overridden by set_ghost_fill_context
        self._ghost_fill_lock = False  # reentrant guard

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

        if self.simulate:
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

        if self.simulate:
            return self._simulate_buy(code, qty, rec, reason)

        return self._live_buy(code, qty, rec, reason)

    # ── Mock Mode (internal simulation, no broker) ──────────────────────────

    def _simulate_sell(self, code, qty, rec, reason) -> Dict:
        price = self.get_live_price(code)
        if price <= 0:
            self.tracker.mark_rejected(rec.order_id, "no price")
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "no price for simulation"}

        order_no = f"MOCK_{rec.order_id}"
        self.tracker.mark_filled(rec.order_id, price, qty)
        self.tracker.record_fill(order_no, "SELL", code, qty, price, qty, "MOCK")

        self.trade_logger.log_trade(code, "SELL", qty, price, mode="MOCK")
        logger.info(f"[MOCK SELL] {code} qty={qty} @ {price:,.0f} ({reason})")

        return {"order_no": order_no, "exec_price": price,
                "exec_qty": qty, "error": ""}

    def _simulate_buy(self, code, qty, rec, reason) -> Dict:
        price = self.get_live_price(code)
        if price <= 0:
            self.tracker.mark_rejected(rec.order_id, "no price")
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "no price for simulation"}

        order_no = f"MOCK_{rec.order_id}"
        self.tracker.mark_filled(rec.order_id, price, qty)
        self.tracker.record_fill(order_no, "BUY", code, qty, price, qty, "MOCK")

        self.trade_logger.log_trade(code, "BUY", qty, price, mode="MOCK")
        logger.info(f"[MOCK BUY] {code} qty={qty} @ {price:,.0f} ({reason})")

        return {"order_no": order_no, "exec_price": price,
                "exec_qty": qty, "error": ""}

    # ── Broker Mode (paper + live via Kiwoom API) ─────────────────────────

    def _check_broker_gate(self) -> None:
        """2nd hard gate: verify trading_mode allows broker orders."""
        if self.trading_mode == "mock":
            raise RuntimeError(
                "[MODE_GATE] mock mode must not send broker orders. "
                "Use simulate=True for mock mode.")
        if self.trading_mode == "paper" and self.provider:
            svr = getattr(self.provider, '_server_type', None)
            if svr and svr != "MOCK":
                raise RuntimeError(
                    f"[MODE_GATE] paper mode requires MOCK server, got {svr}")
        if self.trading_mode == "live" and self.provider:
            svr = getattr(self.provider, '_server_type', None)
            if svr and svr != "REAL":
                raise RuntimeError(
                    f"[MODE_GATE] live mode requires REAL server, got {svr}")

    def _live_sell(self, code, qty, rec, reason) -> Dict:
        self._check_broker_gate()
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
            exec_qty = result["exec_qty"]
            req_qty = result.get("requested_qty", actual_qty)
            self.tracker.mark_filled(rec.order_id, result["exec_price"], exec_qty)
            self.tracker.record_fill(
                result["order_no"], "SELL", code,
                exec_qty, result["exec_price"],
                exec_qty, "CHEJAN")
            self.trade_logger.log_trade(
                code, "SELL", exec_qty, result["exec_price"], mode="LIVE")
            logger.info(
                f"[EXEC] SELL {code} requested_qty={req_qty} exec_qty={exec_qty} "
                f"price={result['exec_price']:,.0f} order_no={result.get('order_no','')}")

        return result

    def _live_buy(self, code, qty, rec, reason) -> Dict:
        self._check_broker_gate()
        self.tracker.mark_submitted(rec.order_id)
        time.sleep(ORDER_INTERVAL)

        result = self.provider.send_order(code, "BUY", qty, 0, "03")

        if result.get("error"):
            self.tracker.mark_rejected(rec.order_id, result["error"])
            logger.error(f"BUY FAILED {code}: {result['error']}")
        else:
            exec_qty = result["exec_qty"]
            req_qty = result.get("requested_qty", qty)

            # Slippage check
            decision_price = self.get_live_price(code)
            if decision_price > 0 and result["exec_price"] > 0:
                slip = abs(result["exec_price"] - decision_price) / decision_price
                if slip > MAX_SLIPPAGE_CAP:
                    logger.warning(
                        f"HIGH SLIPPAGE {code}: {slip:.1%} "
                        f"(decision={decision_price:,.0f}, fill={result['exec_price']:,.0f})")

            self.tracker.mark_filled(rec.order_id, result["exec_price"], exec_qty)
            self.tracker.record_fill(
                result["order_no"], "BUY", code,
                exec_qty, result["exec_price"],
                exec_qty, "CHEJAN")
            self.trade_logger.log_trade(
                code, "BUY", exec_qty, result["exec_price"], mode="LIVE")
            logger.info(
                f"[EXEC] BUY {code} requested_qty={req_qty} exec_qty={exec_qty} "
                f"price={result['exec_price']:,.0f} order_no={result.get('order_no','')}")

        return result

    # ── Ghost Fill Sync ───────────────────────────────────────────────

    def set_ghost_fill_context(self, portfolio, state_mgr,
                               buy_cost: float = 0.00115) -> None:
        """Set portfolio + state_mgr for ghost fill sync.
        Call after portfolio is initialized in run_live().
        """
        self._portfolio = portfolio
        self._state_mgr = state_mgr
        self._buy_cost = buy_cost

    def on_ghost_fill(self, info: dict) -> None:
        """Handle delayed fill after timeout — sync tracker + portfolio + state.

        Called from kiwoom_provider ghost_fill_callback.
        """
        if self._ghost_fill_lock:
            logger.warning("[GHOST_FILL] Reentrant call blocked")
            return
        self._ghost_fill_lock = True

        try:
            code = info["code"]
            side = info["side"]
            exec_qty = info["exec_qty"]
            exec_price = info["exec_price"]
            order_no = info["order_no"]

            delta_qty = exec_qty
            if delta_qty <= 0:
                logger.warning(f"[GHOST_FILL] {code} delta_qty={delta_qty}, skip")
                return

            # Step 1: Tracker (dedup via record_fill)
            is_new = self.tracker.record_fill(
                order_no, side, code, delta_qty, exec_price, delta_qty, "GHOST")
            if not is_new:
                logger.info(f"[GHOST_FILL] {side} {code} duplicate in tracker, skip")
                return

            # Step 2: Trade log
            self.trade_logger.log_trade(
                code, side, delta_qty, exec_price, mode="GHOST")

            # Step 3: Portfolio sync
            if self._portfolio is None:
                logger.warning(f"[GHOST_FILL] No portfolio context — tracker only")
                return

            if side == "SELL":
                if code in self._portfolio.positions:
                    trade = self._portfolio.remove_position(
                        code, exec_price, qty=delta_qty)
                    remaining = 0
                    if code in self._portfolio.positions:
                        remaining = self._portfolio.positions[code].quantity
                    if trade:
                        logger.info(
                            f"[GHOST_FILL_APPLIED] SELL {code} qty={delta_qty} "
                            f"price={exec_price:,.0f} remaining={remaining}")
                    else:
                        logger.warning(
                            f"[GHOST_FILL] SELL {code} remove_position returned None")
                else:
                    logger.warning(
                        f"[GHOST_FILL] SELL {code} — position not in portfolio! "
                        f"Broker sold but engine has no position. "
                        f"Reconciliation needed.")

            elif side == "BUY":
                # Unified rule: avg_price = pure execution price (no fee).
                # Fee is applied to cash only, once.
                gross_cost = delta_qty * exec_price
                cash_cost = gross_cost * (1 + self._buy_cost)  # fee included

                if code in self._portfolio.positions:
                    pos = self._portfolio.positions[code]
                    old_qty = pos.quantity
                    old_gross = old_qty * pos.avg_price  # gross basis (no fee)
                    total_qty = old_qty + delta_qty
                    if total_qty > 0:
                        pos.avg_price = (old_gross + gross_cost) / total_qty
                    pos.quantity = total_qty
                    pos.current_price = exec_price
                    self._portfolio.cash -= cash_cost
                    logger.info(
                        f"[GHOST_FILL_APPLIED] BUY {code} +{delta_qty} "
                        f"(total={total_qty}) price={exec_price:,.0f} "
                        f"avg={pos.avg_price:,.0f} cash_deducted={cash_cost:,.0f}")
                else:
                    # New position: add_position handles cash deduction internally
                    success = self._portfolio.add_position(
                        code, delta_qty, exec_price,
                        entry_date=str(date.today()),
                        buy_cost=self._buy_cost)
                    if success:
                        logger.info(
                            f"[GHOST_FILL_APPLIED] BUY {code} qty={delta_qty} "
                            f"price={exec_price:,.0f} (new position)")
                    else:
                        logger.error(
                            f"[GHOST_FILL] BUY {code} add_position failed")

            # Step 4: State save (with retry)
            if self._state_mgr:
                saved = False
                for _attempt in range(3):
                    saved = self._state_mgr.save_portfolio(self._portfolio.to_dict())
                    if saved:
                        break
                    logger.warning(f"[GHOST_STATE_SAVE_RETRY] {side} {code} "
                                   f"attempt {_attempt+1}/3")
                    time.sleep(0.5)
                if saved:
                    logger.info(f"[GHOST_STATE_SAVED] {side} {code} qty={delta_qty}")
                else:
                    logger.error(f"[GHOST_STATE_SAVE_FAIL] {side} {code} — "
                                 f"3 attempts exhausted!")

            logger.info(f"[GHOST_PORTFOLIO_SYNCED] {side} {code} "
                        f"delta={delta_qty} order_no={order_no}")

        except Exception as e:
            logger.error(f"[GHOST_FILL_ERROR] {e}", exc_info=True)
        finally:
            self._ghost_fill_lock = False
