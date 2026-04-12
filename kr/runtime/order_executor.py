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
from datetime import date, datetime
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

        # base_qty: broker holdings BEFORE this sell order.
        # Used by EOD settle to compute delta:
        #   BUY:  delta = broker_qty_after - base_qty  (increase = filled)
        #   SELL: delta = base_qty - broker_qty_after   (decrease = sold)
        # Both sides use the same formula direction: base_qty is the
        # pre-order snapshot, broker_qty is the post-settle observation.
        rec.base_qty = broker_hold

        actual_qty = min(qty, broker_hold)
        if actual_qty < qty:
            logger.warning(f"Sell qty adjusted: {qty} -> {actual_qty} (broker_hold={broker_hold})")

        self.tracker.mark_submitted(rec.order_id)
        time.sleep(ORDER_INTERVAL)

        result = self.provider.send_order(code, "SELL", actual_qty, 0, "03")

        if result.get("error"):
            error_str = result["error"]
            if "TIMEOUT_UNCERTAIN" in error_str:
                self.tracker.mark_pending_external(rec.order_id)
                logger.warning(
                    f"[SELL_PENDING_EXTERNAL] {code} qty={actual_qty} — "
                    f"timeout, awaiting ghost/reconcile "
                    f"(order_no={result.get('order_no', '')})")
                self._persist_pending_external(rec)
            else:
                self.tracker.mark_rejected(rec.order_id, error_str)
                logger.error(f"SELL FAILED {code}: {error_str}")
        else:
            exec_qty = result["exec_qty"]
            req_qty = result.get("requested_qty", actual_qty)

            # C3 FIX: PARTIAL_TIMEOUT with 0 fills — KRX accepted but
            # no chejan arrived before timeout.  Treat as PENDING_EXTERNAL
            # instead of FILLED(0) to let ghost/reconcile catch the fill.
            if exec_qty == 0 and req_qty > 0:
                self.tracker.mark_pending_external(rec.order_id)
                logger.warning(
                    f"[SELL_PARTIAL_TIMEOUT_ZERO] {code} req={req_qty} exec=0 — "
                    f"KRX may have filled, routing to PENDING_EXTERNAL "
                    f"(order_no={result.get('order_no', '')})")
                self._persist_pending_external(rec)
            elif 0 < exec_qty < req_qty:
                # Partial fill: record what we got, remainder to PENDING_EXTERNAL
                self.tracker.mark_filled(rec.order_id, result["exec_price"], exec_qty)
                self.tracker.record_fill(
                    result["order_no"], "SELL", code,
                    exec_qty, result["exec_price"],
                    exec_qty, "CHEJAN")
                self.trade_logger.log_trade(
                    code, "SELL", exec_qty, result["exec_price"], mode="LIVE")
                logger.warning(
                    f"[SELL_PARTIAL_TIMEOUT] {code} req={req_qty} exec={exec_qty} "
                    f"— partial filled, remainder {req_qty - exec_qty} tracked via ghost "
                    f"(order_no={result.get('order_no', '')})")
            else:
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

        # base_qty: 주문 직전 보유 수량 (broker 기준 보정)
        portfolio_qty = 0
        if self._portfolio:
            pos = self._portfolio.positions.get(code)
            portfolio_qty = pos.quantity if pos else 0
        base_qty = portfolio_qty
        base_source = "portfolio"
        try:
            snap = self.provider.query_account_summary()
            broker_hold = {h["code"]: h.get("qty", h.get("quantity", 0))
                           for h in snap.get("holdings", [])}
            broker_base = broker_hold.get(code, 0)
            if broker_base != portfolio_qty:
                _diff = abs(broker_base - portfolio_qty)
                _level = "CRITICAL" if _diff > max(1, portfolio_qty * 0.5) else "WARNING"
                getattr(logger, _level.lower() if _level != "CRITICAL" else "critical")(
                    f"[BASE_QTY_CORRECTED] {code}: portfolio={portfolio_qty} "
                    f"broker={broker_base} diff={_diff} — using broker")
            base_qty = broker_base  # always prefer broker
            base_source = "broker"
        except Exception:
            logger.warning(f"[BASE_QTY_FALLBACK] {code}: broker query failed, "
                           f"using portfolio={portfolio_qty}")
        logger.info(f"[BASE_QTY_SOURCE] {code}: broker={base_qty if base_source == 'broker' else '?'} "
                    f"portfolio={portfolio_qty} chosen={base_qty} source={base_source}")
        rec.base_qty = base_qty

        self.tracker.mark_submitted(rec.order_id)
        time.sleep(ORDER_INTERVAL)

        result = self.provider.send_order(code, "BUY", qty, 0, "03")

        if result.get("error"):
            error_str = result["error"]
            if "TIMEOUT_UNCERTAIN" in error_str:
                self.tracker.mark_pending_external(rec.order_id)
                logger.warning(
                    f"[BUY_PENDING_EXTERNAL] {code} qty={qty} base_qty={base_qty} — "
                    f"timeout, awaiting ghost/reconcile "
                    f"(order_no={result.get('order_no', '')})")
                self._persist_pending_external(rec)
            else:
                self.tracker.mark_rejected(rec.order_id, error_str)
                logger.error(f"BUY FAILED {code}: {error_str}")
        else:
            exec_qty = result["exec_qty"]
            req_qty = result.get("requested_qty", qty)

            # C3 FIX: PARTIAL_TIMEOUT with 0 fills — KRX accepted but
            # no chejan arrived before timeout.  This is the exact pattern
            # from 2026-04-03 LIVE (17 BUY orders).
            if exec_qty == 0 and req_qty > 0:
                self.tracker.mark_pending_external(rec.order_id)
                logger.warning(
                    f"[BUY_PARTIAL_TIMEOUT_ZERO] {code} req={req_qty} exec=0 — "
                    f"KRX may have filled, routing to PENDING_EXTERNAL "
                    f"(order_no={result.get('order_no', '')})")
                self._persist_pending_external(rec)
            elif 0 < exec_qty < req_qty:
                # Slippage check on partial
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
                logger.warning(
                    f"[BUY_PARTIAL_TIMEOUT] {code} req={req_qty} exec={exec_qty} "
                    f"— partial filled, remainder {req_qty - exec_qty} tracked via ghost "
                    f"(order_no={result.get('order_no', '')})")
            else:
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

            # Cumulative qty for dedup (already_recorded + this delta)
            already_recorded = info.get("already_recorded_qty", 0)
            cumulative_qty = already_recorded + delta_qty

            # Step 1: Tracker (dedup via record_fill)
            is_new = self.tracker.record_fill(
                order_no, side, code, delta_qty, exec_price, cumulative_qty, "GHOST")
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
                if code not in self._portfolio.positions:
                    logger.info(f"[GHOST_FILL] SELL {code} — position gone, skip")
                    return
                pos = self._portfolio.positions[code]
                actual_delta = min(delta_qty, pos.quantity)
                if actual_delta <= 0:
                    logger.info(f"[GHOST_FILL] SELL {code} — qty=0, skip")
                    return
                if actual_delta < delta_qty:
                    logger.warning(
                        f"[GHOST_FILL] SELL {code} clamped: "
                        f"{delta_qty} -> {actual_delta} (held={pos.quantity})")
                trade = self._portfolio.remove_position(
                    code, exec_price, qty=actual_delta)
                remaining = 0
                if code in self._portfolio.positions:
                    remaining = self._portfolio.positions[code].quantity
                if trade:
                    logger.info(
                        f"[GHOST_FILL_APPLIED] SELL {code} qty={actual_delta} "
                        f"price={exec_price:,.0f} remaining={remaining}")
                else:
                    logger.warning(
                        f"[GHOST_FILL] SELL {code} remove_position returned None")

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
                    pos.invested_total += cash_cost
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

            is_terminal = info.get("is_terminal", False)
            if is_terminal:
                logger.info(
                    f"[GHOST_FILL_FINALIZED] {side} {code} "
                    f"requested={info.get('requested_qty', '?')} "
                    f"order_no={order_no} — fully settled")
                self._settle_tracker_ghost(
                    order_no, code, side,
                    info.get("requested_qty", 0), exec_price)
                self._try_upgrade_sell_status()
            else:
                logger.info(f"[GHOST_PORTFOLIO_SYNCED] {side} {code} "
                            f"delta={delta_qty} order_no={order_no}")

        except Exception as e:
            logger.error(f"[GHOST_FILL_ERROR] {e}", exc_info=True)
        finally:
            self._ghost_fill_lock = False

    def _try_upgrade_sell_status(self) -> None:
        """Upgrade rebal_sell_status PARTIAL→COMPLETE if all ghosts settled."""
        if not self._state_mgr:
            return
        try:
            rt = self._state_mgr.load_runtime()
            status = rt.get("rebal_sell_status", "")
            if status not in ("PARTIAL", "UNCERTAIN"):
                return  # nothing to upgrade

            # Check: does provider still have unsettled ghosts?
            if self.provider and hasattr(self.provider, '_ghost_orders'):
                unsettled = [g for g in self.provider._ghost_orders
                             if g.get("status") not in ("GHOST_FILLED",)]
                if unsettled:
                    logger.info(f"[SELL_STATUS_CHECK] {len(unsettled)} "
                                f"ghost(s) still unsettled — keeping {status}")
                    return

            # All ghosts settled → upgrade
            rt["rebal_sell_status"] = "COMPLETE"
            rt["fast_reentry_dirty"] = True
            self._state_mgr.save_runtime(rt)
            logger.info(f"[SELL_STATUS_UPGRADED] {status} -> COMPLETE "
                        f"(all ghost orders settled)")
        except Exception as e:
            logger.warning(f"[SELL_STATUS_UPGRADE_ERROR] {e}")

    def _settle_tracker_ghost(self, order_no: str, code: str, side: str,
                              total_qty: int, avg_price: float) -> None:
        """Find PENDING_EXTERNAL OrderRecord and settle via ghost fill."""
        from runtime.order_tracker import OrderStatus
        for rec in self.tracker._orders.values():
            if (rec.code == code and rec.side == side and
                rec.status in (OrderStatus.PENDING_EXTERNAL,
                               OrderStatus.TIMEOUT_UNCERTAIN,
                               OrderStatus.SUBMITTED)):
                self.tracker.mark_ghost_settled(rec.order_id, total_qty, avg_price)
                logger.info(f"[TRACKER_GHOST_SETTLED] {rec.order_id} "
                            f"cum={total_qty}/{rec.quantity} order_no={order_no}")
                return
        logger.debug(f"[TRACKER_GHOST_SETTLE] no matching record for "
                     f"order_no={order_no} {side} {code}")

    def _persist_pending_external(self, rec) -> None:
        """Save PENDING_EXTERNAL order info for reconcile after restart."""
        if not self._state_mgr:
            return
        try:
            existing = self._state_mgr.load_pending_external()
            entry = {
                "order_id": rec.order_id,
                "code": rec.code,
                "side": rec.side,
                "requested_qty": rec.quantity,
                "exec_qty": rec.exec_qty,
                "exec_price": rec.exec_price,
                "base_qty": getattr(rec, "base_qty", 0),
                "timestamp": datetime.now().isoformat(),
            }
            existing.append(entry)
            self._state_mgr.save_pending_external(existing)
            logger.info(f"[PENDING_EXTERNAL_SAVED] {rec.side} {rec.code} "
                        f"qty={rec.quantity} base_qty={entry['base_qty']}")
        except Exception as e:
            logger.warning(f"[PENDING_EXTERNAL_SAVE_ERROR] {e}")
