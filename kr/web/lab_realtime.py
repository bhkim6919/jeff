# -*- coding: utf-8 -*-
"""
lab_realtime.py -- Real-time Lab Simulator (WebSocket price tracking)
=====================================================================
Virtual portfolio simulation with 3 strategies using live WebSocket
price ticks for TP/SL triggering. NO real orders -- virtual only.

Uses KiwoomWebSocket Type 0B for real-time price data.
Thread-safe: WebSocket callback runs in WS thread, state reads from main.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from web.lab_simulator import (
    DEFAULT_PARAMS, INITIAL_CASH, VirtualTrade, VirtualPosition,
    _save_result,
)

logger = logging.getLogger("gen4.rest.lab.realtime")

# Market close time (KST)
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30


# ── Strategy Position (extends VirtualPosition with TP/SL) ──────

@dataclass
class RealtimePosition:
    code: str
    name: str
    strategy: str           # "A", "B", "C"
    entry_price: int
    qty: int
    current_price: int = 0
    high_price: int = 0     # for trailing stop (strategy C)
    tp_price: int = 0       # take-profit trigger price
    sl_price: int = 0       # stop-loss trigger price
    tp_pct: float = 0.0     # TP percent from entry
    sl_pct: float = 0.0     # SL percent from entry
    entry_time: str = ""
    exit_time: str = ""
    exit_reason: str = ""   # "TP", "SL", "TRAIL", "CLOSE", ""
    pnl: float = 0.0
    pnl_pct: float = 0.0
    closed: bool = False


@dataclass
class StrategyState:
    name: str               # "A", "B", "C"
    label: str
    positions: List[RealtimePosition] = field(default_factory=list)
    closed_trades: List[Dict] = field(default_factory=list)
    cash: float = INITIAL_CASH
    total_pnl: float = 0.0
    win_count: int = 0
    loss_count: int = 0


# ── Realtime Simulator ──────────────────────────────────────────

class RealtimeSimulator:
    """Manages a real-time simulation session with WebSocket price tracking."""

    def __init__(self, provider, params: Optional[Dict] = None):
        self.provider = provider
        self.params: Dict[str, Any] = dict(DEFAULT_PARAMS)
        if params:
            self.params.update(params)

        self.running = False
        self.strategies: List[StrategyState] = []
        self.price_cache: Dict[str, int] = {}    # {code: last_price}
        self._lock = threading.Lock()
        self._start_time: float = 0.0
        self._stop_time: float = 0.0
        self._ranking: List[Dict] = []
        self._subscribed_codes: List[str] = []
        self._listener_key = f"lab_rt:{id(self)}"
        self._tick_count = 0
        self._events: List[Dict] = []            # recent events for UI

    def start(self, ranking: List[Dict]) -> Dict:
        """
        Start real-time simulation.

        1. Enter virtual positions based on strategy rules
        2. Subscribe to WebSocket Type 0B for all position codes
        3. On each price tick, check TP/SL
        """
        if self.running:
            return {"error": "Simulator already running"}

        self._ranking = ranking
        self._start_time = time.time()
        # ISO wall-clock for backdata (Jeff 2026-04-29 — start/stop time
        # capture for downstream live-trading expansion analysis).
        self._start_dt = datetime.now()
        self._stop_dt = None
        self._tick_count = 0
        self._events = []
        self.price_cache.clear()

        # Build strategy states
        self.strategies = [
            self._build_strategy_a(ranking),
            self._build_strategy_b(ranking),
            self._build_strategy_c(ranking),
        ]

        # Collect all codes that have positions
        codes_set = set()
        for strat in self.strategies:
            for pos in strat.positions:
                codes_set.add(pos.code)
                self.price_cache[pos.code] = pos.entry_price

        if not codes_set:
            return {"error": "No positions entered (check ranking/params)"}

        self._subscribed_codes = sorted(codes_set)

        # Subscribe to WebSocket via event bus (no legacy callback overwrite)
        try:
            ws = self.provider._ensure_ws()
            ws.add_price_listener(self._on_price_tick, key=self._listener_key)
            ws.subscribe(self._subscribed_codes, "0B",
                         owner_key=self._listener_key)
            logger.info(
                f"[LAB_RT] Subscribed to {len(self._subscribed_codes)} codes "
                f"(key={self._listener_key})"
            )
        except Exception as e:
            # Rollback listener on subscribe failure
            try:
                ws.remove_price_listener(self._listener_key)
            except Exception:
                pass
            logger.error(f"[LAB_RT] WebSocket subscribe failed: {e}")
            return {"error": f"WebSocket subscribe failed: {e}"}

        self.running = True
        self._add_event("SIM_START", f"Started with {len(self._subscribed_codes)} codes")

        # Log entry summary
        for strat in self.strategies:
            entry_count = len(strat.positions)
            self._add_event(
                "ENTRY",
                f"Strategy {strat.name}: {entry_count} positions entered"
            )

        return {"ok": True, "codes": self._subscribed_codes}

    def stop(self) -> Dict:
        """Stop simulation, close all open positions at current price."""
        if not self.running:
            return {"error": "Simulator not running"}

        self.running = False
        self._stop_time = time.time()
        self._stop_dt = datetime.now()

        # Close all open positions at current price
        with self._lock:
            for strat in self.strategies:
                for pos in strat.positions:
                    if not pos.closed:
                        self._close_position(strat, pos, "CLOSE")

        # Detach listener + owned subscriptions only (shared WS stays alive)
        try:
            if hasattr(self.provider, '_ws') and self.provider._ws:
                ws = self.provider._ws
                ws.remove_price_listener(self._listener_key)
                if self._subscribed_codes:
                    ws.unsubscribe(self._subscribed_codes, "0B",
                                   owner_key=self._listener_key)
        except Exception as e:
            logger.warning(f"[LAB_RT] WebSocket cleanup: {e}")

        self._add_event("SIM_STOP", "Simulation stopped")

        # Build and save result
        result = self._build_result()
        try:
            _save_result(result)
        except Exception as e:
            logger.warning(f"[LAB_RT] Save result failed: {e}")

        return {"ok": True, "result": result}

    def _on_price_tick(self, code: str, values: dict) -> None:
        """
        Called from WebSocket thread on each Type 0B price tick.
        Parse price, update cache, check TP/SL for all strategies.
        """
        if not self.running:
            return

        try:
            price_raw = values.get("10", "0")
            price = abs(int(price_raw.replace("+", "").replace("-", "") or "0"))
            if price <= 0:
                return
        except (ValueError, TypeError):
            return

        self._tick_count += 1

        with self._lock:
            self.price_cache[code] = price

            for strat in self.strategies:
                for pos in strat.positions:
                    if pos.closed or pos.code != code:
                        continue

                    pos.current_price = price

                    # Update high watermark for trailing stop (Strategy C)
                    if price > pos.high_price:
                        pos.high_price = price
                        # Recalculate trailing stop for C
                        if strat.name == "C":
                            self._update_trail_stop(pos)

                    # Check TP
                    if pos.tp_price > 0 and price >= pos.tp_price:
                        self._close_position(strat, pos, "TP")
                        self._add_event(
                            "TP",
                            f"[{strat.name}] {pos.name} TP hit @ {price:,}"
                        )
                        continue

                    # Check SL
                    if pos.sl_price > 0 and price <= pos.sl_price:
                        self._close_position(strat, pos, "SL")
                        self._add_event(
                            "SL",
                            f"[{strat.name}] {pos.name} SL hit @ {price:,}"
                        )
                        continue

                    # Check trailing stop for C
                    if strat.name == "C" and pos.high_price > pos.entry_price:
                        trail_pct = self.params.get("trail_max_c", 6.0) / 100
                        trail_stop = int(pos.high_price * (1 - trail_pct))
                        if price <= trail_stop:
                            self._close_position(strat, pos, "TRAIL")
                            self._add_event(
                                "TRAIL",
                                f"[C] {pos.name} TRAIL hit @ {price:,} "
                                f"(high={pos.high_price:,})"
                            )

        # Auto-stop after market close
        now = datetime.now()
        if now.hour >= MARKET_CLOSE_HOUR and now.minute >= MARKET_CLOSE_MINUTE:
            if self.running:
                logger.info("[LAB_RT] Market closed, auto-stopping")
                self.stop()

        # (event bus handles dispatch — no legacy callback forwarding needed)

    def _close_position(
        self, strat: StrategyState, pos: RealtimePosition, reason: str
    ) -> None:
        """Close a position and record the trade. Caller must hold _lock."""
        if pos.closed:
            return

        exit_price = pos.current_price if pos.current_price > 0 else pos.entry_price
        pos.closed = True
        pos.exit_reason = reason
        pos.exit_time = datetime.now().strftime("%H:%M:%S")
        pos.pnl = (exit_price - pos.entry_price) * pos.qty
        pos.pnl_pct = round(
            (exit_price - pos.entry_price) / pos.entry_price * 100, 2
        ) if pos.entry_price > 0 else 0.0

        strat.cash += exit_price * pos.qty
        strat.total_pnl += pos.pnl

        if pos.pnl > 0:
            strat.win_count += 1
        else:
            strat.loss_count += 1

        strat.closed_trades.append({
            "code": pos.code,
            "name": pos.name,
            "strategy": pos.strategy,
            "side": "SELL",
            "entry_price": pos.entry_price,
            "price": exit_price,
            "qty": pos.qty,
            "pnl": round(pos.pnl),
            "pnl_pct": pos.pnl_pct,
            "reason": reason,
            "timestamp": pos.exit_time,
        })

        logger.info(
            f"[LAB_RT] CLOSE [{strat.name}] {pos.name} {reason} "
            f"entry={pos.entry_price} exit={exit_price} "
            f"pnl={pos.pnl:+,.0f} ({pos.pnl_pct:+.2f}%)"
        )

    def _update_trail_stop(self, pos: RealtimePosition) -> None:
        """Update trailing stop price for Strategy C. Caller must hold _lock."""
        initial_tp_pct = self.params.get("exit_target_c", 1.5) / 100
        trail_max_pct = self.params.get("trail_max_c", 6.0) / 100

        gain_from_entry = (pos.high_price - pos.entry_price) / pos.entry_price
        trail_pct = min(initial_tp_pct + gain_from_entry * 0.5, trail_max_pct)
        pos.sl_price = int(pos.high_price * (1 - trail_pct))

    def get_state(self) -> Dict:
        """Return current simulation state for SSE / API response."""
        with self._lock:
            elapsed = time.time() - self._start_time if self._start_time else 0

            strat_states = []
            for strat in self.strategies:
                open_positions = []
                for pos in strat.positions:
                    if not pos.closed:
                        cur = pos.current_price or pos.entry_price
                        unrealized_pnl = (cur - pos.entry_price) * pos.qty
                        unrealized_pct = round(
                            (cur - pos.entry_price) / pos.entry_price * 100, 2
                        ) if pos.entry_price > 0 else 0.0
                        open_positions.append({
                            "code": pos.code,
                            "name": pos.name,
                            "entry_price": pos.entry_price,
                            "current_price": cur,
                            "high_price": pos.high_price,
                            "qty": pos.qty,
                            "tp_price": pos.tp_price,
                            "sl_price": pos.sl_price,
                            "unrealized_pnl": round(unrealized_pnl),
                            "unrealized_pnl_pct": unrealized_pct,
                        })

                position_value = sum(
                    (p.current_price or p.entry_price) * p.qty
                    for p in strat.positions if not p.closed
                )
                total_trades = strat.win_count + strat.loss_count
                win_rate = round(
                    strat.win_count / max(total_trades, 1) * 100, 1
                )
                unrealized_total = sum(
                    (p.current_price or p.entry_price) * p.qty - p.entry_price * p.qty
                    for p in strat.positions if not p.closed
                )

                strat_states.append({
                    "name": strat.name,
                    "label": strat.label,
                    "open_positions": open_positions,
                    "closed_trades": strat.closed_trades,
                    "total_pnl": round(strat.total_pnl + unrealized_total),
                    "realized_pnl": round(strat.total_pnl),
                    "unrealized_pnl": round(unrealized_total),
                    "win_count": strat.win_count,
                    "loss_count": strat.loss_count,
                    "win_rate": win_rate,
                    "cash": round(strat.cash),
                    "total_value": round(strat.cash + position_value),
                    "open_count": len(open_positions),
                    "closed_count": total_trades,
                })

            return {
                "running": self.running,
                "strategies": strat_states,
                "price_cache": dict(self.price_cache),
                "elapsed_sec": round(elapsed, 1),
                "tick_count": self._tick_count,
                "subscribed_codes": self._subscribed_codes,
                "events": self._events[-20:],  # last 20 events
                "initial_cash": INITIAL_CASH,
            }

    def _add_event(self, event_type: str, message: str) -> None:
        """Add an event to the event log (thread-safe via _lock if needed)."""
        self._events.append({
            "type": event_type,
            "message": message,
            "time": datetime.now().strftime("%H:%M:%S"),
        })
        # Keep last 100 events
        if len(self._events) > 100:
            self._events = self._events[-100:]

    def _build_result(self) -> Dict:
        """Build result dict compatible with _save_result format."""
        strategies_out = []
        for strat in self.strategies:
            all_trades = []
            # Entry trades
            for pos in strat.positions:
                all_trades.append({
                    "code": pos.code,
                    "name": pos.name,
                    "strategy": strat.name,
                    "side": "BUY",
                    "price": pos.entry_price,
                    "qty": pos.qty,
                    "pnl": 0,
                    "pnl_pct": 0,
                    "reason": "",
                    "timestamp": pos.entry_time,
                })
            # Exit trades
            all_trades.extend(strat.closed_trades)

            total_trades = strat.win_count + strat.loss_count
            position_value = sum(
                (p.current_price or p.entry_price) * p.qty
                for p in strat.positions if not p.closed
            )

            strategies_out.append({
                "name": strat.name,
                "label": strat.label,
                "trades": all_trades,
                "positions": [],
                "total_pnl": round(strat.total_pnl),
                "win_count": strat.win_count,
                "loss_count": strat.loss_count,
                "win_rate": round(
                    strat.win_count / max(total_trades, 1) * 100, 1
                ),
                "cash": round(strat.cash),
                "total_value": round(strat.cash + position_value),
            })

        # Wall-clock start/stop (Jeff 2026-04-29 — backdata for live
        # trading expansion). Falls back to current time if stop()
        # wasn't called (defensive — _build_result is also reachable
        # mid-flight via /api/lab/realtime/state).
        _started_at = (
            self._start_dt.strftime("%Y-%m-%d %H:%M:%S")
            if self._start_dt is not None else ""
        )
        _stopped_at = (
            self._stop_dt.strftime("%Y-%m-%d %H:%M:%S")
            if self._stop_dt is not None
            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        return {
            "timestamp": _stopped_at,  # legacy alias for stopped_at
            "started_at": _started_at,
            "stopped_at": _stopped_at,
            "initial_cash": INITIAL_CASH,
            "ranking_count": len(self._ranking),
            "params": self.params,
            "mode": "realtime",
            "elapsed_sec": round(time.time() - self._start_time, 1),
            "tick_count": self._tick_count,
            "strategies": strategies_out,
        }

    # ── Strategy Builders ────────────────────────────────────────

    def _build_strategy_a(self, ranking: List[Dict]) -> StrategyState:
        """Strategy A: Conservative -- TP/SL, 1-day max hold."""
        strat = StrategyState(name="A", label="Conservative")
        tp_pct = self.params.get("exit_target_a", 1.0) / 100
        sl_pct = self.params.get("stop_loss_a", -0.5) / 100
        max_pos = self.params.get("max_positions", 5)
        size_pct = self.params.get("position_size_pct", 20.0) / 100
        price_min = self.params.get("price_min", 5000)
        entry_thresh = self.params.get("entry_threshold", 3.0)

        eligible = [
            s for s in ranking
            if s["change_pct"] >= entry_thresh
            and s["price"] >= price_min
            and s.get("code", "")
        ]

        for stock in eligible[:max_pos]:
            alloc = INITIAL_CASH * size_pct
            if strat.cash < alloc * 0.5:
                break
            buy_amount = min(alloc, strat.cash)
            qty = int(buy_amount / stock["price"])
            if qty <= 0:
                continue

            cost = qty * stock["price"]
            strat.cash -= cost

            tp_price = int(stock["price"] * (1 + tp_pct))
            sl_price = int(stock["price"] * (1 + sl_pct))

            strat.positions.append(RealtimePosition(
                code=stock["code"],
                name=stock["name"],
                strategy="A",
                entry_price=stock["price"],
                qty=qty,
                current_price=stock["price"],
                high_price=stock["price"],
                tp_price=tp_price,
                sl_price=sl_price,
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                entry_time=datetime.now().strftime("%H:%M:%S"),
            ))

        return strat

    def _build_strategy_b(self, ranking: List[Dict]) -> StrategyState:
        """Strategy B: Aggressive -- wider TP/SL, more positions."""
        strat = StrategyState(name="B", label="Aggressive")
        tp_pct = self.params.get("exit_target_b", 2.0) / 100
        sl_pct = self.params.get("stop_loss_b", -1.0) / 100
        max_pos = min(self.params.get("max_positions", 5) * 2, 20)
        size_pct = self.params.get("position_size_pct", 20.0) / 100 * 0.5
        price_min = self.params.get("price_min", 5000)
        entry_thresh = self.params.get("entry_threshold", 3.0) * 0.7

        eligible = [
            s for s in ranking
            if s["change_pct"] >= entry_thresh
            and s["price"] >= price_min
            and s.get("code", "")
        ]

        for stock in eligible[:max_pos]:
            alloc = INITIAL_CASH * size_pct
            if strat.cash < alloc * 0.3:
                break
            buy_amount = min(alloc, strat.cash)
            qty = int(buy_amount / stock["price"])
            if qty <= 0:
                continue

            cost = qty * stock["price"]
            strat.cash -= cost

            tp_price = int(stock["price"] * (1 + tp_pct))
            sl_price = int(stock["price"] * (1 + sl_pct))

            strat.positions.append(RealtimePosition(
                code=stock["code"],
                name=stock["name"],
                strategy="B",
                entry_price=stock["price"],
                qty=qty,
                current_price=stock["price"],
                high_price=stock["price"],
                tp_price=tp_price,
                sl_price=sl_price,
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                entry_time=datetime.now().strftime("%H:%M:%S"),
            ))

        return strat

    def _build_strategy_c(self, ranking: List[Dict]) -> StrategyState:
        """Strategy C: Dynamic -- trailing stop that widens."""
        strat = StrategyState(name="C", label="Dynamic")
        initial_tp_pct = self.params.get("exit_target_c", 1.5) / 100
        trail_max_pct = self.params.get("trail_max_c", 6.0) / 100
        max_pos = self.params.get("max_positions", 5)
        size_pct = self.params.get("position_size_pct", 20.0) / 100
        price_min = self.params.get("price_min", 5000)
        entry_thresh = self.params.get("entry_threshold", 3.0)

        eligible = [
            s for s in ranking
            if s["change_pct"] >= entry_thresh
            and s["price"] >= price_min
            and s.get("code", "")
        ]

        for stock in eligible[:max_pos]:
            alloc = INITIAL_CASH * size_pct
            if strat.cash < alloc * 0.5:
                break
            buy_amount = min(alloc, strat.cash)
            qty = int(buy_amount / stock["price"])
            if qty <= 0:
                continue

            cost = qty * stock["price"]
            strat.cash -= cost

            tp_price = int(stock["price"] * (1 + initial_tp_pct))
            # Initial SL = trailing from entry (no gain yet)
            sl_price = int(stock["price"] * (1 - initial_tp_pct))

            strat.positions.append(RealtimePosition(
                code=stock["code"],
                name=stock["name"],
                strategy="C",
                entry_price=stock["price"],
                qty=qty,
                current_price=stock["price"],
                high_price=stock["price"],
                tp_price=tp_price,
                sl_price=sl_price,
                tp_pct=initial_tp_pct,
                sl_pct=-initial_tp_pct,
                entry_time=datetime.now().strftime("%H:%M:%S"),
            ))

        return strat
