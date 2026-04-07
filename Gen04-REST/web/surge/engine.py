# -*- coding: utf-8 -*-
"""
engine.py -- Surge Simulator Engine (단일 Lock 소유자)
========================================================
모든 상태 변경은 self._lock 안에서만 발생.
price_cache write, snapshot freeze, entry/exit 판단이 동일 lock 범위.
_evaluate_entry / _evaluate_exit는 전달받은 snap만 사용 (price_cache 재조회 금지).
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from web.surge.config import SurgeConfig
from web.surge.state_machine import StateTracker, StockState
from web.surge.sim_logger import SurgeLogger, SurgeLogTag
from web.surge.signal_rules import (
    check_entry, check_tp, check_sl, check_time_exit, check_force_exit,
    calc_buy_fill_price, calc_sell_fill_price, calc_pnl_pct, get_time_slippage,
)
from web.surge.risk_guard import RiskGuard
from web.surge.order_simulator import simulate_buy, simulate_sell
from web.surge.scanner import SurgeScanner, SurgeCandidate, filter_candidates
from web.surge.metrics import (
    TradeRecord, compute_summary,
    get_output_dir, save_trades_csv, save_summary_json, save_state_json,
)

logger = logging.getLogger("gen4.rest.surge")


@dataclass
class SurgePosition:
    code: str
    name: str
    entry_fill_price: int       # 체결가 (ask + slippage)
    raw_ask_at_entry: int       # 진입 시 raw ask
    qty: int
    entry_ts: float             # time.time()
    entry_time_str: str         # "2026-04-07 09:15:23"
    entry_slippage_pct: float
    entry_fee: float
    # Live tracking
    current_price: int = 0
    current_bid: int = 0
    current_ask: int = 0


class SurgeSimulator:
    """
    Main orchestrator. Owns the single _lock.
    Sub-components (StateTracker, RiskGuard) have NO internal locks.
    """

    def __init__(self, provider: Any, config: SurgeConfig):
        self.provider = provider
        self.config = config
        self.running = False

        # Single lock — ALL state mutations go through here
        self._lock = threading.Lock()

        # Sub-components (lock-free, engine provides lock)
        self._state = StateTracker()
        self._risk = RiskGuard()
        self._scanner = SurgeScanner()
        self._logger = SurgeLogger()

        # Runtime state
        self._positions: Dict[str, SurgePosition] = {}
        self._trades: List[TradeRecord] = []
        self._trade_counter: int = 0
        self._price_cache: Dict[str, dict] = {}
        self._candidate_queue: deque = deque(maxlen=200)
        self._candidates: Dict[str, SurgeCandidate] = {}  # code → candidate
        self._seen_codes: set = set()

        # WebSocket
        self._original_price_cb = None
        self._subscribed_codes: List[str] = []

        # Scan timer
        self._scan_timer: Optional[threading.Timer] = None
        self._scan_running = False

        # SSE state hash
        self._state_hash: str = ""

        # Timing
        self._start_time: float = 0
        self._tick_count: int = 0

        # Cash tracking
        self._cash: float = config.initial_cash

    # ── Public API ────────────────────────────────────────

    def start(self) -> dict:
        """Start surge simulator: initial scan + WS subscribe + periodic scan."""
        if self.running:
            return {"error": "Simulator already running"}

        self._start_time = time.time()
        self._tick_count = 0

        # Setup output
        out_dir = get_output_dir()
        self._logger.set_csv_path(out_dir / "decisions_sim.csv")

        # Initial scan
        try:
            candidates = self._scanner.scan(self.provider, self.config)
            filtered = filter_candidates(candidates, self.config)
        except Exception as e:
            logger.error(f"[SURGE] Initial scan failed: {e}")
            filtered = []

        if not filtered:
            return {"error": "No candidates found in initial scan"}

        # Register candidates
        with self._lock:
            for c in filtered:
                if c.code and c.code not in self._seen_codes:
                    self._seen_codes.add(c.code)
                    self._candidates[c.code] = c
                    self._state.transition(c.code, StockState.SCANNED, "TR_RECEIVED")
                    self._state.transition(c.code, StockState.WATCHING, "FILTER_PASS")
                    self._logger.log(
                        SurgeLogTag.SURGE_CANDIDATE,
                        code=c.code, name=c.name,
                        last_price=c.price,
                        trigger_reason=f"rank={c.rank} chg={c.change_pct}%",
                    )

        codes = [c.code for c in filtered if c.code]
        if not codes:
            return {"error": "No valid codes after filtering"}

        # Subscribe WebSocket
        try:
            ws = self.provider._ensure_ws()
            self._original_price_cb = ws._on_price_tick
            ws.set_on_price_tick(self._on_price_tick)
            ws.subscribe(codes, "0B")
            self._subscribed_codes = codes
            logger.info(f"[SURGE] Subscribed to {len(codes)} codes")
        except Exception as e:
            logger.error(f"[SURGE] WebSocket subscribe failed: {e}")
            return {"error": f"WebSocket subscribe failed: {e}"}

        # Start periodic scan
        self._scan_running = True
        self._schedule_scan()

        self.running = True
        self._logger.log(SurgeLogTag.SURGE_TR_RECEIVED,
                         trigger_reason=f"START {len(codes)} codes")

        return {"ok": True, "codes": codes, "candidates": len(filtered)}

    def stop(self) -> dict:
        """Stop simulator: force-close positions, unsubscribe, save results."""
        if not self.running:
            return {"error": "Simulator not running"}

        self.running = False
        self._scan_running = False
        if self._scan_timer:
            self._scan_timer.cancel()
            self._scan_timer = None

        # Force-close all open positions
        now = time.time()
        with self._lock:
            for code in list(self._positions.keys()):
                snap = dict(self._price_cache.get(code, {}))
                self._close_position(code, snap, "FORCE_EXIT", now)

        # Unsubscribe WebSocket
        try:
            ws = self.provider._ensure_ws()
            if self._subscribed_codes:
                ws.unsubscribe(self._subscribed_codes, "0B")
            if self._original_price_cb is not None:
                ws.set_on_price_tick(self._original_price_cb)
                self._original_price_cb = None
        except Exception as e:
            logger.warning(f"[SURGE] WebSocket unsubscribe: {e}")

        # Save results
        summary = compute_summary(self._trades)
        out_dir = get_output_dir()
        try:
            save_trades_csv(self._trades, out_dir)
            save_summary_json(summary, out_dir)
            save_state_json(self.get_state(), out_dir)
            self._logger.flush_debug_log(out_dir / "debug_events_sim.log")
        except Exception as e:
            logger.error(f"[SURGE] Save results failed: {e}")

        self._logger.log(SurgeLogTag.SIM_RESULT,
                         trigger_reason=f"STOP trades={len(self._trades)}")

        return {"ok": True, "summary": summary}

    # ── WebSocket Callback (called from WS thread) ────────

    def _on_price_tick(self, code: str, values: dict) -> None:
        """
        Called from WebSocket thread.
        1. Parse tick (lock 밖)
        2. Lock 안에서: write cache → freeze snap → drain queue → evaluate
        3. Forward to original callback (lock 밖)
        """
        if not self.running:
            if self._original_price_cb:
                self._original_price_cb(code, values)
            return

        # 1. Parse tick data (lock 밖 — pure transformation)
        parsed = self._parse_tick(code, values)
        if not parsed:
            if self._original_price_cb:
                self._original_price_cb(code, values)
            return

        self._tick_count += 1

        # 2. Lock 안에서 모든 상태 변경
        with self._lock:
            # 2a. Write to price cache
            self._price_cache[code] = parsed

            # 2b. Freeze snapshot (1회 — 이 tick의 모든 판단에 이 snap 사용)
            snap = dict(parsed)

            # 2c. Drain candidate queue
            self._drain_candidate_queue()

            # 2d. Evaluate
            now = time.time()
            if code in self._positions:
                self._evaluate_exit(code, snap, now)
            else:
                current_state = self._state.get(code)
                if current_state == StockState.WATCHING:
                    self._evaluate_entry(code, snap, now)

            # 2e. Update position live data
            if code in self._positions:
                pos = self._positions[code]
                pos.current_price = snap.get("price", 0)
                pos.current_bid = snap.get("bid", 0)
                pos.current_ask = snap.get("ask", 0)

        # 3. Forward to original callback (lock 밖)
        if self._original_price_cb:
            self._original_price_cb(code, values)

    def _parse_tick(self, code: str, values: dict) -> Optional[dict]:
        """Parse WebSocket 0B values into normalized dict. Pure function."""
        try:
            def _abs_int(v):
                return abs(int(str(v).replace("+", "").replace("-", "").replace(",", "") or "0"))

            price = _abs_int(values.get("10", "0"))
            if price <= 0:
                return None
            return {
                "price": price,
                "ask": _abs_int(values.get("27", "0")),
                "bid": _abs_int(values.get("28", "0")),
                "ask_size": _abs_int(values.get("1030", "0")),
                "bid_size": _abs_int(values.get("1031", "0")),
                "volume": _abs_int(values.get("13", "0")),
                "ts_epoch": time.time(),
                "ts_str": values.get("20", ""),
            }
        except Exception as e:
            logger.debug(f"[SURGE] Parse tick error {code}: {e}")
            return None

    # ── Entry / Exit (called under self._lock) ────────────

    def _evaluate_entry(self, code: str, snap: dict, now: float) -> None:
        """
        Entry evaluation. MUST be called under self._lock.
        Uses ONLY the passed `snap` — no price_cache re-read.
        """
        candidate = self._candidates.get(code)
        if not candidate:
            return

        # Risk guard check
        can_enter, risk_reason = self._risk.can_enter(code, self.config)
        if not can_enter:
            self._logger.log(
                SurgeLogTag.ENTRY_BLOCKED,
                code=code, name=candidate.name,
                strategy_state=str(self._state.get(code)),
                trigger_reason=risk_reason,
                bid=snap.get("bid", 0), ask=snap.get("ask", 0),
                bid_size=snap.get("bid_size", 0), ask_size=snap.get("ask_size", 0),
                last_price=snap.get("price", 0),
            )
            if "HALT" in risk_reason or "DAILY_LIMIT" in risk_reason:
                self._logger.log(SurgeLogTag.DAILY_STOP_TRIGGER,
                                 code=code, trigger_reason=risk_reason)
            return

        # Cooldown check
        if not self._state.is_cooled_down(code):
            self._logger.log(
                SurgeLogTag.COOLDOWN_SKIP,
                code=code, name=candidate.name,
                strategy_state=str(self._state.get(code)),
                trigger_reason="COOLDOWN_ACTIVE",
            )
            return

        # Duplicate check
        if code in self._positions:
            self._logger.log(
                SurgeLogTag.DUPLICATE_BLOCK,
                code=code, name=candidate.name,
                trigger_reason="ALREADY_IN_POSITION",
            )
            return

        # Calculate order qty
        per_trade_cash = self._cash * (self.config.per_trade_pct / 100)
        ask_price = snap.get("ask", 0)
        if ask_price <= 0:
            return
        qty = int(per_trade_cash / ask_price)
        if qty <= 0:
            return

        # Signal rules check
        self._logger.log(
            SurgeLogTag.ENTRY_CHECK,
            code=code, name=candidate.name,
            strategy_state=str(self._state.get(code)),
            bid=snap.get("bid", 0), ask=ask_price,
            bid_size=snap.get("bid_size", 0), ask_size=snap.get("ask_size", 0),
            last_price=snap.get("price", 0),
        )

        passed, reason = check_entry(
            candidate_tr_ts=candidate.tr_ts,
            snap=snap,
            qty=qty,
            config=self.config,
            now=now,
        )
        if not passed:
            tag = SurgeLogTag.STALE_SKIP if "STALE" in reason else SurgeLogTag.ENTRY_BLOCKED
            self._logger.log(
                tag,
                code=code, name=candidate.name,
                strategy_state=str(self._state.get(code)),
                trigger_reason=reason,
                bid=snap.get("bid", 0), ask=ask_price,
                bid_size=snap.get("bid_size", 0), ask_size=snap.get("ask_size", 0),
                last_price=snap.get("price", 0),
            )
            if "STALE" in reason:
                self._state.transition(code, StockState.SKIPPED, reason)
            return

        # Simulate fill
        fill = simulate_buy(code, qty, snap, now, self.config)
        if not fill:
            self._logger.log(
                SurgeLogTag.ENTRY_BLOCKED,
                code=code, name=candidate.name,
                trigger_reason="FILL_FAILED",
                ask=ask_price, ask_size=snap.get("ask_size", 0),
            )
            self._state.transition(code, StockState.SKIPPED, "FILL_FAILED")
            return

        # Entry success
        self._state.transition(code, StockState.BOUGHT, "SIM_FILLED")

        pos = SurgePosition(
            code=code,
            name=candidate.name,
            entry_fill_price=fill.fill_price,
            raw_ask_at_entry=fill.market_price,
            qty=fill.fill_qty,
            entry_ts=now,
            entry_time_str=datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
            entry_slippage_pct=fill.slippage_pct,
            entry_fee=fill.fee,
            current_price=snap.get("price", 0),
            current_bid=snap.get("bid", 0),
            current_ask=snap.get("ask", 0),
        )
        self._positions[code] = pos
        self._cash -= fill.fill_price * fill.fill_qty + fill.fee
        self._risk.on_entry(code)

        self._logger.log(
            SurgeLogTag.ENTRY_SIM_FILLED,
            code=code, name=candidate.name,
            strategy_state="BOUGHT",
            trigger_reason=f"qty={qty} fill@{fill.fill_price}",
            bid=snap.get("bid", 0), ask=ask_price,
            bid_size=snap.get("bid_size", 0), ask_size=snap.get("ask_size", 0),
            last_price=snap.get("price", 0),
            expected_fill_price=fill.fill_price,
        )

    def _evaluate_exit(self, code: str, snap: dict, now: float) -> None:
        """
        Exit evaluation. MUST be called under self._lock.
        Uses ONLY the passed `snap`.
        """
        pos = self._positions.get(code)
        if not pos:
            return

        bid = snap.get("bid", 0)
        if bid <= 0:
            bid = snap.get("price", 0)
        if bid <= 0:
            return

        # Check exit conditions (priority: force > SL > TP > time)
        exit_reason = None
        if check_force_exit(now):
            exit_reason = "FORCE_EXIT"
        elif check_sl(pos.entry_fill_price, bid, self.config):
            exit_reason = "SL"
        elif check_tp(pos.entry_fill_price, bid, self.config):
            exit_reason = "TP"
        elif check_time_exit(pos.entry_ts, now, self.config):
            exit_reason = "TIME_EXIT"

        if exit_reason:
            self._close_position(code, snap, exit_reason, now)

    def _close_position(self, code: str, snap: dict, exit_reason: str, now: float) -> None:
        """Close position and record trade. MUST be called under self._lock."""
        pos = self._positions.get(code)
        if not pos:
            return

        # Simulate sell
        fill = simulate_sell(code, pos.qty, snap, now, self.config)
        if not fill:
            # Fallback: use current price
            fill_price = snap.get("price", pos.entry_fill_price)
            raw_bid = snap.get("bid", fill_price)
            slip = get_time_slippage(now, self.config) * 100
            fee = fill_price * pos.qty * self.config.fee_rate
            tax = fill_price * pos.qty * self.config.tax_rate
        else:
            fill_price = fill.fill_price
            raw_bid = fill.market_price
            slip = fill.slippage_pct
            fee = fill.fee
            tax = fill.tax

        # PnL
        gross_pnl_pct, net_pnl_pct = calc_pnl_pct(
            pos.entry_fill_price, fill_price, self.config
        )
        gross_pnl_krw = (fill_price - pos.entry_fill_price) * pos.qty
        net_pnl_krw = gross_pnl_krw - pos.entry_fee - fee - tax
        holding_sec = now - pos.entry_ts

        # Record trade
        self._trade_counter += 1
        trade = TradeRecord(
            trade_id=self._trade_counter,
            code=code,
            name=pos.name,
            entry_time=pos.entry_time_str,
            exit_time=datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S"),
            entry_fill_price=pos.entry_fill_price,
            exit_fill_price=fill_price,
            raw_ask_at_entry=pos.raw_ask_at_entry,
            raw_bid_at_exit=raw_bid,
            qty=pos.qty,
            gross_pnl_pct=gross_pnl_pct,
            net_pnl_pct=net_pnl_pct,
            gross_pnl_krw=gross_pnl_krw,
            net_pnl_krw=net_pnl_krw,
            entry_reason="SURGE_ENTRY",
            exit_reason=exit_reason,
            entry_slippage_pct=pos.entry_slippage_pct,
            exit_slippage_pct=slip,
            fee_entry=pos.entry_fee,
            fee_exit=fee,
            tax=tax,
            holding_seconds=round(holding_sec, 1),
        )
        self._trades.append(trade)

        # Update cash
        self._cash += fill_price * pos.qty - fee - tax

        # Log exit
        log_tag = {
            "TP": SurgeLogTag.TP_HIT,
            "SL": SurgeLogTag.SL_HIT,
            "TIME_EXIT": SurgeLogTag.TIME_EXIT,
            "FORCE_EXIT": SurgeLogTag.TIME_EXIT,
        }.get(exit_reason, SurgeLogTag.SIM_RESULT)

        self._logger.log(
            log_tag,
            code=code, name=pos.name,
            strategy_state="CLOSED",
            trigger_reason=exit_reason,
            bid=snap.get("bid", 0), ask=snap.get("ask", 0),
            last_price=snap.get("price", 0),
            expected_fill_price=fill_price,
            pnl_pct=net_pnl_pct,
            holding_seconds=round(holding_sec, 1),
        )

        # State transition
        self._state.transition(code, StockState.CLOSED, exit_reason)
        is_win = net_pnl_pct > 0
        self._risk.on_exit(code, is_win, self.config)

        # Set cooldown
        self._state.set_cooldown(code, self.config.cooldown_sec)

        # Remove position
        del self._positions[code]

    # ── Candidate Queue ───────────────────────────────────

    def _drain_candidate_queue(self) -> None:
        """Process pending candidates. MUST be called under self._lock."""
        now = time.time()
        while self._candidate_queue:
            c = self._candidate_queue.popleft()
            # Stale discard
            if now - c.tr_ts > self.config.max_tr_lag_sec:
                continue
            # Already registered
            if self._state.get(c.code) is not None:
                continue
            self._seen_codes.add(c.code)
            self._candidates[c.code] = c
            self._state.transition(c.code, StockState.SCANNED, "TR_RECEIVED")
            self._state.transition(c.code, StockState.WATCHING, "FILTER_PASS")

            self._logger.log(
                SurgeLogTag.SURGE_CANDIDATE,
                code=c.code, name=c.name,
                last_price=c.price,
                trigger_reason=f"rank={c.rank} chg={c.change_pct}%",
            )

            # Subscribe new code to WS
            try:
                ws = self.provider._ensure_ws()
                ws.subscribe([c.code], "0B")
                self._subscribed_codes.append(c.code)
            except Exception as e:
                logger.warning(f"[SURGE] WS subscribe {c.code}: {e}")

    # ── Periodic Scan ─────────────────────────────────────

    def _schedule_scan(self) -> None:
        if not self._scan_running:
            return
        self._scan_timer = threading.Timer(
            self.config.scan_interval_sec,
            self._periodic_scan,
        )
        self._scan_timer.daemon = True
        self._scan_timer.start()

    def _periodic_scan(self) -> None:
        """Runs in background thread. Puts candidates into queue only."""
        if not self.running or not self._scan_running:
            return

        try:
            candidates = self._scanner.scan(self.provider, self.config)
            filtered = filter_candidates(candidates, self.config)

            self._logger.log(
                SurgeLogTag.SURGE_TR_RECEIVED,
                trigger_reason=f"SCAN found={len(candidates)} filtered={len(filtered)}",
            )

            for c in filtered:
                if c.code and c.code not in self._seen_codes:
                    self._candidate_queue.append(c)

        except Exception as e:
            logger.warning(f"[SURGE] Periodic scan error: {e}")

        # Reschedule
        self._schedule_scan()

    # ── State Accessors ───────────────────────────────────

    def get_state(self) -> dict:
        """Full state snapshot for SSE/API."""
        with self._lock:
            positions = []
            for code, pos in self._positions.items():
                bid = pos.current_bid or pos.current_price
                if bid > 0 and pos.entry_fill_price > 0:
                    pnl_pct = (bid / pos.entry_fill_price - 1) * 100
                else:
                    pnl_pct = 0
                positions.append({
                    "code": code,
                    "name": pos.name,
                    "entry_price": pos.entry_fill_price,
                    "current_price": pos.current_price,
                    "bid": pos.current_bid,
                    "ask": pos.current_ask,
                    "qty": pos.qty,
                    "pnl_pct": round(pnl_pct, 2),
                    "holding_sec": round(time.time() - pos.entry_ts, 1),
                    "tp_price": math.ceil(pos.entry_fill_price * (1 + self.config.tp_pct / 100)),
                    "sl_price": math.floor(pos.entry_fill_price * (1 - self.config.sl_pct / 100)),
                })

            trades = [asdict(t) for t in self._trades[-50:]]

            watching = self._state.get_codes_in_state(StockState.WATCHING)
            candidates_list = []
            for code in watching[:20]:
                c = self._candidates.get(code)
                if c:
                    candidates_list.append({
                        "code": c.code, "name": c.name,
                        "price": c.price, "change_pct": c.change_pct,
                        "rank": c.rank,
                    })

            return {
                "running": self.running,
                "elapsed_sec": round(time.time() - self._start_time, 1) if self._start_time else 0,
                "tick_count": self._tick_count,
                "cash": round(self._cash),
                "positions": positions,
                "trades": trades,
                "trade_count": len(self._trades),
                "candidates": candidates_list,
                "risk": self._risk.get_state(),
                "state_summary": self._state.get_state_summary(),
                "events": self._logger.get_recent(30),
            }

    def get_trades(self) -> List[dict]:
        with self._lock:
            return [asdict(t) for t in self._trades]

    def get_summary(self) -> dict:
        with self._lock:
            return compute_summary(self._trades)

    def has_state_changed(self) -> bool:
        """Check if state changed since last call (for SSE optimization)."""
        state = self.get_state()
        h = hashlib.md5(
            json.dumps(state, sort_keys=True, default=str).encode()
        ).hexdigest()
        if h != self._state_hash:
            self._state_hash = h
            return True
        return False
