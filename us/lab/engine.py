# -*- coding: utf-8 -*-
"""
engine.py — US Strategy Lab Simulation Engine
===============================================
Daily loop: universe filter → signal generation → order execution → trail stop.
Immutable DailySnapshot pattern (no look-ahead).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .lab_config import BUY_COST, SELL_COST, INITIAL_CASH, CASH_BUFFER

logger = logging.getLogger("qtron.us.lab.engine")


# ── Immutable Daily Snapshot ────────────────────────────

@dataclass(frozen=True)
class DailySnapshot:
    """Frozen snapshot of a single trading day. No look-ahead."""
    date: str
    day_idx: int
    close_dict: Dict[str, float]     # {symbol: close_price}
    open_dict: Dict[str, float]      # {symbol: open_price}
    high_dict: Dict[str, float]      # {symbol: high_price}
    low_dict: Dict[str, float]       # {symbol: low_price}
    volume_dict: Dict[str, float]    # {symbol: volume}


# ── Position & State ───────────────────────────────────

@dataclass
class SimPosition:
    symbol: str
    quantity: int
    avg_price: float
    entry_date: str
    high_watermark: float
    entry_day_idx: int = 0

    @property
    def cost_basis(self) -> float:
        return self.quantity * self.avg_price


@dataclass
class StrategyState:
    name: str
    cash: float = INITIAL_CASH
    positions: Dict[str, SimPosition] = field(default_factory=dict)
    trades: List[dict] = field(default_factory=list)
    equity_history: List[Tuple[str, float]] = field(default_factory=list)
    positions_count_history: List[int] = field(default_factory=list)
    pending_buys: List[dict] = field(default_factory=list)
    day_count: int = 0


# ── Strategy Interface ──────────────────────────────────

class StrategyBase:
    """
    All strategies implement:
    - generate_signals(snapshot, state, ohlcv_matrices) -> (buys, sells)
      buys: [{"symbol": str, "reason": str}, ...]
      sells: [{"symbol": str, "reason": str}, ...]
    """
    name: str = "base"
    config: dict = {}

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        raise NotImplementedError


# ── Simulation Engine ───────────────────────────────────

def run_simulation(
    strategy: StrategyBase,
    ohlcv_dict: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    config: dict = None,
) -> StrategyState:
    """
    Run daily simulation loop for a single strategy.

    Args:
        strategy: StrategyBase implementation
        ohlcv_dict: {symbol: DataFrame(date, open, high, low, close, volume)}
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        config: strategy config from lab_config.STRATEGY_CONFIGS
    """
    config = config or {}
    trail_pct = config.get("trail_pct", 0.12)
    max_positions = config.get("max_positions", 20)

    # Build aligned date index
    all_dates = set()
    for df in ohlcv_dict.values():
        if "date" in df.columns:
            all_dates.update(df["date"].astype(str).tolist())
        elif hasattr(df.index, "strftime"):
            all_dates.update(df.index.strftime("%Y-%m-%d").tolist())

    dates = sorted(d for d in all_dates if start_date <= d <= end_date)
    if not dates:
        logger.warning(f"[ENGINE] No dates in range {start_date}~{end_date}")
        return StrategyState(name=strategy.name)

    # Pre-build price matrices for fast lookup
    matrices = _build_matrices(ohlcv_dict, dates)

    state = StrategyState(name=strategy.name)

    for day_idx, date_str in enumerate(dates):
        state.day_count = day_idx + 1

        # Build immutable snapshot (no look-ahead)
        snapshot = _build_snapshot(matrices, day_idx, date_str)

        # 1. Process pending buys (T+1 fill at open)
        _process_pending_buys(state, snapshot)

        # 2. Check trail stops
        _check_trail_stops(state, snapshot, trail_pct)

        # 3. Generate signals
        try:
            buys, sells = strategy.generate_signals(snapshot, state, matrices)
        except Exception as e:
            logger.error(f"[ENGINE] {strategy.name} signal error on {date_str}: {e}")
            buys, sells = [], []

        # 4. Process sell signals (immediate at close)
        for sell in sells:
            sym = sell.get("symbol", "")
            if sym in state.positions:
                _execute_sell(state, sym, snapshot.close_dict.get(sym, 0),
                             date_str, day_idx, sell.get("reason", "signal"))

        # 5. Queue buy signals (T+1 at next open)
        available_slots = max_positions - len(state.positions) - len(state.pending_buys)
        for buy in buys[:available_slots]:
            sym = buy.get("symbol", "")
            if sym not in state.positions and sym not in [p["symbol"] for p in state.pending_buys]:
                state.pending_buys.append({
                    "symbol": sym,
                    "reason": buy.get("reason", "signal"),
                    "queued_date": date_str,
                    "queued_day_idx": day_idx,
                })

        # 6. Mark-to-market equity
        equity = state.cash
        for sym, pos in state.positions.items():
            price = snapshot.close_dict.get(sym, pos.avg_price)
            equity += pos.quantity * price

        state.equity_history.append((date_str, equity))
        state.positions_count_history.append(len(state.positions))

    return state


# ── Internal Helpers ────────────────────────────────────

def _build_matrices(ohlcv_dict: Dict[str, pd.DataFrame],
                    dates: List[str]) -> dict:
    """Build price matrices for fast vectorized access."""
    date_set = set(dates)
    close_mat = {}
    open_mat = {}
    high_mat = {}
    low_mat = {}
    vol_mat = {}

    for sym, df in ohlcv_dict.items():
        if "date" in df.columns:
            df = df.set_index("date")
        df.index = pd.to_datetime(df.index).strftime("%Y-%m-%d")
        close_mat[sym] = df["close"].to_dict()
        open_mat[sym] = df["open"].to_dict()
        high_mat[sym] = df["high"].to_dict()
        low_mat[sym] = df["low"].to_dict()
        vol_mat[sym] = df["volume"].to_dict() if "volume" in df.columns else {}

    return {
        "close": close_mat,
        "open": open_mat,
        "high": high_mat,
        "low": low_mat,
        "volume": vol_mat,
        "dates": dates,
    }


def _build_snapshot(matrices: dict, day_idx: int, date_str: str) -> DailySnapshot:
    """Build immutable snapshot for a single day."""
    close_dict = {sym: prices.get(date_str, 0) for sym, prices in matrices["close"].items()
                  if prices.get(date_str, 0) > 0}
    open_dict = {sym: prices.get(date_str, 0) for sym, prices in matrices["open"].items()
                 if prices.get(date_str, 0) > 0}
    high_dict = {sym: prices.get(date_str, 0) for sym, prices in matrices["high"].items()
                 if prices.get(date_str, 0) > 0}
    low_dict = {sym: prices.get(date_str, 0) for sym, prices in matrices["low"].items()
                if prices.get(date_str, 0) > 0}
    volume_dict = {sym: prices.get(date_str, 0) for sym, prices in matrices["volume"].items()
                   if prices.get(date_str, 0) > 0}

    return DailySnapshot(
        date=date_str, day_idx=day_idx,
        close_dict=close_dict, open_dict=open_dict,
        high_dict=high_dict, low_dict=low_dict,
        volume_dict=volume_dict,
    )


def _process_pending_buys(state: StrategyState, snapshot: DailySnapshot):
    """Fill pending buys at today's open price (T+1 model)."""
    filled = []
    for pending in state.pending_buys:
        sym = pending["symbol"]
        open_price = snapshot.open_dict.get(sym, 0)
        if open_price <= 0:
            continue

        # Equal weight allocation
        n_target = max(1, len(state.pending_buys))
        allocation = state.cash * CASH_BUFFER / n_target
        cost_price = open_price * (1 + BUY_COST)
        qty = int(allocation / cost_price)

        if qty <= 0:
            continue

        total_cost = qty * cost_price
        if total_cost > state.cash:
            continue

        state.cash -= total_cost
        state.positions[sym] = SimPosition(
            symbol=sym,
            quantity=qty,
            avg_price=open_price,
            entry_date=snapshot.date,
            high_watermark=open_price,
            entry_day_idx=snapshot.day_idx,
        )
        state.trades.append({
            "symbol": sym,
            "side": "BUY",
            "qty": qty,
            "price": open_price,
            "date": snapshot.date,
            "reason": pending.get("reason", "signal"),
        })
        filled.append(pending)

    for f in filled:
        state.pending_buys.remove(f)


def _check_trail_stops(state: StrategyState, snapshot: DailySnapshot,
                       trail_pct: float):
    """Check trail stops for all positions."""
    to_sell = []
    for sym, pos in state.positions.items():
        price = snapshot.close_dict.get(sym, 0)
        if price <= 0:
            continue

        # Update HWM
        pos.high_watermark = max(pos.high_watermark, price)

        # Check trigger
        trigger = pos.high_watermark * (1 - trail_pct)
        if price <= trigger:
            to_sell.append((sym, price))

    for sym, price in to_sell:
        _execute_sell(state, sym, price, snapshot.date, snapshot.day_idx, "trail_stop")


def _execute_sell(state: StrategyState, symbol: str, price: float,
                  date_str: str, day_idx: int, reason: str):
    """Execute a sell order at given price."""
    pos = state.positions.pop(symbol, None)
    if not pos or price <= 0:
        return

    proceeds = pos.quantity * price * (1 - SELL_COST)
    state.cash += proceeds

    pnl = proceeds - pos.cost_basis
    hold_days = day_idx - pos.entry_day_idx

    state.trades.append({
        "symbol": symbol,
        "side": "SELL",
        "qty": pos.quantity,
        "price": price,
        "date": date_str,
        "pnl": round(pnl, 2),
        "pnl_pct": round(pnl / pos.cost_basis * 100, 2) if pos.cost_basis > 0 else 0,
        "hold_days": hold_days,
        "exit_reason": reason,
    })


# ── Helper: safe_slice for indicators ───────────────────

def safe_close_series(matrices: dict, symbol: str, day_idx: int) -> pd.Series:
    """Get close series up to (not including) day_idx — no look-ahead."""
    dates = matrices["dates"][:day_idx]
    prices = matrices["close"].get(symbol, {})
    values = [prices.get(d, np.nan) for d in dates]
    return pd.Series(values, index=dates)
