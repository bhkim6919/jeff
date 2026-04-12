# -*- coding: utf-8 -*-
"""
mean_reversion.py — RSI Oversold Entry
========================================
EVENT group | No scheduled rebalance | Trail -5% | 5 positions
Entry: RSI(14) < 30 AND close > MA200. Exit: RSI > 50 / 5-day hold / stop.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


def _calc_rsi(close_arr, period=14):
    """Compute RSI from close price array."""
    if len(close_arr) < period + 1:
        return 50  # neutral default
    deltas = np.diff(close_arr[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains)
    avg_loss = np.mean(losses)
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


class MeanReversionStrategy(StrategyBase):
    name = "mean_reversion"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        max_pos = self.config.get("max_positions", 5)
        rsi_entry = self.config.get("rsi_entry", 30)
        rsi_exit = self.config.get("rsi_exit", 50)
        max_hold = self.config.get("max_hold_days", 5)

        if snapshot.day_idx < 200 + 15:
            return buys, sells

        # Check exits first: RSI > exit OR hold > max days
        for sym, pos in list(state.positions.items()):
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 15:
                continue
            rsi = _calc_rsi(series.values)
            hold_days = snapshot.day_idx - pos.entry_day_idx

            if rsi > rsi_exit:
                sells.append({"symbol": sym, "reason": "rsi_exit"})
            elif hold_days >= max_hold:
                sells.append({"symbol": sym, "reason": "timeout"})

        # Entry: RSI < entry AND close > MA200
        available = max_pos - len(state.positions) + len(sells)
        if available <= 0:
            return buys, sells

        candidates = []
        for sym in snapshot.close_dict:
            if sym in state.positions:
                continue

            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 200:
                continue

            close_arr = series.values
            rsi = _calc_rsi(close_arr)
            ma200 = np.mean(close_arr[-200:])
            cur = snapshot.close_dict[sym]

            if rsi < rsi_entry and cur > ma200 and ma200 > 0:
                # Priority: lower RSI = higher priority
                candidates.append((sym, rsi))

        candidates.sort(key=lambda x: x[1])  # lowest RSI first
        for sym, rsi in candidates[:available]:
            buys.append({"symbol": sym, "reason": "rsi_oversold"})

        return buys, sells
