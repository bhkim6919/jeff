# -*- coding: utf-8 -*-
"""
breakout_trend.py — 60-Day High Breakout
==========================================
EVENT group | No scheduled rebalance | Trail -8% | 15 positions
Entry: price breaks 60-day high. Exit: trail stop or signal.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class BreakoutTrendStrategy(StrategyBase):
    name = "breakout_trend"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        max_pos = self.config.get("max_positions", 15)
        window = self.config.get("breakout_window", 60)

        if snapshot.day_idx < window + 5:
            return buys, sells

        # Scan for breakouts
        breakout_candidates = []
        for sym in snapshot.close_dict:
            if sym in state.positions:
                continue

            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < window:
                continue

            close_arr = series.values
            high_60 = np.max(close_arr[-window:])
            cur = snapshot.close_dict[sym]

            if cur > high_60 and high_60 > 0:
                strength = cur / high_60 - 1  # breakout strength
                breakout_candidates.append((sym, strength))

        # Sort by strength, take up to available slots
        breakout_candidates.sort(key=lambda x: -x[1])
        available = max_pos - len(state.positions)
        for sym, strength in breakout_candidates[:max(available, 0)]:
            buys.append({"symbol": sym, "reason": "breakout_60d"})

        return buys, sells
