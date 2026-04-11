# -*- coding: utf-8 -*-
"""
momentum_base.py — Pure 12-1 Month Momentum
=============================================
REBAL group | 21-day rebalance | Trail -12% | 20 positions
No volatility filter — pure momentum ranking.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class MomentumBaseStrategy(StrategyBase):
    name = "momentum_base"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        rebal_days = self.config.get("rebal_days", 21)
        max_pos = self.config.get("max_positions", 20)
        mom_lookback = 252
        mom_skip = 22

        # Rebalance check
        if state.day_count > 0 and state.day_count % rebal_days != 0:
            return buys, sells

        # Need enough history
        if snapshot.day_idx < mom_lookback + 10:
            return buys, sells

        # Score all symbols: Mom12-1
        scores = []
        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < mom_lookback:
                continue
            close_arr = series.values
            # Skip last mom_skip days, use mom_lookback window
            if len(close_arr) < mom_lookback:
                continue
            cur = close_arr[-mom_skip] if len(close_arr) > mom_skip else close_arr[-1]
            past = close_arr[-mom_lookback] if len(close_arr) >= mom_lookback else close_arr[0]
            if past <= 0:
                continue
            mom = cur / past - 1
            if mom > 0:
                scores.append((sym, mom))

        # Rank by momentum descending
        scores.sort(key=lambda x: -x[1])
        target = [s[0] for s in scores[:max_pos]]
        target_set = set(target)

        # Sell: positions not in new target
        for sym in list(state.positions.keys()):
            if sym not in target_set:
                sells.append({"symbol": sym, "reason": "rebal_exit"})

        # Buy: new target not in positions
        current_set = set(state.positions.keys())
        for sym in target:
            if sym not in current_set:
                buys.append({"symbol": sym, "reason": "rebal_entry"})

        return buys, sells
