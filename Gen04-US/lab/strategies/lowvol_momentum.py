# -*- coding: utf-8 -*-
"""
lowvol_momentum.py — Gen4 Core: LowVol + Momentum
===================================================
REBAL group | 21-day rebalance | Trail -12% | 20 positions
LowVol bottom 20%ile → Mom12-1 top N.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class LowvolMomentumStrategy(StrategyBase):
    name = "lowvol_momentum"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        rebal_days = self.config.get("rebal_days", 21)
        max_pos = self.config.get("max_positions", 20)
        vol_percentile = self.config.get("vol_percentile", 0.20)
        vol_lookback = 252
        mom_lookback = 252
        mom_skip = 22

        if state.day_count > 0 and state.day_count % rebal_days != 0:
            return buys, sells

        if snapshot.day_idx < vol_lookback + 10:
            return buys, sells

        # Step 1: Calculate volatility for all
        vol_scores = []
        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < vol_lookback:
                continue
            returns = np.diff(np.log(series.values[-vol_lookback:]))
            returns = returns[np.isfinite(returns)]
            if len(returns) < 200:
                continue
            vol = np.std(returns)
            if vol > 0:
                vol_scores.append((sym, vol))

        if not vol_scores:
            return buys, sells

        # Step 2: LowVol filter (bottom percentile)
        vol_scores.sort(key=lambda x: x[1])
        cutoff = int(len(vol_scores) * vol_percentile)
        low_vol_syms = set(s[0] for s in vol_scores[:max(cutoff, 1)])

        # Step 3: Momentum ranking within low-vol
        mom_scores = []
        for sym in low_vol_syms:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < mom_lookback:
                continue
            close_arr = series.values
            cur = close_arr[-mom_skip] if len(close_arr) > mom_skip else close_arr[-1]
            past = close_arr[-mom_lookback] if len(close_arr) >= mom_lookback else close_arr[0]
            if past <= 0:
                continue
            mom = cur / past - 1
            if mom > 0:
                mom_scores.append((sym, mom))

        mom_scores.sort(key=lambda x: -x[1])
        target = [s[0] for s in mom_scores[:max_pos]]
        target_set = set(target)

        for sym in list(state.positions.keys()):
            if sym not in target_set:
                sells.append({"symbol": sym, "reason": "rebal_exit"})

        current_set = set(state.positions.keys())
        for sym in target:
            if sym not in current_set:
                buys.append({"symbol": sym, "reason": "rebal_entry"})

        return buys, sells
