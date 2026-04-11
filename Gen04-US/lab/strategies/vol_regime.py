# -*- coding: utf-8 -*-
"""
vol_regime.py — VIX-Based Adaptive Strategy
=============================================
REGIME group (isolated) | 21-day rebalance | Trail -12% | 20 positions
High VIX → LowVol stocks (defensive)
Low VIX → High momentum stocks (offensive)
Neutral → Gen4 core (LowVol + Mom)
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class VolRegimeStrategy(StrategyBase):
    name = "vol_regime"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        rebal_days = self.config.get("rebal_days", 21)
        max_pos = self.config.get("max_positions", 20)

        if state.day_count > 0 and state.day_count % rebal_days != 0:
            return buys, sells

        if snapshot.day_idx < 272:
            return buys, sells

        # Determine regime from market volatility (proxy: 20d realized vol of broad index)
        # Use average vol across all stocks as proxy
        all_vols = []
        all_moms = []
        sym_data = {}

        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 252:
                continue

            close_arr = series.values
            returns = np.diff(np.log(close_arr[-252:]))
            returns = returns[np.isfinite(returns)]
            if len(returns) < 200:
                continue

            vol_252 = np.std(returns)
            vol_20 = np.std(returns[-20:]) if len(returns) >= 20 else vol_252

            # Momentum
            if close_arr[-252] > 0:
                mom = close_arr[-22] / close_arr[-252] - 1
            else:
                mom = 0

            all_vols.append(vol_20)
            sym_data[sym] = {"vol": vol_252, "vol_20": vol_20, "mom": mom}

        if not all_vols:
            return buys, sells

        # Market regime: percentile of current market vol
        market_vol = np.mean(all_vols)
        vol_arr = np.array(all_vols)
        vol_pct = np.percentile(vol_arr, [25, 75])

        if market_vol >= vol_pct[1]:
            regime = "high_vol"  # defensive
        elif market_vol <= vol_pct[0]:
            regime = "low_vol"   # offensive
        else:
            regime = "neutral"

        # Select stocks based on regime
        candidates = []
        for sym, d in sym_data.items():
            if regime == "high_vol":
                # Defensive: lowest volatility stocks
                score = -d["vol"]  # lower vol = better
            elif regime == "low_vol":
                # Offensive: highest momentum (positive only)
                if d["mom"] <= 0:
                    continue
                score = d["mom"]
            else:
                # Neutral: LowVol + Mom (Gen4 core)
                if d["vol"] == 0 or d["mom"] <= 0:
                    continue
                # Low vol percentile check
                vol_threshold = np.percentile([v["vol"] for v in sym_data.values()], 20)
                if d["vol"] > vol_threshold:
                    continue
                score = d["mom"]

            candidates.append((sym, score))

        candidates.sort(key=lambda x: -x[1])
        target = [s[0] for s in candidates[:max_pos]]
        target_set = set(target)

        for sym in list(state.positions.keys()):
            if sym not in target_set:
                sells.append({"symbol": sym, "reason": f"regime_{regime}_exit"})

        current_set = set(state.positions.keys())
        for sym in target:
            if sym not in current_set:
                buys.append({"symbol": sym, "reason": f"regime_{regime}_entry"})

        return buys, sells
