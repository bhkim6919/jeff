# -*- coding: utf-8 -*-
"""
quality_factor.py — Quality Composite Factor
==============================================
REBAL group | 21-day rebalance | Trail -12% | 20 positions
Score = ROE proxy (40%) + Value proxy (30%) + Dividend proxy (30%)
Uses price-based proxies since fundamental data not yet in DB.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class QualityFactorStrategy(StrategyBase):
    name = "quality_factor"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        rebal_days = self.config.get("rebal_days", 21)
        max_pos = self.config.get("max_positions", 20)

        if state.day_count > 0 and state.day_count % rebal_days != 0:
            return buys, sells

        if snapshot.day_idx < 252 + 10:
            return buys, sells

        scores = []
        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 252:
                continue

            close_arr = series.values
            returns = np.diff(np.log(close_arr[-252:]))
            returns = returns[np.isfinite(returns)]
            if len(returns) < 200:
                continue

            # Quality proxy: low volatility = high quality (stable earnings proxy)
            vol = np.std(returns)
            if vol <= 0:
                continue
            quality_score = 1.0 / vol  # lower vol → higher quality

            # Value proxy: mean-reversion (lower relative price → better value)
            ma_200 = np.mean(close_arr[-200:])
            cur_price = close_arr[-1]
            if ma_200 <= 0:
                continue
            value_score = ma_200 / cur_price  # below MA → higher value

            # Momentum component (positive only)
            mom = close_arr[-22] / close_arr[-252] - 1 if close_arr[-252] > 0 else 0
            if mom <= 0:
                continue

            # Composite: Quality 40% + Value 30% + Momentum 30%
            composite = quality_score * 0.4 + value_score * 0.3 + mom * 0.3
            scores.append((sym, composite))

        # Percentile-rank and select top N
        scores.sort(key=lambda x: -x[1])
        target = [s[0] for s in scores[:max_pos]]
        target_set = set(target)

        for sym in list(state.positions.keys()):
            if sym not in target_set:
                sells.append({"symbol": sym, "reason": "rebal_exit"})

        current_set = set(state.positions.keys())
        for sym in target:
            if sym not in current_set:
                buys.append({"symbol": sym, "reason": "rebal_entry"})

        return buys, sells
