# -*- coding: utf-8 -*-
"""
hybrid_qscore.py — 5-Axis Hybrid Composite
=============================================
REBAL group | 21-day rebalance | Trail -12% | 20 positions
RS 25% + Sector 20% + Quality(LowVol) 20% + Trend(MA200) 15% + LowVol 20%
Most complex — highest overfitting risk.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class HybridQscoreStrategy(StrategyBase):
    name = "hybrid_qscore"

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
        # Pre-compute sector returns (60d) if sector data available
        # For now, skip sector axis (set to 0)

        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 252:
                continue

            close_arr = series.values
            returns = np.diff(np.log(close_arr[-252:]))
            returns = returns[np.isfinite(returns)]
            if len(returns) < 200:
                continue

            # Axis 1: RS (60-day relative strength) — 25%
            if len(close_arr) >= 60 and close_arr[-60] > 0:
                rs = close_arr[-1] / close_arr[-60] - 1
            else:
                rs = 0

            # Axis 2: Sector — 20% (placeholder, 0 for now)
            sector_score = 0

            # Axis 3: Quality (inverse vol) — 20%
            vol = np.std(returns)
            quality = 1.0 / vol if vol > 0 else 0

            # Axis 4: Trend (above MA200) — 15%
            ma200 = np.mean(close_arr[-200:]) if len(close_arr) >= 200 else 0
            trend = 1.0 if close_arr[-1] > ma200 and ma200 > 0 else 0

            # Axis 5: LowVol score — 20%
            lowvol = 1.0 / (vol + 0.001)  # lower vol → higher score

            # Composite (normalized within ranking later)
            composite = rs * 0.25 + sector_score * 0.20 + quality * 0.20 + trend * 0.15 + lowvol * 0.20

            if rs > 0:  # only positive momentum
                scores.append((sym, composite))

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
