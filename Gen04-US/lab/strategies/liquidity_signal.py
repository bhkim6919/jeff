# -*- coding: utf-8 -*-
"""
liquidity_signal.py — Volume Surge Entry
==========================================
EVENT group | No scheduled rebalance | Trail -10% | 10 positions
Entry: Volume 2x surge + green candle. Exit: volume decay or trail.
"""
from __future__ import annotations

import numpy as np
from typing import List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series


class LiquiditySignalStrategy(StrategyBase):
    name = "liquidity_signal"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        max_pos = self.config.get("max_positions", 10)
        surge_ratio = self.config.get("vol_surge_ratio", 2.0)

        if snapshot.day_idx < 25:
            return buys, sells

        dates = matrices["dates"]

        # Check exits: volume decay
        for sym, pos in list(state.positions.items()):
            vol_data = matrices["volume"].get(sym, {})
            # Recent 5d avg vs 20d avg
            recent_vols = [vol_data.get(dates[i], 0) for i in range(max(0, snapshot.day_idx - 5), snapshot.day_idx)]
            avg_5d = np.mean(recent_vols) if recent_vols else 0
            longer_vols = [vol_data.get(dates[i], 0) for i in range(max(0, snapshot.day_idx - 20), snapshot.day_idx)]
            avg_20d = np.mean(longer_vols) if longer_vols else 1

            if avg_20d > 0 and avg_5d / avg_20d < 0.5:
                sells.append({"symbol": sym, "reason": "vol_decay"})

        # Entry: volume surge + green candle
        available = max_pos - len(state.positions) + len(sells)
        if available <= 0:
            return buys, sells

        candidates = []
        for sym in snapshot.close_dict:
            if sym in state.positions:
                continue

            today_vol = snapshot.volume_dict.get(sym, 0)
            if today_vol <= 0:
                continue

            # 20-day avg volume
            vol_data = matrices["volume"].get(sym, {})
            hist_vols = [vol_data.get(dates[i], 0)
                        for i in range(max(0, snapshot.day_idx - 20), snapshot.day_idx)]
            avg_vol = np.mean([v for v in hist_vols if v > 0]) if hist_vols else 0

            if avg_vol <= 0:
                continue

            ratio = today_vol / avg_vol

            # Green candle: close > open
            today_close = snapshot.close_dict.get(sym, 0)
            today_open = snapshot.open_dict.get(sym, 0)

            if ratio >= surge_ratio and today_close > today_open and today_close > 0:
                candidates.append((sym, ratio))

        candidates.sort(key=lambda x: -x[1])  # highest surge first
        for sym, ratio in candidates[:available]:
            buys.append({"symbol": sym, "reason": "vol_surge"})

        return buys, sells
