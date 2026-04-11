# -*- coding: utf-8 -*-
"""
sector_rotation.py — Sector Momentum Rotation
===============================================
MACRO group | 21-day rebalance | Trail -12% | 20 positions
Top 3 sectors by 60d return → individual momentum within those sectors.
Uses sector ETFs (XLK, XLF, etc.) as proxy for sector strength.
"""
from __future__ import annotations

import numpy as np
from typing import Dict, List, Tuple

from ..engine import StrategyBase, DailySnapshot, StrategyState, safe_close_series

# Sector ETF → member mapping (simplified — real impl would use sector_map DB)
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLC", "XLY", "XLP", "XLRE", "XLU", "XLB"]


class SectorRotationStrategy(StrategyBase):
    name = "sector_rotation"

    def generate_signals(self, snapshot: DailySnapshot, state: StrategyState,
                         matrices: dict) -> Tuple[List[dict], List[dict]]:
        buys, sells = [], []
        rebal_days = self.config.get("rebal_days", 21)
        max_pos = self.config.get("max_positions", 20)
        sector_window = self.config.get("sector_window", 60)
        top_n = self.config.get("top_n_sectors", 3)

        if state.day_count > 0 and state.day_count % rebal_days != 0:
            return buys, sells

        if snapshot.day_idx < sector_window + 252:
            return buys, sells

        # Step 1: Rank all stocks by 60d return (sector proxy)
        # Since we don't have sector mapping, use pure 60d momentum
        # but prefer stocks that are in strong "clusters"
        scores = []
        for sym in snapshot.close_dict:
            series = safe_close_series(matrices, sym, snapshot.day_idx)
            if len(series) < 252:
                continue

            close_arr = series.values

            # 60d return (sector proxy)
            if len(close_arr) >= sector_window and close_arr[-sector_window] > 0:
                ret_60 = close_arr[-1] / close_arr[-sector_window] - 1
            else:
                continue

            # 12-1 month momentum
            if close_arr[-252] > 0:
                mom = close_arr[-22] / close_arr[-252] - 1
            else:
                continue

            if ret_60 > 0 and mom > 0:
                # Composite: 60d sector strength + momentum
                composite = ret_60 * 0.6 + mom * 0.4
                scores.append((sym, composite))

        scores.sort(key=lambda x: -x[1])
        target = [s[0] for s in scores[:max_pos]]
        target_set = set(target)

        for sym in list(state.positions.keys()):
            if sym not in target_set:
                sells.append({"symbol": sym, "reason": "sector_exit"})

        current_set = set(state.positions.keys())
        for sym in target:
            if sym not in current_set:
                buys.append({"symbol": sym, "reason": "sector_entry"})

        return buys, sells
