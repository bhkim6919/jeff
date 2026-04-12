"""
breakout_trend.py — Strategy 3: Breakout_Trend
================================================
Alpha: 60일 신고가 돌파
Exit: Trail -8%
Group: event
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import BreakoutExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal


class BreakoutTrend(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["breakout_trend"]
        super().__init__(
            name="breakout_trend",
            config=cfg,
            exit_policy=BreakoutExit(trail_pct=0.08),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        # 매일 돌파 종목 스캔
        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        if len(close_hist) < 60:
            return signals

        breakouts = []
        for tk in snapshot.universe:
            if tk not in close_hist.columns:
                continue
            if tk in positions:
                continue

            series = close_hist[tk].dropna()
            if len(series) < 60:
                continue

            today_close = float(snapshot.close.get(tk, 0))
            if today_close <= 0 or pd.isna(today_close):
                continue

            # 60일 신고가 돌파
            high_60 = float(series.iloc[-60:].max())
            if high_60 <= 0:
                continue

            if today_close > high_60:
                # 돌파 강도 = 현재가 / 60일 고가
                strength = today_close / high_60 - 1
                breakouts.append({"tk": tk, "strength": strength})

        if not breakouts:
            return signals

        # max_positions 제한
        available_slots = self.config.max_positions - len(positions)
        if available_slots <= 0:
            return signals

        bdf = pd.DataFrame(breakouts)
        top = bdf.sort_values(["strength", "tk"], ascending=[False, True]).head(available_slots)

        for _, row in top.iterrows():
            signals.append(Signal(
                ticker=row["tk"],
                direction="BUY",
                reason="BREAKOUT_60D",
                priority=row["strength"],
            ))

        return signals
