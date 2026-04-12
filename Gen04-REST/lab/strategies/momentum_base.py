"""
momentum_base.py — Strategy 1: Momentum_Base (순수 모멘텀)
==========================================================
Alpha: 12-1 month cross-sectional momentum, Top N
Exit: Trail -12% / rebalance
Group: rebal

LowVol 필터 없음 — 순수 모멘텀 벤치마크.
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import TrailStopExit
from lab.lab_config import StrategyConfig, STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from strategy.scoring import calc_momentum


class MomentumBase(BaseStrategy):
    """순수 12-1M 모멘텀. LowVol 필터 없음."""

    def __init__(self):
        cfg = STRATEGY_CONFIGS["momentum_base"]
        super().__init__(
            name="momentum_base",
            config=cfg,
            exit_policy=TrailStopExit(trail_pct=0.12),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        scored = []
        for tk in snapshot.universe:
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 252:
                continue

            m = calc_momentum(series, 252, 22)
            if np.isnan(m) or m <= 0:
                continue
            scored.append({"tk": tk, "mom": m})

        if not scored:
            return signals

        sdf = pd.DataFrame(scored)
        top = (sdf.sort_values(["mom", "tk"], ascending=[False, True])
               .head(self.config.max_positions))
        target_codes = set(top["tk"].tolist())

        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL", reason="REBALANCE"))

        for tk in sorted(target_codes):
            if tk not in positions:
                mom_val = float(top[top["tk"] == tk]["mom"].iloc[0])
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason="MOM_TOP",
                    priority=mom_val,
                ))

        return signals
