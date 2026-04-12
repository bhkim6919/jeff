"""
lowvol_momentum.py — Strategy 2: LowVol_Momentum (Gen4 대조군)
================================================================
Alpha: Low Volatility 30%ile → Momentum 12-1 Top N
Exit: Trail -12% / rebalance
Group: rebal

backtester.py run_backtest와 동일 로직. validation 대조용.
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import TrailStopExit
from lab.lab_config import StrategyConfig, STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal

# Reuse Gen4 SHARED scoring functions
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from strategy.scoring import calc_volatility, calc_momentum


class LowVolMomentum(BaseStrategy):
    """Gen4 Core LowVol+Momentum — Lab 대조군."""

    def __init__(self):
        cfg = STRATEGY_CONFIGS["lowvol_momentum"]
        super().__init__(
            name="lowvol_momentum",
            config=cfg,
            exit_policy=TrailStopExit(trail_pct=0.12),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        # Score universe — safe_slice로 당일 제외
        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        scored = []
        for tk in snapshot.universe:
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 252:
                continue

            v = calc_volatility(series, 252)
            if np.isnan(v):
                continue
            m = calc_momentum(series, 252, 22)
            if np.isnan(m):
                continue
            scored.append({"tk": tk, "vol": v, "mom": m})

        if not scored:
            return signals

        sdf = pd.DataFrame(scored)
        vol_thresh = sdf["vol"].quantile(0.30)
        low_vol = sdf[sdf["vol"] <= vol_thresh]
        candidates = low_vol[low_vol["mom"] > 0]

        if candidates.empty:
            return signals

        # BASELINE_SPEC: tie-break by ticker ascending
        top = (candidates.sort_values(["mom", "tk"], ascending=[False, True])
               .head(self.config.max_positions))
        target_codes = set(top["tk"].tolist())

        # SELL: non-targets
        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL", reason="REBALANCE"))

        # BUY: new targets (sorted for determinism)
        for tk in sorted(target_codes):
            if tk not in positions:
                mom_val = float(top[top["tk"] == tk]["mom"].iloc[0])
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason="LOWVOL_MOM_TOP",
                    priority=mom_val,
                ))

        return signals
