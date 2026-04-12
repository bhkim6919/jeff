"""
vol_regime.py — Strategy 7: Volatility_Regime
===============================================
Alpha: KOSPI 변동성 레짐 → 방어(LowVol) / 공격(Momentum) 전환
Exit: Trail -12% / rebalance
Group: regime (isolated)
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import TrailStopExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from strategy.scoring import calc_volatility, calc_momentum


class VolRegime(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["vol_regime"]
        super().__init__(
            name="vol_regime",
            config=cfg,
            exit_policy=TrailStopExit(trail_pct=0.12),
        )

    def _detect_regime(self, snapshot: DailySnapshot) -> str:
        """KOSPI 20일 실현변동성으로 레짐 판별."""
        idx = snapshot.index_series
        if len(idx) < 252:
            return "neutral"

        # 20일 realized vol
        rets = idx.pct_change().dropna()
        if len(rets) < 252:
            return "neutral"

        vol_20d = float(rets.iloc[-20:].std())
        # 252일 분포에서 quartile
        vol_hist = rets.rolling(20).std().dropna()
        if len(vol_hist) < 50:
            return "neutral"

        q25 = float(vol_hist.quantile(0.25))
        q75 = float(vol_hist.quantile(0.75))

        if vol_20d >= q75:
            return "high_vol"  # 방어
        elif vol_20d <= q25:
            return "low_vol"   # 공격
        return "neutral"

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        regime = self._detect_regime(snapshot)
        self._state["regime"] = regime

        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        if len(close_hist) < 252:
            return signals

        scored = []
        for tk in snapshot.universe:
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 252:
                continue

            v = calc_volatility(series, 252)
            m = calc_momentum(series, 252, 22)
            if np.isnan(v) or np.isnan(m):
                continue
            scored.append({"tk": tk, "vol": v, "mom": m})

        if not scored:
            return signals

        sdf = pd.DataFrame(scored)

        if regime == "high_vol":
            # 방어: LowVol stocks (vol 낮은 순)
            candidates = sdf.sort_values("vol", ascending=True).head(self.config.max_positions)
        elif regime == "low_vol":
            # 공격: High Momentum
            candidates = sdf[sdf["mom"] > 0].sort_values("mom", ascending=False).head(self.config.max_positions)
        else:
            # Neutral: LowVol + Momentum (Gen4 core)
            vol_thresh = sdf["vol"].quantile(0.30)
            low_vol = sdf[sdf["vol"] <= vol_thresh]
            candidates = (low_vol[low_vol["mom"] > 0]
                         .sort_values("mom", ascending=False)
                         .head(self.config.max_positions))

        target_codes = set(candidates["tk"].tolist())

        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL",
                    reason=f"REGIME_{regime.upper()}_REBAL"))

        for _, row in candidates.iterrows():
            tk = row["tk"]
            if tk not in positions:
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason=f"REGIME_{regime.upper()}",
                    priority=abs(row["mom"]),
                ))

        return signals
