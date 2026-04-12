"""
sector_rotation.py — Strategy 6: Sector_Rotation
==================================================
Alpha: Sector-level 60-day momentum → Top 3 sectors → 개별 momentum Top N
Exit: Trail -12% / sector fallout
Group: macro
"""
from __future__ import annotations
from typing import Dict, List, Set

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import SectorRotationExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from strategy.scoring import calc_momentum


class SectorRotation(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["sector_rotation"]
        super().__init__(
            name="sector_rotation",
            config=cfg,
            exit_policy=SectorRotationExit(trail_pct=0.12),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        if len(close_hist) < 60:
            return signals

        # Build sector -> stocks mapping
        sector_stocks: Dict[str, List[str]] = {}
        for tk in snapshot.universe:
            info = snapshot.sector_map.get(tk, {})
            sector = info.get("sector", "") if isinstance(info, dict) else ""
            if not sector:
                continue
            if sector not in sector_stocks:
                sector_stocks[sector] = []
            sector_stocks[sector].append(tk)

        if not sector_stocks:
            return signals

        # Calculate sector-level 60-day return
        sector_returns = {}
        for sector, stocks in sector_stocks.items():
            rets = []
            for tk in stocks:
                if tk not in close_hist.columns:
                    continue
                series = close_hist[tk].dropna()
                if len(series) < 60:
                    continue
                ret = series.iloc[-1] / series.iloc[-60] - 1
                if not np.isnan(ret):
                    rets.append(ret)
            if rets:
                sector_returns[sector] = np.mean(rets)

        if not sector_returns:
            return signals

        # Top 3 sectors
        sorted_sectors = sorted(sector_returns.items(), key=lambda x: -x[1])
        top_sectors: Set[str] = {s[0] for s in sorted_sectors[:3]}
        self._state["top_sectors"] = top_sectors

        # Within top sectors, rank by individual momentum
        candidates = []
        for tk in snapshot.universe:
            info = snapshot.sector_map.get(tk, {})
            sector = info.get("sector", "") if isinstance(info, dict) else ""
            if sector not in top_sectors:
                continue
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 60:
                continue
            mom = series.iloc[-1] / series.iloc[-60] - 1
            if np.isnan(mom) or mom <= 0:
                continue
            candidates.append({"tk": tk, "mom": mom, "sector": sector})

        if not candidates:
            return signals

        cdf = pd.DataFrame(candidates)
        top = (cdf.sort_values(["mom", "tk"], ascending=[False, True])
               .head(self.config.max_positions))
        target_codes = set(top["tk"].tolist())

        # SELL non-targets
        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL", reason="SECTOR_REBAL"))

        # BUY
        for _, row in top.iterrows():
            tk = row["tk"]
            if tk not in positions:
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason="SECTOR_TOP",
                    priority=row["mom"],
                ))

        return signals
