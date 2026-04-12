"""
quality_factor.py — Strategy 5: Quality_Factor
================================================
Alpha: ROE (EPS/BPS) + Value (1/PBR) + Dividend yield 복합
Exit: Trail -12% / rebalance
Group: rebal
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import TrailStopExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal


class QualityFactor(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["quality_factor"]
        super().__init__(
            name="quality_factor",
            config=cfg,
            exit_policy=TrailStopExit(trail_pct=0.12),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        # Need fundamental data
        if snapshot.fundamental is None or snapshot.fundamental.empty:
            return signals

        fund = snapshot.fundamental.copy()

        # Ensure numeric
        for col in ["eps", "bps", "pbr", "div_yield"]:
            if col in fund.columns:
                fund[col] = pd.to_numeric(fund[col], errors="coerce")

        # Filter to universe
        if "ticker" in fund.columns:
            fund = fund[fund["ticker"].isin(snapshot.universe)]
        elif "stk_cd" in fund.columns:
            fund = fund[fund["stk_cd"].isin(snapshot.universe)]
            fund = fund.rename(columns={"stk_cd": "ticker"})
        else:
            return signals

        if fund.empty:
            return signals

        # Quality composite score
        # ROE proxy: EPS / BPS
        if "eps" in fund.columns and "bps" in fund.columns:
            fund["roe"] = fund["eps"] / fund["bps"].replace(0, np.nan)
        else:
            fund["roe"] = np.nan

        # Value: 1/PBR (lower PBR = higher value)
        if "pbr" in fund.columns:
            fund["value"] = 1.0 / fund["pbr"].replace(0, np.nan)
        else:
            fund["value"] = np.nan

        # Dividend yield
        if "div_yield" not in fund.columns:
            fund["div_yield"] = 0

        # Rank each component (higher = better)
        for col in ["roe", "value", "div_yield"]:
            fund[f"{col}_rank"] = fund[col].rank(ascending=True, pct=True)

        # Composite: 40% ROE + 30% Value + 30% Dividend
        fund["q_score"] = (
            fund["roe_rank"].fillna(0) * 0.40
            + fund["value_rank"].fillna(0) * 0.30
            + fund["div_yield_rank"].fillna(0) * 0.30
        )

        top = (fund.sort_values(["q_score", "ticker"], ascending=[False, True])
               .head(self.config.max_positions))
        target_codes = set(top["ticker"].tolist())

        # SELL non-targets
        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL", reason="QUALITY_REBAL"))

        # BUY new targets
        for _, row in top.iterrows():
            tk = row["ticker"]
            if tk not in positions:
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason="QUALITY_TOP",
                    priority=row["q_score"],
                ))

        return signals
