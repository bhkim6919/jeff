"""
mean_reversion.py — Strategy 4: MeanReversion
================================================
Alpha: RSI(14) < 30 + MA200 filter
Exit: RSI > 50 / 5일 / -5%
Group: event
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import MeanReversionExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal


def _calc_rsi(close_series: pd.Series, period: int = 14) -> float:
    """RSI 계산."""
    if len(close_series) < period + 1:
        return 50.0  # neutral
    delta = close_series.diff().iloc[-period:]
    gain = delta.clip(lower=0).mean()
    loss = (-delta.clip(upper=0)).mean()
    if loss == 0:
        return 100.0
    rs = gain / loss
    return 100.0 - (100.0 / (1.0 + rs))


class MeanReversionStrategy(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["mean_reversion"]
        super().__init__(
            name="mean_reversion",
            config=cfg,
            exit_policy=MeanReversionExit(rsi_exit=50.0, max_hold=5, max_loss=-0.05),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        if len(close_hist) < 200:
            return signals

        # Update RSI state for exit policy
        for tk in list(positions.keys()):
            if tk in close_hist.columns:
                series = close_hist[tk].dropna()
                rsi = _calc_rsi(series, 14)
                self._state[f"rsi_{tk}"] = rsi

        # Scan for oversold entries
        available_slots = self.config.max_positions - len(positions)
        if available_slots <= 0:
            return signals

        oversold = []
        for tk in snapshot.universe:
            if tk in positions:
                continue
            if tk not in close_hist.columns:
                continue

            series = close_hist[tk].dropna()
            if len(series) < 200:
                continue

            rsi = _calc_rsi(series, 14)
            self._state[f"rsi_{tk}"] = rsi

            if rsi >= 30:
                continue

            # MA200 trend filter — only buy if above 200-day MA
            ma200 = series.iloc[-200:].mean()
            today_close = float(snapshot.close.get(tk, 0))
            if today_close <= 0 or pd.isna(today_close):
                continue
            if today_close < ma200:
                continue

            oversold.append({"tk": tk, "rsi": rsi})

        if not oversold:
            return signals

        odf = pd.DataFrame(oversold)
        # Most oversold first
        top = odf.sort_values(["rsi", "tk"], ascending=[True, True]).head(available_slots)

        for _, row in top.iterrows():
            signals.append(Signal(
                ticker=row["tk"],
                direction="BUY",
                reason="RSI_OVERSOLD",
                priority=30.0 - row["rsi"],  # lower RSI = higher priority
            ))

        return signals
