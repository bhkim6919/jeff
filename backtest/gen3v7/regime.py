"""
regime.py — Regime detection (MA200 + Breadth + Flip Gate) + RAL
================================================================
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Dict, Tuple


# ── Regime (BULL / BEAR) ─────────────────────────────────────────

class RegimeDetector:
    """MA200-based regime with breadth filter and flip gate."""

    def __init__(self, ma_period: int = 200, breadth_threshold: float = 0.35,
                 flip_gate_days: int = 2):
        self.ma_period = ma_period
        self.breadth_threshold = breadth_threshold
        self.flip_gate_days = flip_gate_days

        self._prev_raw_bull: bool = True
        self._flip_countdown: int = 0
        self._confirmed_bull: bool = True

    def update(self, kospi_close: float, kospi_ma200: float,
               breadth: float) -> bool:
        """Update regime. Returns is_bull_effective.

        Args:
            kospi_close: KOSPI index close
            kospi_ma200: 200-day MA of KOSPI
            breadth: fraction of universe above MA20 (0~1)
        """
        raw_bull = kospi_close > kospi_ma200

        # Breadth override: even if MA200 says BULL, low breadth forces BEAR
        is_bull_eff = raw_bull and (breadth >= self.breadth_threshold)

        # Flip gate: delay regime changes by N days
        if raw_bull != self._prev_raw_bull:
            self._flip_countdown = self.flip_gate_days
            self._prev_raw_bull = raw_bull
        elif self._flip_countdown > 0:
            self._flip_countdown -= 1

        if self._flip_countdown > 0:
            # During gate period, keep previous confirmed regime
            pass
        else:
            self._confirmed_bull = is_bull_eff

        return self._confirmed_bull

    @property
    def is_bull(self) -> bool:
        return self._confirmed_bull


# ── RAL (Reactive Adaptive Layer) ────────────────────────────────

class RALDetector:
    """RAL mode based on KOSPI daily return (shifted by 1 day)."""

    CRASH = "CRASH"
    SURGE = "SURGE"
    NORMAL = "NORMAL"

    def __init__(self, crash_threshold: float = -0.02,
                 surge_threshold: float = 0.015):
        self.crash_threshold = crash_threshold
        self.surge_threshold = surge_threshold
        self.mode = self.NORMAL

    def update(self, kospi_daily_return: float) -> str:
        """Update RAL mode from KOSPI daily return (use shift(1) value).

        Returns: "CRASH" | "SURGE" | "NORMAL"
        """
        if kospi_daily_return < self.crash_threshold:
            self.mode = self.CRASH
        elif kospi_daily_return > self.surge_threshold:
            self.mode = self.SURGE
        else:
            self.mode = self.NORMAL
        return self.mode

    def adjust_sl(self, current_sl: float, entry_price: float,
                  atr20: float, crash_sl_mult: float = 0.60,
                  surge_relax: float = 0.50) -> float:
        """Adjust SL based on RAL mode.

        CRASH: tighten SL (pull up toward entry)
        SURGE: relax SL (push down from current)
        """
        if self.mode == self.CRASH:
            tighter_sl = entry_price - 1.0 * crash_sl_mult * atr20
            return max(current_sl, tighter_sl)
        elif self.mode == self.SURGE:
            relaxed_sl = current_sl - surge_relax * atr20
            return min(current_sl, relaxed_sl)
        return current_sl


def compute_breadth(universe_signals: Dict[str, pd.DataFrame],
                    date_idx: int) -> float:
    """Compute breadth = fraction of universe above MA20."""
    above = 0
    total = 0
    for ticker, df in universe_signals.items():
        if date_idx < len(df):
            val = df.iloc[date_idx].get("above_ma20", np.nan)
            if not np.isnan(val):
                total += 1
                if val == 1:
                    above += 1
    return above / total if total > 0 else 0.0
