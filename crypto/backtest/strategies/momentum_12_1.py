"""Momentum 12-1 strategy.

Definition (KR Gen4 차용):
    Score = total return over the past 12 months EXCLUDING the most recent
    month. Standard academic momentum factor — captures medium-term trend
    without being polluted by short-term reversal in the last 30 days.

Window:
    * Signal date ``asof``
    * Window end   = asof - 30 days
    * Window start = asof - 365 days

NaN handling (Jeff E5=A):
    A pair is skipped THIS rebalance if any of:
        - Insufficient data in the window (gaps > tolerance)
        - Either window endpoint price is NaN
        - Either endpoint price <= 0
    The pair returns to the universe automatically on the next rebalance
    once data is healthy — no permanent exclusion.

Tiebreaker (Jeff 보완 #4):
    Equal scores → sort by pair name ascending. Same input → same output.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import math

from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.strategies.base import Strategy


@dataclass(frozen=True)
class Momentum12_1Config:
    lookback_days: int = 365
    skip_days: int = 30
    min_data_days: int = 250  # tolerance for the 365-day window (allow up to ~115 missing)


class Momentum12_1(Strategy):
    """Top-N by 12-1 momentum score, sorted ascending by pair name."""

    name = "momentum_12_1"

    def __init__(self, config: Optional[Momentum12_1Config] = None) -> None:
        self.config = config or Momentum12_1Config()
        self.lookback_days = self.config.lookback_days

    def select(
        self,
        *,
        asof: date,
        universe: list[str],
        loader: OhlcvLoader,
        top_n: int,
    ) -> list[str]:
        cfg = self.config
        window_start = asof - timedelta(days=cfg.lookback_days)
        window_end = asof - timedelta(days=cfg.skip_days)

        scores: dict[str, float] = {}
        for pair in sorted(universe):  # deterministic iteration
            df = loader.load_pair(pair, window_start, window_end)
            if len(df) < cfg.min_data_days:
                continue
            close_start = df["close"].iloc[0]
            close_end = df["close"].iloc[-1]
            if not _is_finite_positive(close_start) or not _is_finite_positive(close_end):
                continue
            scores[pair] = float(close_end / close_start - 1.0)

        # Rank: score desc, pair asc as tiebreaker.
        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        picks = [pair for pair, _ in ranked[:top_n]]
        # Final return order is sorted ascending by pair name (engine-side
        # determinism — score-rank order is captured in ``ranked`` above for
        # logging if the engine wants it).
        return sorted(picks)


def _is_finite_positive(value) -> bool:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if math.isnan(v) or math.isinf(v):
        return False
    return v > 0
