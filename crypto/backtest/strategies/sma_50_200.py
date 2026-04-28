"""50/200 SMA Trend strategy.

Signal:
    A pair is "in trend" at signal date ``asof`` when SMA(close, 50) is
    strictly greater than SMA(close, 200) — the canonical golden-cross
    state. The opposite (death cross) means the pair is dropped from
    picks; engine smart-rebal will exit any held position naturally.

Window:
    * lookback = 200 days (the slow SMA window). The engine retreats
      the start so 200 closes are available before the first rebal.

Score (for top-N selection when more than ``top_n`` pairs are in trend):
    score = SMA50 / SMA200 - 1
    Higher = stronger trend acceleration. This is NOT a tunable parameter
    — it is a deterministic ranking key for the cross-active set, used
    only when the trend-active universe exceeds ``top_n``. Per Jeff PR #3
    lock: no parameter tuning.

NaN handling (Jeff E5=A):
    A pair is skipped THIS rebal if any of:
        - Fewer than ``min_data_days`` closes in the 200-day window
        - SMA200 is NaN, zero, or non-positive at asof
        - SMA50 is NaN at asof

Tiebreaker: equal scores → pair name ascending (G6 determinism).

Defaults are canonical (50/200), not tuned. The ``Config`` dataclass
exists so PR #3 reviewers can see the constants in one place; downstream
PRs MUST NOT vary them without an explicit Jeff decision.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.strategies.base import Strategy


@dataclass(frozen=True)
class SMA50_200Config:
    fast_window: int = 50
    slow_window: int = 200
    lookback_days: int = 200
    min_data_days: int = 180  # tolerate up to ~20 missing days within the 200d window


class SMA50_200Trend(Strategy):
    """Top-N trend-active pairs (SMA50 > SMA200), ranked by SMA50/SMA200."""

    name = "sma_50_200_trend"

    def __init__(self, config: Optional[SMA50_200Config] = None) -> None:
        self.config = config or SMA50_200Config()
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

        scores: dict[str, float] = {}
        for pair in sorted(universe):
            df = loader.load_pair(pair, window_start, asof)
            if len(df) < cfg.min_data_days:
                continue
            closes = df["close"].astype(float)
            # SMA at asof = mean of last N closes (inclusive of asof).
            if len(closes) < cfg.slow_window:
                continue
            sma_fast = closes.iloc[-cfg.fast_window:].mean()
            sma_slow = closes.iloc[-cfg.slow_window:].mean()
            if not _is_finite_positive(sma_slow) or not _is_finite(sma_fast):
                continue
            if sma_fast <= sma_slow:  # not in trend
                continue
            scores[pair] = float(sma_fast / sma_slow - 1.0)

        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        picks = [pair for pair, _ in ranked[:top_n]]
        return sorted(picks)


def _is_finite(value) -> bool:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return not (math.isnan(v) or math.isinf(v))


def _is_finite_positive(value) -> bool:
    if not _is_finite(value):
        return False
    return float(value) > 0
