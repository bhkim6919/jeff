"""ATR-buffered Donchian breakout strategy.

Signal:
    A pair triggers a breakout at signal date ``asof`` when

        close[asof] > donchian_high[asof-1, 20] + k * atr[asof, 14]

    where:
        donchian_high = rolling 20-day max of close, ENDING the day before
                        asof (we must not peek at asof itself when forming
                        the threshold — that would make the signal trivially
                        unsatisfiable when close[asof] equals its own max).
        atr           = mean of true-range over the trailing 14 days
                        (Wilder's classical ATR period; mean is used in
                        place of Wilder smoothing for determinism).
        k             = 2.0 (literature-standard ATR multiple).

Window:
    * lookback_days = 35 (enough for both 20-day donchian + 14-day ATR
      with one extra day of buffer for the shift).

Score (top-N selection):
    score = (close[asof] - threshold) / atr
    Higher = more ATRs above the breakout level (stronger thrust). When
    fewer than ``top_n`` pairs are above threshold, all of them are picked.

NaN handling (Jeff E5=A):
    Skip the pair THIS rebal if any of:
        - Fewer than ``min_data_days`` rows in the window
        - close[asof] is NaN, zero, or non-positive
        - ATR is NaN or zero (no true volatility signal)
        - Donchian high is NaN

Tiebreaker: equal scores → pair name ascending (G6 determinism).

Defaults are canonical (Wilder ATR=14, Donchian=20, k=2.0). Per Jeff PR
#3 lock: no parameter tuning. The ``Config`` dataclass lives in this
module so reviewers can audit constants in one place; downstream PRs MUST
NOT vary them without explicit Jeff decision.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.strategies.base import Strategy


@dataclass(frozen=True)
class ATRBreakoutConfig:
    donchian_window: int = 20
    atr_window: int = 14
    k_multiplier: float = 2.0
    lookback_days: int = 35  # max(donchian, atr) + 1 buffer
    min_data_days: int = 30  # tolerate up to 5 missing days


class ATRBreakout(Strategy):
    """Top-N pairs whose close exceeds donchian_high + k*ATR at asof."""

    name = "atr_breakout"

    def __init__(self, config: Optional[ATRBreakoutConfig] = None) -> None:
        self.config = config or ATRBreakoutConfig()
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
            close = df["close"].astype(float)
            high = df["high"].astype(float)
            low = df["low"].astype(float)

            close_asof = close.iloc[-1]
            if not _is_finite_positive(close_asof):
                continue

            # Donchian high over the 20 closes ENDING the day before asof.
            # rolling(window).max().shift(1) gives us the max as-of day
            # before — the canonical "look-back-only" channel.
            donchian = close.rolling(cfg.donchian_window).max().shift(1)
            donchian_at_asof = donchian.iloc[-1]
            if not _is_finite(donchian_at_asof):
                continue

            atr_value = _compute_atr(high, low, close, cfg.atr_window)
            if not _is_finite_positive(atr_value):
                continue

            threshold = float(donchian_at_asof) + cfg.k_multiplier * float(atr_value)
            if close_asof <= threshold:
                continue  # not in breakout

            scores[pair] = float((close_asof - threshold) / atr_value)

        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        picks = [pair for pair, _ in ranked[:top_n]]
        return sorted(picks)


def _compute_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    window: int,
) -> float:
    """ATR = mean(true_range, window) where TR = max(H-L, |H-C_prev|, |L-C_prev|).

    Returns the value at the most recent index. Uses simple-mean rather than
    Wilder smoothing so two runs over the same data produce byte-identical
    floats (G6 determinism — Wilder's recursive form is also deterministic
    in principle but its initial-value convention varies between libraries
    and would therefore couple our hash to a third-party choice).
    """
    if len(close) < 2:
        return float("nan")
    prev_close = close.shift(1)
    tr_a = high - low
    tr_b = (high - prev_close).abs()
    tr_c = (low - prev_close).abs()
    tr = pd.concat([tr_a, tr_b, tr_c], axis=1).max(axis=1)
    if len(tr) < window:
        return float("nan")
    atr = tr.iloc[-window:].mean()
    return float(atr)


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
