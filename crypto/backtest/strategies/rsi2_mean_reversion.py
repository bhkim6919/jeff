"""RSI2 Mean Reversion — D5 STEP 2 (Jeff F10=B / F11=A / F12=B).

Signal at signal date ``asof`` — three conditions ALL required:

    1. SMA200 safety filter (Jeff F11=A — avoid down-trending pairs)
       Close[asof] > SMA200[asof]

    2. RSI2 oversold (Jeff F10=B)
       RSI2[asof] < 10

    3. Volatility-band confirmation (Jeff F10=B — at least one)
       Close[asof] < LowerBB20,2[asof]
         OR
       Close[asof] < SMA20[asof] - 2 * STD20[asof]
       (these are the same band, written two ways for spec clarity)

Score (Jeff F12=B — z-score ascending):
    z = (Close[asof] - SMA20[asof]) / STD20[asof]
    Most negative z first → deepest oversold ranks highest.

Window:
    SMA200 dominates: lookback_days = 200. The engine retreats by 200
    calendar days so SMA200 is computable at the first rebal anchor.
    No discretionary buffer beyond this — Jeff D5 lock applies.

NaN handling (Jeff E5=A):
    Skip pair THIS rebal if any of:
        - Insufficient data (< min_data_days in the 200-day window)
        - SMA200 / SMA20 / STD20 non-finite or non-positive
        - Close[asof] non-finite or non-positive
        - RSI2 non-finite (e.g. < 2 daily deltas computable)
        - Any of the three signal conditions fails

Tiebreaker: equal z-scores → pair name ascending (G6 determinism).

RSI2 implementation (deterministic for G6):
    Standard 2-period RSI from simple-mean gain/loss over the most recent
    two daily deltas. We do NOT use Wilder smoothing; for period=2 the
    smoothing tail is a single bar and Wilder's recursive form would
    couple this hash to a third-party library's init-value convention.
    Same input → same RSI2.

Engine fit caveat (acknowledged in PR #20 description):
    The original RSI2 strategy is a 3~5 day pullback play. Our engine is
    rebal-cycle (21 days) per Jeff D5 STEP 1 F3=A — no SL/TP/time-based
    exits. So a pair entered at a rebal anchor is held until the NEXT
    anchor 21 days later, by which time the bounce has typically ended.
    Alpha is expected to weaken vs the original spec. STEP 2 verifies
    deterministic execution, NOT strategy alpha.

Per Jeff D5 STEP 2 lock: no parameter tuning of any default; engine /
cost_model / universe untouched; no SL / TP / time-based / Risk Gate /
Regime Switching.
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
class RSI2MeanReversionConfig:
    rsi_period: int = 2
    rsi_threshold: float = 10.0
    bb_window: int = 20
    bb_std_mult: float = 2.0
    sma_safety: int = 200
    lookback_days: int = 200
    min_data_days: int = 180  # tolerance for ~10% missing in 200-day window


class RSI2MeanReversion(Strategy):
    """Top-N oversold pairs (RSI2 + Bollinger), ranked by z-score asc."""

    name = "rsi2_mean_reversion"

    def __init__(self, config: Optional[RSI2MeanReversionConfig] = None) -> None:
        self.config = config or RSI2MeanReversionConfig()
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
        # Calendar-day window with a small buffer; the engine's warmup
        # retreat already aligns ``data_min`` to ``asof - lookback_days``,
        # but per-pair listings may start later, in which case the
        # min_data_days check filters the pair.
        window_start = asof - timedelta(days=cfg.lookback_days + 5)

        scores: dict[str, float] = {}
        for pair in sorted(universe):
            df = loader.load_pair(pair, window_start, asof)
            if len(df) < cfg.min_data_days:
                continue

            close = df["close"].astype(float)
            if len(close) < cfg.sma_safety:
                continue

            close_asof = close.iloc[-1]
            if not _is_finite_positive(close_asof):
                continue

            # SMA200 safety filter (Jeff F11=A)
            sma200 = close.iloc[-cfg.sma_safety:].mean()
            if not _is_finite_positive(sma200):
                continue
            if close_asof <= sma200:
                continue  # down-trending — skip per safety filter

            # Bollinger band over 20 days, sample-std (ddof=1) per
            # standard convention.
            if len(close) < cfg.bb_window:
                continue
            bb_window_close = close.iloc[-cfg.bb_window:]
            sma20 = bb_window_close.mean()
            std20 = bb_window_close.std(ddof=1)
            if not _is_finite_positive(sma20) or not _is_finite_positive(std20):
                continue
            lower_bb = sma20 - cfg.bb_std_mult * std20
            band_active = (close_asof < lower_bb)
            if not band_active:
                continue  # not below the volatility band — skip

            # RSI2
            rsi_value = _compute_rsi(close, cfg.rsi_period)
            if not _is_finite(rsi_value):
                continue
            if rsi_value >= cfg.rsi_threshold:
                continue  # not oversold enough

            # Score = z-score (Jeff F12=B), ascending
            z = float((close_asof - sma20) / std20)
            scores[pair] = z

        # Ascending: most negative (deepest oversold) first; pair-name
        # asc as tiebreaker.
        ranked = sorted(scores.items(), key=lambda x: (x[1], x[0]))
        picks = [pair for pair, _ in ranked[:top_n]]
        return sorted(picks)


def _compute_rsi(close: pd.Series, period: int) -> float:
    """Simple-mean RSI over the most recent ``period`` daily deltas.

    For period=2 (RSI2), this is the canonical short-term RSI used in
    Connors-style mean-reversion entries. Simple mean (not Wilder
    smoothing) is used to keep the value byte-stable across runs (G6).
    """
    if len(close) < period + 1:
        return float("nan")
    delta = close.diff().dropna()
    if len(delta) < period:
        return float("nan")
    last = delta.iloc[-period:]
    gains = last.clip(lower=0).mean()
    losses = (-last).clip(lower=0).mean()
    if losses == 0 and gains == 0:
        return 50.0  # no movement
    if losses == 0:
        return 100.0  # all gains
    rs = gains / losses
    return float(100.0 - 100.0 / (1.0 + rs))


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
