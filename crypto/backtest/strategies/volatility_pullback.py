"""[EXPERIMENTAL] Volatility Pullback — D5 STEP 3.

⚠️  EXPERIMENTAL TAG (Jeff F21=C, name + docstring)

This strategy is event-driven with a 2~4 day intended horizon, while the
D5 backtest engine runs on a 21-day rebal cycle (Jeff D5 STEP 1 F3=A
lock — no SL / TP / time-based exits). The structural mismatch means
observed alpha is NOT representative of the strategy's true edge:

  * Entries fire on a rare triple-condition signal at the rebal anchor;
  * The 2~4 day post-pullback bounce typically completes long before
    the next anchor 21 days later;
  * The position is therefore held through the post-event drift, often
    giving back the bounce gain and accumulating MDD.

STEP 3 verifies that the engine treats this kind of sparse, event-
driven strategy *deterministically* — same input → same output, no
crashes on rare-signal days, idempotent across re-runs. Returns are
not the metric of success here. See PR #23 description for the full
"engine boundary stress test" framing.

Entry conditions (Jeff F16~F18 — ALL three required):

  1. Volatility expansion (F16=A): within the recent 3-day window, the
     price range exceeded 1.8 × ATR20.
        recent_range = max(close[asof-3..asof]) - min(close[asof-3..asof])
        atr20        = mean(true_range over last 20 days, ending asof)
        cond1        = recent_range > 1.8 * atr20

  2. Pullback (F17=A): today's close pulled back at least 3% from the
     recent peak.
        recent_peak = max(High[asof-3..asof-1])
        cond2       = close[asof] < recent_peak * 0.97

  3. Bounce signal (F18=A): today's close above EMA5.
        ema5  = exponential moving average of close over 5 days
        cond3 = close[asof] > ema5[asof]

Score (F20=A — ATR multiple desc):
    score = recent_range / atr20  (higher = stronger expansion)
    Tiebreaker: pair name ascending (G6 determinism).

Lookback (Jeff 보완 — assertion guard):
    Required: max(20 + 3 ATR window, 5 EMA, 3 expansion) = 23 days.
    Configured: 30 calendar days for sparse-day buffer.
    Boundary assertion at module load time enforces this — see
    ``__post_init__`` style class-time check below.

NaN handling (Jeff E5=A):
    Skip pair THIS rebal if any of:
        - Insufficient history (< min_data_days within window)
        - ATR20 non-finite or zero (no volatility signal)
        - close[asof] non-finite or non-positive
        - EMA5 non-finite
        - recent_peak / recent_range non-finite

Determinism (G6):
    EMA5 is computed manually with SMA-as-initial-value (no Wilder
    smoothing, no pandas.ewm) so two runs over the same data return
    byte-identical floats — independent of any third-party library's
    initial-value convention.

Per Jeff D5 STEP 3 lock:
    - Defaults are Jeff spec verbatim. NO parameter tuning.
    - cost_model / universe untouched (KRWStaticTop100 shared).
    - No SL / TP / time-based exit (rebal-cycle exit only).
    - No BTC Risk Gate, no Regime Switching.
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
class VolatilityPullbackConfig:
    atr_window: int = 20
    expansion_window: int = 3
    expansion_multiple: float = 1.8
    pullback_threshold: float = 0.97  # close < peak * 0.97  (3% pullback)
    ema_span: int = 5
    lookback_days: int = 30
    min_data_days: int = 25  # tolerance for the ATR20 / EMA5 / pullback windows


# Boundary assertion (Jeff 보완): required lookback >= 23 to safely
# cover ATR20+3, EMA5, expansion 3-day window. Fires at module import
# if a future config change drops below the safe minimum.
_REQUIRED_MIN_LOOKBACK = 23
assert VolatilityPullbackConfig.__dataclass_fields__["lookback_days"].default >= _REQUIRED_MIN_LOOKBACK, (
    f"VolatilityPullbackConfig.lookback_days "
    f"({VolatilityPullbackConfig.__dataclass_fields__['lookback_days'].default}) "
    f"must be >= {_REQUIRED_MIN_LOOKBACK} to safely cover ATR20+3, EMA5, "
    f"expansion 3-day windows simultaneously."
)


class VolatilityPullbackExperimental(Strategy):
    """[EXPERIMENTAL] Pullback re-entry after a volatility expansion event.

    NAME tag (F21=C): ``volatility_pullback_experimental`` — the
    ``_experimental`` suffix is intentional and propagates into
    canonical hashes, run_ids, and dashboard pickers so this strategy
    can never be confused with a production candidate.
    """

    name = "volatility_pullback_experimental"

    def __init__(self, config: Optional[VolatilityPullbackConfig] = None) -> None:
        self.config = config or VolatilityPullbackConfig()
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
        # Calendar-day buffer; engine retreats start by ``lookback_days``
        # but per-pair listings may begin later — caught by min_data_days.
        window_start = asof - timedelta(days=cfg.lookback_days + 5)

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

            # ATR20 — true range over the last 20 days, simple mean
            # (deterministic; no Wilder smoothing — matches the
            # ATR Breakout strategy's approach).
            atr_value = _compute_atr(high, low, close, cfg.atr_window)
            if not _is_finite_positive(atr_value):
                continue

            # Cond 1: Volatility expansion in the last 3 days
            #   recent_range > 1.8 * ATR20
            if len(close) < cfg.expansion_window:
                continue
            recent_window = close.iloc[-cfg.expansion_window:]
            recent_max = float(recent_window.max())
            recent_min = float(recent_window.min())
            if not (_is_finite_positive(recent_max) and _is_finite_positive(recent_min)):
                continue
            recent_range = recent_max - recent_min
            expansion_threshold = cfg.expansion_multiple * atr_value
            if recent_range <= expansion_threshold:
                continue

            # Cond 2: Pullback — close < recent peak * (1 - pullback_pct)
            #   recent_peak = max(High over last ``expansion_window`` days,
            #                     EXCLUDING today — peak first, then pullback).
            if len(high) < cfg.expansion_window + 1:
                continue
            peak_window = high.iloc[-cfg.expansion_window - 1:-1]
            recent_peak = float(peak_window.max())
            if not _is_finite_positive(recent_peak):
                continue
            if close_asof >= recent_peak * cfg.pullback_threshold:
                continue  # not pulled back enough

            # Cond 3: Bounce signal — Close > EMA5
            ema5_value = _compute_ema_simple(close, cfg.ema_span)
            if not _is_finite(ema5_value):
                continue
            if close_asof <= ema5_value:
                continue  # not bouncing yet

            # Score: expansion magnitude / ATR (F20=A)
            score = float(recent_range / atr_value)
            scores[pair] = score

        # Debug log (Jeff 보완) — sparse-day analysis
        try:
            import logging
            _logger = logging.getLogger("crypto.backtest.strategies.volatility_pullback")
            _logger.debug(
                f"[VOL_PULLBACK] picks={len(scores)} at {asof} "
                f"(universe={len(universe)})"
            )
        except Exception:
            pass

        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        picks = [pair for pair, _ in ranked[:top_n]]
        return sorted(picks)


def _compute_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    window: int,
) -> float:
    """Simple-mean ATR over the last ``window`` true-range values.

    Identical methodology to the ATR Breakout strategy so two strategies
    that both reference ATR see byte-identical numbers — important for
    cross-strategy idempotency proofs in G13/G14.
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
    return float(tr.iloc[-window:].mean())


def _compute_ema_simple(values: pd.Series, span: int) -> float:
    """Manually-computed EMA using SMA as the initial seed.

    Avoids the third-party-library coupling we'd inherit from
    ``pandas.Series.ewm()`` (whose initial-value convention has
    historically varied by version). Same input → same output → safe
    for G6-style idempotency proofs.
    """
    if len(values) < span:
        return float("nan")
    series = values.astype(float).tolist()
    # SMA over the first ``span`` observations seeds the EMA.
    if not all(_is_finite(v) for v in series[:span]):
        return float("nan")
    seed = sum(series[:span]) / span
    if span >= len(series):
        return seed
    alpha = 2.0 / (span + 1.0)
    ema = seed
    for v in series[span:]:
        if not _is_finite(v):
            return float("nan")
        ema = alpha * v + (1.0 - alpha) * ema
    return ema


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
