"""20-day Donchian breakout — D5 STEP 1 (Jeff F1=C / F2=A / F6=A).

Signal at signal date ``asof``:

    1. Channel breakout
       Close[asof]    >  max(High[asof-20 .. asof-1])
    2. Fresh-cross gate (avoid picking pairs already broken out)
       Close[asof-1]  <= max(High[asof-21 .. asof-2])
    3. SMA filter (regime — Jeff F1=C "+ SMA50 필터")
       Close[asof] > SMA50[asof]   OR   SMA20[asof] > SMA50[asof]

Only pairs satisfying all three are candidates.

Score (Jeff F2=A, breakout strength):
    score = (Close[asof] - channel_high) / channel_high
    Higher = a more decisive breakout above the prior 20-day ceiling.

Window:
    The breakout signal needs 21 days of high data (max over 20 days +
    one extra day for the cross gate). The SMA50 filter needs 50 days of
    close. Engine retreat must accommodate the longer of the two = 50.
    Per Jeff F6=A — no extra discretionary stabilization buffer beyond
    what the signal itself requires.

NaN handling (Jeff E5=A):
    Skip the pair THIS rebal if any of:
        - Insufficient data window
        - Channel high or prior-channel high non-finite/non-positive
        - Close at asof or asof-1 non-finite/non-positive
        - SMA fast/slow non-finite or SMA50 non-positive
        - Both SMA filter conditions fail (no regime green light)
        - Not a fresh cross (Close[asof-1] already above prior channel)
        - Close[asof] not above the channel (no breakout)
    Pairs return automatically next rebal once they re-qualify.

Tiebreaker: equal scores → pair name ascending (G6 determinism).

Exit policy (Jeff F3=A):
    No SL / trail / time-based exit inside the strategy. The engine's
    smart-rebal naturally exits at the next anchor when the cross signal
    no longer fires for that pair (the cross is fresh-only by design,
    so a held pair almost always exits at the next rebal).

Defaults (Jeff F1=C / F6=A — literature-canonical, NOT tuned):
    breakout_window = 20, sma_fast = 20, sma_slow = 50,
    lookback_days = 50, min_data_days = 45.

Per Jeff D5 STEP 1 lock: no parameter tuning; cost_model / universe /
engine all unchanged. SL/trail/time-based exits forbidden.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.strategies.base import Strategy


@dataclass(frozen=True)
class Donchian20DConfig:
    breakout_window: int = 20
    sma_fast: int = 20
    sma_slow: int = 50
    lookback_days: int = 50
    min_data_days: int = 45  # tolerance for the 50-day window


class Donchian20DBreakout(Strategy):
    """Top-N fresh 20-day breakouts above SMA50, ranked by breakout strength."""

    name = "donchian_20d"

    def __init__(self, config: Optional[Donchian20DConfig] = None) -> None:
        self.config = config or Donchian20DConfig()
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
        # Extra calendar buffer so the load window covers 50 trading days
        # plus the two-row tail used by the cross gate. Engine retreats by
        # ``lookback_days`` of calendar days; we add a small margin here.
        window_start = asof - timedelta(days=cfg.lookback_days + 5)

        scores: dict[str, float] = {}
        for pair in sorted(universe):
            df = loader.load_pair(pair, window_start, asof)
            if len(df) < cfg.min_data_days:
                continue

            close = df["close"].astype(float)
            high = df["high"].astype(float)

            if len(close) < cfg.sma_slow:
                continue  # SMA50 not computable
            if len(high) < cfg.breakout_window + 2:
                continue  # cross gate needs 22 high rows

            close_asof = close.iloc[-1]
            close_prev = close.iloc[-2]
            if not _is_finite_positive(close_asof) or not _is_finite_positive(close_prev):
                continue

            # Channel high windows (mathematical inclusive ranges):
            #   today:    High[asof-20 .. asof-1]  -> 20 rows ending at index -2
            #   prior:    High[asof-21 .. asof-2]  -> 20 rows ending at index -3
            # In iloc slicing (start inclusive, stop exclusive):
            #   today:  iloc[-21:-1]  (indices -21..-2 inclusive)
            #   prior:  iloc[-22:-2]  (indices -22..-3 inclusive)
            high_today_window = high.iloc[-21:-1]
            high_prior_window = high.iloc[-22:-2]
            if (
                len(high_today_window) < cfg.breakout_window
                or len(high_prior_window) < cfg.breakout_window
            ):
                continue
            channel_high = high_today_window.max()
            channel_high_prior = high_prior_window.max()
            if not _is_finite_positive(channel_high) or not _is_finite_positive(
                channel_high_prior
            ):
                continue

            # 1) Channel breakout TODAY
            if close_asof <= channel_high:
                continue
            # 2) Fresh-cross gate — yesterday was NOT already above prior channel
            if close_prev > channel_high_prior:
                continue

            # 3) SMA filter — Close > SMA50  OR  SMA20 > SMA50
            sma_fast_value = close.iloc[-cfg.sma_fast:].mean()
            sma_slow_value = close.iloc[-cfg.sma_slow:].mean()
            if not _is_finite(sma_fast_value) or not _is_finite_positive(sma_slow_value):
                continue
            regime_ok = (close_asof > sma_slow_value) or (sma_fast_value > sma_slow_value)
            if not regime_ok:
                continue

            scores[pair] = float((close_asof - channel_high) / channel_high)

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
