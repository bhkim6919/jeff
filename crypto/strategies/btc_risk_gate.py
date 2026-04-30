"""BTC Risk Gate — D6 strategy filter (skeleton).

Concept (per d6_regime_switching_spec.md §4.1):
    BTC weekly EMA200 cross is a coarse macro filter. When BTC's weekly
    close drops below EMA200, alt-coin strategies face headwinds —
    breakouts fail more often, mean-reversions get steamrolled by
    persistent down-trends. The gate suppresses ALT buys during those
    regimes so D5 strategies stay defensive without any per-strategy
    rewrite.

Status:
    SKELETON — no engine integration yet. This module only computes
    the gate state. Wiring into the rebalance engine is a separate D6
    PR (Stage 2 in d6_regime_switching_spec.md §7).

Determinism contract:
    * is_active() reads OHLCV up to and including the resampled weekly
      close at ``asof`` — no lookahead.
    * Insufficient history → returns ``True`` (no gating) and logs a
      warning. Conservative default: do not block trades when the
      filter cannot decide.
    * The gate is pure: same inputs → same output. No I/O, no caching.

Limitations (out of scope for skeleton):
    * Backtest engine integration.
    * Live data fetch — caller must supply BTC OHLCV.
    * Multi-signal regime detection (vol cluster, ALT/BTC ratio) —
      separate detector module per spec §2.2.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


# Accepted asof types — caller may pass either a stdlib ``date``
# / ``datetime`` or a ``pd.Timestamp``. The gate normalises to a
# common form internally so the OHLCV index dtype (object-of-date
# vs DatetimeIndex) doesn't matter.
AsofLike = Union[date, datetime, pd.Timestamp]


def _normalize_index_and_asof(
    btc_ohlcv: pd.DataFrame, asof: AsofLike
) -> tuple[pd.DataFrame, pd.Timestamp]:
    """Coerce the OHLCV frame to a ``DatetimeIndex`` and the asof to
    a matching ``pd.Timestamp``.

    Pre-fix bug (Jeff 2026-04-30 D6 Stage 2 verifier first end-to-end run):
        ``OhlcvLoader.load_pair`` returns a frame whose index is
        ``object`` dtype with ``datetime.date`` elements. The gate
        was slicing it as ``btc_ohlcv.loc[:pd.Timestamp(asof)]``,
        which raises ``TypeError('Cannot compare Timestamp with
        datetime.date.')`` on every rebal — the engine logs a warning
        and safe-defaults the gate to ``True`` (no gating). The bug
        was silent because the Stage 1 smoke test only used always-on
        / always-off mock gates; the real ``BTCRiskGate`` was never
        exercised end-to-end before PR-2.

    Post-fix:
        * If the index is already a ``DatetimeIndex``, use it as-is.
        * Otherwise, ``pd.to_datetime`` the index — this works for
          object-dtype indexes containing ``date`` / ``datetime`` /
          ISO-string elements.
        * The asof becomes a ``pd.Timestamp`` aligned with the
          normalised index. ``date`` arguments map to midnight; that
          plus the ``loc[:asof]`` slice's right-inclusivity means a
          rebal on date D still sees the close of D in the slice.
    """
    if not isinstance(btc_ohlcv.index, pd.DatetimeIndex):
        btc_ohlcv = btc_ohlcv.copy()
        btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index)
    return btc_ohlcv, pd.Timestamp(asof)


@dataclass(frozen=True)
class BTCRiskGateConfig:
    """Filter parameters."""

    ema_period: int = 200
    timeframe: str = "1W"  # pandas resample rule: weekly close
    min_history_weeks: int = 200  # safety floor — must equal ema_period


class BTCRiskGate:
    """Coarse BTC trend filter for alt-strategy buy gating.

    Usage::

        gate = BTCRiskGate(BTCRiskGateConfig())
        if gate.is_active(btc_ohlcv_df, asof=trade_date):
            # ALT strategies allowed to buy
            pass
        else:
            # Skip buys this rebalance (sells still allowed by engine)
            pass
    """

    def __init__(self, config: Optional[BTCRiskGateConfig] = None):
        self.config = config or BTCRiskGateConfig()
        if self.config.ema_period < 1:
            raise ValueError(f"ema_period must be >= 1, got {self.config.ema_period}")
        if self.config.min_history_weeks < self.config.ema_period:
            raise ValueError(
                "min_history_weeks must be >= ema_period "
                f"({self.config.min_history_weeks} < {self.config.ema_period})"
            )

    def is_active(self, btc_ohlcv: pd.DataFrame, asof: AsofLike) -> bool:
        """True = BULL (alt buys allowed), False = BEAR (alt buys blocked).

        Args:
            btc_ohlcv: DataFrame indexed by date with at least a 'close'
                       column. Daily OHLCV data; weekly resample is
                       performed inside the gate. The index may be
                       either a ``DatetimeIndex`` or an object-dtype
                       index of ``datetime.date`` elements — both are
                       normalised internally.
            asof:      Signal date. May be ``datetime.date``,
                       ``datetime.datetime``, or ``pd.Timestamp``.
                       The gate reads weekly closes up to (and
                       including) the week containing ``asof``.

        Returns:
            bool. ``True`` when the BTC weekly close at ``asof`` is
            above the trailing EMA200, ``False`` otherwise. Insufficient
            history → ``True`` (safe default — no gating) + warning log.
        """
        if btc_ohlcv is None or btc_ohlcv.empty:
            logger.warning("[BTC_RISK_GATE] empty OHLCV — defaulting to active=True")
            return True
        if "close" not in btc_ohlcv.columns:
            raise KeyError("btc_ohlcv must contain 'close' column")

        # Normalise both the index and asof to a common ``DatetimeIndex``
        # / ``pd.Timestamp`` form. Pre-fix this was ``loc[:pd.Timestamp(
        # asof)]`` against an object-dtype-of-``date`` index which
        # raised TypeError every rebal.
        df, asof_ts = _normalize_index_and_asof(btc_ohlcv, asof)
        df = df.loc[:asof_ts].copy()
        if df.empty:
            logger.warning(
                f"[BTC_RISK_GATE] no data on or before {asof} — defaulting to active=True"
            )
            return True

        weekly = df["close"].resample(self.config.timeframe).last().dropna()
        if len(weekly) < self.config.min_history_weeks:
            logger.warning(
                f"[BTC_RISK_GATE] insufficient weekly history "
                f"({len(weekly)} < {self.config.min_history_weeks}) — "
                f"defaulting to active=True"
            )
            return True

        ema = weekly.ewm(span=self.config.ema_period, adjust=False).mean()
        latest_close = float(weekly.iloc[-1])
        latest_ema = float(ema.iloc[-1])

        active = latest_close > latest_ema
        logger.info(
            f"[BTC_RISK_GATE] asof={asof} close={latest_close:.2f} "
            f"ema{self.config.ema_period}={latest_ema:.2f} active={active}"
        )
        return active

    def diagnostic(self, btc_ohlcv: pd.DataFrame, asof: AsofLike) -> dict:
        """Return raw values for backtesting/debugging."""
        if btc_ohlcv is None or btc_ohlcv.empty:
            return {"active": True, "reason": "empty_ohlcv"}
        df, asof_ts = _normalize_index_and_asof(btc_ohlcv, asof)
        df = df.loc[:asof_ts]
        if df.empty:
            return {"active": True, "reason": "no_data_before_asof"}
        weekly = df["close"].resample(self.config.timeframe).last().dropna()
        if len(weekly) < self.config.min_history_weeks:
            return {
                "active": True,
                "reason": "insufficient_history",
                "weeks_available": len(weekly),
                "weeks_required": self.config.min_history_weeks,
            }
        ema = weekly.ewm(span=self.config.ema_period, adjust=False).mean()
        latest_close = float(weekly.iloc[-1])
        latest_ema = float(ema.iloc[-1])
        return {
            "active": latest_close > latest_ema,
            "reason": "computed",
            "asof": str(asof),
            "latest_weekly_close": round(latest_close, 4),
            "latest_ema": round(latest_ema, 4),
            "weeks_used": len(weekly),
        }
