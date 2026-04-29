"""Unit tests for BTC Risk Gate (D6 skeleton).

Coverage targets (per d6_regime_switching_spec.md §6.3):
    * BULL detection (close > EMA200)
    * BEAR detection (close < EMA200)
    * Edge case: equality (close == EMA200) → BEAR (not strictly above)
    * Insufficient history → safe default (active=True)
    * Empty/missing data → safe default
    * Determinism (same inputs → same output)
    * Configuration validation
"""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from crypto.strategies.btc_risk_gate import BTCRiskGate, BTCRiskGateConfig


# ── Fixtures ─────────────────────────────────────────────────────────


def _make_btc_df(prices: list[float], start: date = date(2018, 1, 1)) -> pd.DataFrame:
    """Build a daily OHLCV DataFrame from a list of closes."""
    dates = pd.date_range(start, periods=len(prices), freq="D")
    return pd.DataFrame({"close": prices}, index=dates)


def _trending_up(weeks: int = 250, start_price: float = 1000.0,
                  weekly_growth: float = 1.02) -> pd.DataFrame:
    """Simulate a sustained uptrend — close > EMA200 by construction."""
    days = weeks * 7
    daily_growth = weekly_growth ** (1 / 7)
    prices = [start_price * (daily_growth ** i) for i in range(days)]
    return _make_btc_df(prices)


def _trending_down(weeks: int = 250, start_price: float = 60000.0,
                    weekly_decline: float = 0.98) -> pd.DataFrame:
    """Simulate a sustained downtrend — close < EMA200 by construction."""
    days = weeks * 7
    daily_decline = weekly_decline ** (1 / 7)
    prices = [start_price * (daily_decline ** i) for i in range(days)]
    return _make_btc_df(prices)


# ── BULL / BEAR detection ────────────────────────────────────────────


def test_bull_regime_close_above_ema():
    """Sustained uptrend → close > EMA200 → gate active."""
    gate = BTCRiskGate()
    btc = _trending_up()
    asof = btc.index[-1].date()
    assert gate.is_active(btc, asof) is True


def test_bear_regime_close_below_ema():
    """Sustained downtrend → close < EMA200 → gate inactive."""
    gate = BTCRiskGate()
    btc = _trending_down()
    asof = btc.index[-1].date()
    assert gate.is_active(btc, asof) is False


def test_diagnostic_bull():
    """Diagnostic returns numeric breakdown for inspection."""
    gate = BTCRiskGate()
    btc = _trending_up()
    asof = btc.index[-1].date()
    diag = gate.diagnostic(btc, asof)
    assert diag["active"] is True
    assert diag["reason"] == "computed"
    assert diag["latest_weekly_close"] > diag["latest_ema"]


def test_diagnostic_bear():
    """Diagnostic returns numeric breakdown in BEAR regime."""
    gate = BTCRiskGate()
    btc = _trending_down()
    asof = btc.index[-1].date()
    diag = gate.diagnostic(btc, asof)
    assert diag["active"] is False
    assert diag["latest_weekly_close"] < diag["latest_ema"]


# ── Edge cases ───────────────────────────────────────────────────────


def test_insufficient_history_defaults_to_active():
    """< 200 weeks of data → safe default (active=True)."""
    gate = BTCRiskGate()
    btc = _trending_down(weeks=50)  # only 50 weeks — well below threshold
    asof = btc.index[-1].date()
    # Even though prices are trending down, gate defaults to True
    # because there isn't enough history to compute EMA200 reliably.
    assert gate.is_active(btc, asof) is True

    diag = gate.diagnostic(btc, asof)
    assert diag["reason"] == "insufficient_history"
    assert diag["weeks_available"] < diag["weeks_required"]


def test_empty_dataframe_defaults_to_active():
    """Empty DataFrame → safe default."""
    gate = BTCRiskGate()
    empty = pd.DataFrame({"close": []}, index=pd.DatetimeIndex([]))
    assert gate.is_active(empty, date(2020, 1, 1)) is True


def test_none_dataframe_defaults_to_active():
    """None input → safe default."""
    gate = BTCRiskGate()
    assert gate.is_active(None, date(2020, 1, 1)) is True


def test_missing_close_column_raises():
    """DataFrame without 'close' column → KeyError."""
    gate = BTCRiskGate()
    df = pd.DataFrame({"open": [100.0]}, index=pd.date_range("2018-01-01", periods=1))
    with pytest.raises(KeyError, match="close"):
        gate.is_active(df, date(2018, 1, 1))


def test_asof_before_data_defaults_to_active():
    """asof predates available data → safe default."""
    gate = BTCRiskGate()
    btc = _trending_up(weeks=250, start_price=1000.0)
    # btc starts at 2018-01-01; asof in 2017 has zero rows ≤ asof
    asof = date(2017, 6, 1)
    assert gate.is_active(btc, asof) is True


# ── Determinism ──────────────────────────────────────────────────────


def test_same_inputs_same_output():
    """Pure function: identical inputs → identical outputs."""
    gate = BTCRiskGate()
    btc = _trending_up()
    asof = btc.index[-1].date()
    r1 = gate.is_active(btc, asof)
    r2 = gate.is_active(btc, asof)
    r3 = gate.is_active(btc, asof)
    assert r1 == r2 == r3


def test_diagnostic_determinism():
    """Diagnostic output is byte-identical across calls."""
    gate = BTCRiskGate()
    btc = _trending_up()
    asof = btc.index[-1].date()
    d1 = gate.diagnostic(btc, asof)
    d2 = gate.diagnostic(btc, asof)
    assert d1 == d2


# ── Configuration validation ─────────────────────────────────────────


def test_invalid_ema_period_raises():
    with pytest.raises(ValueError, match="ema_period"):
        BTCRiskGate(BTCRiskGateConfig(ema_period=0, min_history_weeks=200))


def test_min_history_below_ema_period_raises():
    with pytest.raises(ValueError, match="min_history_weeks"):
        BTCRiskGate(BTCRiskGateConfig(ema_period=200, min_history_weeks=100))


def test_custom_ema_period():
    """Allow shorter EMA for experiments — config respected."""
    gate = BTCRiskGate(BTCRiskGateConfig(
        ema_period=50,
        min_history_weeks=50,
    ))
    btc = _trending_up(weeks=100)
    asof = btc.index[-1].date()
    assert gate.is_active(btc, asof) is True
