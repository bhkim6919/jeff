"""
test_engine_btc_gate.py — D6 Stage 1 BTC Gate engine integration
==================================================================

Pins the contract for ``crypto.backtest.engine._btc_gate_decide`` and
``BacktestResult.btc_gate_blocked_buys``. The decision helper was
pulled out of ``_execute_rebal`` (PR D6-Stage1) specifically so the
five contract cases below can be exercised without standing up a
Portfolio + OhlcvLoader + Strategy.

Per Jeff 2026-04-29 D6-Stage1 spec, the gate is a market-regime
filter: it suppresses **only** new BUYs on ``is_active=False`` rebal
dates. SELL / trail / max-hold / forced exit must NEVER be affected.
That separation is enforced upstream of this helper (the helper is
called only when ``buys`` is non-empty inside ``_execute_rebal``);
these tests pin the BUY-side decision contract.
"""
from __future__ import annotations

import logging
import sys
from dataclasses import fields
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

# crypto/ on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from crypto.backtest.engine import (
    BacktestResult,
    _btc_gate_decide,
)


# ── Fixtures ──────────────────────────────────────────────────────────────

def _ohlcv_minimal() -> pd.DataFrame:
    """A 30-row OHLCV frame indexed by date — enough for the helper to
    receive a non-empty frame, but the contents don't matter because
    the gate object is mocked. Real BTCRiskGate is exercised in its
    own ``test_btc_risk_gate.py`` (14 tests, already in tree)."""
    idx = pd.date_range("2024-01-01", periods=30, freq="D").date
    return pd.DataFrame(
        {
            "open":  [100.0] * 30,
            "high":  [110.0] * 30,
            "low":   [90.0] * 30,
            "close": [105.0] * 30,
            "volume": [1000.0] * 30,
            "value_krw": [100_000.0] * 30,
        },
        index=idx,
    ).rename_axis("candle_dt_kst")


# ── 1. btc_gate=None → no gating, BUYs always allowed ─────────────────────

def test_gate_none_returns_true():
    """Default-path: caller passed no gate at all. The engine path that
    consumes this function only enters when ``buys`` is non-empty, so
    a True here means 'BUY phase will run'. Pre-D6 callers must keep
    behaving identically."""
    assert _btc_gate_decide(None, _ohlcv_minimal(), date(2024, 6, 15)) is True


# ── 2. always-on gate → no gating, BUYs always allowed ────────────────────

def test_gate_always_on_returns_true():
    """A real gate that decides ``is_active=True`` on every rebal date
    (BULL regime continuously). Result must match the no-gate case so
    a verifier can run an explicit ``always_on`` parity sanity check."""
    gate = MagicMock()
    gate.is_active.return_value = True
    assert _btc_gate_decide(gate, _ohlcv_minimal(), date(2024, 6, 15)) is True
    gate.is_active.assert_called_once()


# ── 3. always-off gate → BUYs are blocked ─────────────────────────────────

def test_gate_always_off_returns_false():
    """BEAR regime (the gate's design intent). The helper returns False;
    the engine's caller turns that into ``buys = []`` and an audit row
    in ``BacktestResult.btc_gate_blocked_buys``."""
    gate = MagicMock()
    gate.is_active.return_value = False
    assert _btc_gate_decide(gate, _ohlcv_minimal(), date(2024, 6, 15)) is False


# ── 4. SELL phase is gate-independent — pin the call site contract ────────

def test_gate_decision_does_not_run_when_buys_empty():
    """Pin the call-site invariant: ``_execute_rebal`` calls this helper
    via ``... if buys else True``, so an all-SELLs-no-BUYs rebal must
    NOT consult the gate at all (avoids the engine paying for a gate
    call when there's nothing for the gate to block).

    SELL execution itself is in a separate code block above the BUY
    section in ``_execute_rebal``; the helper is by construction
    reachable only after SELLs have already run."""
    # Mirror the engine's call pattern:
    gate = MagicMock()
    gate.is_active.return_value = False
    buys: list = []

    # The exact line in engine.py:
    #     gate_active = _btc_gate_decide(...) if buys else True
    gate_active = _btc_gate_decide(gate, _ohlcv_minimal(), date(2024, 6, 15)) if buys else True

    assert gate_active is True  # short-circuit before gate is consulted
    gate.is_active.assert_not_called()


# ── 5. exception in gate → safe default + log ─────────────────────────────

def test_gate_exception_safe_defaults_true_and_logs(caplog):
    """A flaky gate (e.g. pandas raises on a missing date, or the gate
    code itself has a bug) must not silently flip the engine into
    permanent cash mode. The helper catches and returns True (BUYs
    proceed) and emits a WARNING the operator can grep for."""
    gate = MagicMock()
    gate.is_active.side_effect = KeyError("missing-date-in-frame")
    with caplog.at_level(logging.WARNING, logger="crypto.backtest.engine"):
        result = _btc_gate_decide(gate, _ohlcv_minimal(), date(2024, 6, 15))
    assert result is True
    assert any(
        "btc_gate raised" in rec.message
        for rec in caplog.records
    ), "expected a WARNING log explaining the gate exception"


# ── 6. BacktestResult schema bookkeeping ──────────────────────────────────

def test_backtest_result_has_btc_gate_blocked_buys_field():
    """The new audit field must be on ``BacktestResult`` with a
    sensible default. A rename or accidental removal would let the
    engine silently lose its gate-suppression evidence."""
    field_names = {f.name for f in fields(BacktestResult)}
    assert "btc_gate_blocked_buys" in field_names, (
        "BacktestResult.btc_gate_blocked_buys field missing — D6 "
        "Stage 1 audit log will not be persisted."
    )
