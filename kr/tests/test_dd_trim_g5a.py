"""Tests for PR 5 (G5-a): _execute_dd_trim — mark_trim_executed gating.

Memory G5-a (verified by audit 2026-05-04): mark_trim_executed was
unconditionally called when risk_action.get("trim_ratio", 0) > 0,
regardless of whether _execute_dd_trim actually trimmed any positions.
When all positions had qty * trim_ratio < 1 (no actual trades), the
guard still recorded "trim done" for the day, blocking same-day
re-attempts even though zero risk reduction had occurred.

This test verifies:
  - trimmed > 0 → mark_trim_executed CALLED
  - trimmed == 0 (qty too small) → mark_trim_executed NOT CALLED
  - Returns trimmed count
  - Logs [DD_TRIM_NO_FILL] when 0 trims with guard passed
  - Backward compat: legacy callers without guard arg still work

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_dd_trim_g5a.py -v
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

from risk.risk_management import _execute_dd_trim  # noqa: E402


def _make_position(qty=10, avg_price=50_000):
    return SimpleNamespace(quantity=qty, avg_price=avg_price)


def _make_portfolio(positions: dict):
    p = SimpleNamespace()
    p.positions = dict(positions)

    def remove_position(code, fill_price, sell_cost, qty):
        # Simulate full remove for simplicity in unit test
        if code in p.positions:
            del p.positions[code]
        return {"pnl_pct": 0.0, "code": code}

    p.remove_position = remove_position
    return p


def _make_executor(price=50_000, exec_qty_match=True):
    ex = MagicMock()
    ex.get_live_price.return_value = price
    ex.execute_sell.return_value = {
        "exec_price": price, "exec_qty": 0,  # set in test
        "error": None,
    }
    return ex


def _make_config():
    return SimpleNamespace(SELL_COST=0.00295)


@pytest.fixture
def trade_logger():
    return MagicMock()


@pytest.fixture
def lg():
    return logging.getLogger("test.dd_trim")


# ── trimmed > 0 → mark_trim_executed CALLED ───────────────────────────


def test_mark_trim_called_when_trims_happen(trade_logger, lg):
    portfolio = _make_portfolio({"005930": _make_position(qty=100)})
    executor = _make_executor(price=70_000)
    executor.execute_sell.return_value = {
        "exec_price": 70_000, "exec_qty": 20, "error": None,
    }
    guard = MagicMock()

    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg, guard=guard, level="DD_SEVERE",
    )

    assert trimmed == 1
    guard.mark_trim_executed.assert_called_once_with("DD_SEVERE")


# ── trimmed == 0 (qty too small) → mark_trim_executed NOT CALLED ──────


def test_mark_trim_not_called_when_qty_too_small(trade_logger, lg, caplog):
    """G5-a regression test: positions with qty=2 + trim_ratio=0.20
    yield qty_to_sell=int(2*0.20)=0, so loop continues without sell.
    Guard MUST NOT be marked."""
    portfolio = _make_portfolio({
        "A": _make_position(qty=2),
        "B": _make_position(qty=3),
    })
    executor = _make_executor()
    guard = MagicMock()

    caplog.set_level(logging.INFO)
    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg, guard=guard, level="DD_SEVERE",
    )

    assert trimmed == 0
    guard.mark_trim_executed.assert_not_called()
    # Should log explicit no-fill notice
    no_fill = [r for r in caplog.records if "[DD_TRIM_NO_FILL]" in r.message]
    assert len(no_fill) == 1
    assert "DD_SEVERE" in no_fill[0].message


def test_mark_trim_not_called_when_all_prices_fail(trade_logger, lg):
    portfolio = _make_portfolio({"005930": _make_position(qty=100)})
    executor = MagicMock()
    executor.get_live_price.return_value = 0  # price fail
    guard = MagicMock()

    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg, guard=guard, level="DD_SAFE_MODE",
    )

    assert trimmed == 0
    guard.mark_trim_executed.assert_not_called()


def test_mark_trim_not_called_when_sell_errors(trade_logger, lg):
    portfolio = _make_portfolio({"005930": _make_position(qty=100)})
    executor = _make_executor(price=70_000)
    executor.execute_sell.return_value = {"error": "BROKER_DOWN"}
    guard = MagicMock()

    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg, guard=guard, level="DD_SEVERE",
    )

    assert trimmed == 0
    guard.mark_trim_executed.assert_not_called()


# ── Backward compat: no guard arg ─────────────────────────────────────


def test_legacy_no_guard_arg_still_works(trade_logger, lg):
    """Legacy callers that don't pass guard should still execute trims.
    This protects against accidental signature breakage."""
    portfolio = _make_portfolio({"005930": _make_position(qty=100)})
    executor = _make_executor(price=70_000)
    executor.execute_sell.return_value = {
        "exec_price": 70_000, "exec_qty": 20, "error": None,
    }

    # Old call style — no guard, no level
    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg,
    )

    assert trimmed == 1
    # No guard → no mark_trim call attempted, no error


def test_partial_trim_marks_guard(trade_logger, lg):
    """Mixed: some positions trim, some skip (qty too small).
    Net trimmed > 0 → guard SHOULD be marked."""
    portfolio = _make_portfolio({
        "A": _make_position(qty=100),  # trims qty 20
        "B": _make_position(qty=2),    # qty_to_sell=0, skips
    })
    executor = _make_executor(price=70_000)
    executor.execute_sell.return_value = {
        "exec_price": 70_000, "exec_qty": 20, "error": None,
    }
    guard = MagicMock()

    trimmed = _execute_dd_trim(
        portfolio, trim_ratio=0.20, executor=executor,
        config=_make_config(), trade_logger=trade_logger, mode_str="LIVE",
        logger=lg, guard=guard, level="DD_SEVERE",
    )

    assert trimmed == 1  # only A trimmed
    guard.mark_trim_executed.assert_called_once_with("DD_SEVERE")
