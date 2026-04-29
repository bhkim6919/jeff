"""Tests for us/notify/alert_dedup.py — alert throttle/dedup helpers.

Run from repo root:
    us/.venv/Scripts/python.exe -m pytest us/tests/test_alert_dedup.py -v
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

# Add us/ to sys.path so `notify.alert_dedup` resolves the same way as runtime.
US_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(US_ROOT))

from notify import alert_dedup  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_state():
    alert_dedup.reset_for_test()
    yield
    alert_dedup.reset_for_test()


# ── DD transition ────────────────────────────────────────────────

def test_dd_transition_normal_to_normal_no_fire():
    fired, prev, new = alert_dedup.dd_transition("NORMAL")
    assert not fired
    assert prev == "NORMAL"


def test_dd_transition_normal_to_blocked_fires():
    fired, prev, new = alert_dedup.dd_transition("DAILY_BLOCKED")
    assert fired
    assert prev == "NORMAL"
    assert new == "DAILY_BLOCKED"


def test_dd_transition_blocked_to_blocked_no_repeat():
    alert_dedup.dd_transition("DAILY_BLOCKED")
    fired, _, _ = alert_dedup.dd_transition("DAILY_BLOCKED")
    assert not fired


def test_dd_transition_recovery_fires():
    alert_dedup.dd_transition("DAILY_BLOCKED")
    fired, prev, new = alert_dedup.dd_transition("NORMAL")
    assert fired
    assert prev == "DAILY_BLOCKED"
    assert new == "NORMAL"


def test_dd_transition_blocked_to_blocked_different_level_fires():
    alert_dedup.dd_transition("DD_CAUTION")
    fired, prev, new = alert_dedup.dd_transition("DAILY_BLOCKED")
    assert fired
    assert prev == "DD_CAUTION"


# ── STALE summary ────────────────────────────────────────────────

def _mock_position(qty: int, last_price_at: str):
    return SimpleNamespace(quantity=qty, last_price_at=last_price_at)


def test_count_stale_positions_age_threshold():
    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(minutes=5)).isoformat()
    stale = (now - timedelta(hours=8)).isoformat()
    positions = {
        "AAA": _mock_position(10, fresh),
        "BBB": _mock_position(20, stale),
        "CCC": _mock_position(30, stale),
    }
    n, syms = alert_dedup.count_stale_positions(positions, now.isoformat())
    assert n == 2
    assert syms == ["BBB", "CCC"]


def test_count_stale_skips_zero_qty():
    now = datetime.now(timezone.utc)
    stale = (now - timedelta(hours=8)).isoformat()
    positions = {
        "AAA": _mock_position(0, stale),
        "BBB": _mock_position(10, stale),
    }
    n, syms = alert_dedup.count_stale_positions(positions, now.isoformat())
    assert n == 1
    assert syms == ["BBB"]


def test_count_stale_skips_empty_last_price_at():
    now = datetime.now(timezone.utc)
    positions = {
        "AAA": _mock_position(10, ""),
    }
    n, _ = alert_dedup.count_stale_positions(positions, now.isoformat())
    assert n == 0


def test_stale_should_fire_first_burst():
    assert alert_dedup.stale_should_fire(5) is True


def test_stale_should_fire_throttled_within_1h():
    alert_dedup.stale_should_fire(5)
    # Same-second second call must not fire again.
    assert alert_dedup.stale_should_fire(5) is False
    assert alert_dedup.stale_should_fire(7) is False


def test_stale_should_fire_after_throttle():
    alert_dedup.stale_should_fire(5)
    # Fast-forward by reaching into module state.
    alert_dedup._last_stale_alert_at = time.time() - alert_dedup.STALE_THROTTLE_SEC - 1
    assert alert_dedup.stale_should_fire(5) is True


def test_stale_should_fire_recovery_always_fires():
    alert_dedup.stale_should_fire(5)
    # Even right after firing, recovery should fire.
    assert alert_dedup.stale_should_fire(0) is True
    # Subsequent zero counts don't fire.
    assert alert_dedup.stale_should_fire(0) is False


def test_stale_no_fire_when_always_zero():
    assert alert_dedup.stale_should_fire(0) is False
    assert alert_dedup.stale_should_fire(0) is False


# ── Equity drop ──────────────────────────────────────────────────

def test_equity_drop_no_prior_no_fire():
    fired, _, _ = alert_dedup.equity_drop_should_fire(100_000)
    assert not fired


def test_equity_drop_under_threshold_no_fire():
    alert_dedup.equity_drop_should_fire(100_000)  # arm
    fired, _, _ = alert_dedup.equity_drop_should_fire(97_000)  # -3%
    assert not fired


def test_equity_drop_over_threshold_fires_once():
    alert_dedup.equity_drop_should_fire(100_000)  # arm
    fired1, prev, curr = alert_dedup.equity_drop_should_fire(94_000)  # -6%
    assert fired1
    assert prev == 100_000
    assert curr == 94_000
    # Continued drop must NOT re-fire (one-shot until recovery).
    fired2, _, _ = alert_dedup.equity_drop_should_fire(90_000)
    assert not fired2


def test_equity_drop_re_arms_on_recovery():
    alert_dedup.equity_drop_should_fire(100_000)
    alert_dedup.equity_drop_should_fire(94_000)  # fire
    alert_dedup.equity_drop_should_fire(95_000)  # tiny loss, still active
    # Recovery: equity stays roughly flat or rises — must re-arm.
    alert_dedup.equity_drop_should_fire(100_000)
    fired, _, _ = alert_dedup.equity_drop_should_fire(94_000)
    assert fired


# ── STARTUP_BLOCKED one-shot ─────────────────────────────────────

def test_startup_block_first_call_fires():
    assert alert_dedup.startup_block_should_fire() is True


def test_startup_block_only_once():
    alert_dedup.startup_block_should_fire()
    assert alert_dedup.startup_block_should_fire() is False
    assert alert_dedup.startup_block_should_fire() is False


# ── Reset helper ─────────────────────────────────────────────────

def test_reset_clears_state():
    alert_dedup.dd_transition("DAILY_BLOCKED")
    alert_dedup.stale_should_fire(5)
    alert_dedup.equity_drop_should_fire(100_000)
    alert_dedup.equity_drop_should_fire(80_000)
    alert_dedup.startup_block_should_fire()

    alert_dedup.reset_for_test()

    # After reset, transitions re-fire as if fresh.
    fired, prev, _ = alert_dedup.dd_transition("DAILY_BLOCKED")
    assert fired
    assert prev == "NORMAL"
    assert alert_dedup.startup_block_should_fire() is True
