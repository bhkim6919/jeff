"""Regression tests for the stale-recovery refactor of update_prices.

Pre-fix bug (Jeff 2026-04-30 P0):
    ``update_prices`` had a one-way stale latch. Once a position's
    ``last_price_at`` fell behind ``STALE_PRICE_MAX_GAP`` (600s) the
    next update was rejected, which meant ``last_price_at`` was never
    advanced, which meant the next update was also rejected. Live
    evidence: 16 of 20 US positions stuck at the 2026-04-21 ET close
    timestamp for 7+ trading days. Engine equity drifted ~$1.8k
    below broker truth and the trail-stop machinery never saw real
    drawdowns.

Post-fix semantics:
    A long gap is now treated as a *recovery event*. The new price
    is accepted; jump-guard tolerance is one-shot widened to 100%
    (still rejects obvious typos) to absorb the multi-day cumulative
    move; an ``[STALE_RECOVERY]`` warning is logged so it correlates
    with the ``[STALE]`` summary alert from PR #35.

These tests pin both the recovery path and the unchanged steady-state
path. Run from repo root::

    .venv64/Scripts/python.exe -m pytest us/tests/test_stale_guard_recovery.py -v
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

US_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(US_ROOT))

from core.portfolio_manager import (  # noqa: E402
    PortfolioManagerUS,
    USPosition,
    JUMP_GUARD_PREV_RATIO,
    JUMP_GUARD_HWM_RATIO,
    STALE_PRICE_MAX_GAP,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _hours_ago_iso(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")


def _make_pos(sym: str, qty: int, avg: float,
              current: float = 0.0, hwm: float = 0.0,
              last_at: str = "") -> USPosition:
    pos = USPosition(
        symbol=sym, quantity=qty, avg_price=avg,
        entry_date="2026-04-01", high_watermark=hwm or current,
    )
    pos.current_price = current
    pos.last_price_at = last_at
    return pos


def _make_pm() -> PortfolioManagerUS:
    return PortfolioManagerUS(cash=100_000.0)


# ── Steady-state (no regressions) ───────────────────────────────

def test_first_ever_update_accepted():
    """No prior last_price_at → not stale → standard path."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos("INTC", 78, 80.0)
    pm.update_prices({"INTC": 90.0}, _now_iso())
    assert pm.positions["INTC"].current_price == 90.0


def test_normal_update_within_window_accepted():
    """5-minute gap is well under STALE threshold — standard path."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=90.0, hwm=90.0,
        last_at=_hours_ago_iso(5 / 60),
    )
    pm.update_prices({"INTC": 91.0}, _now_iso())
    assert pm.positions["INTC"].current_price == 91.0


def test_jump_prev_rejected_in_steady_state():
    """+30% jump within steady state still rejected by 25% guard."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=80.0, hwm=80.0,
        last_at=_hours_ago_iso(5 / 60),
    )
    pm.update_prices({"INTC": 105.0}, _now_iso())  # +31% from 80
    # Rejected → still 80
    assert pm.positions["INTC"].current_price == 80.0


def test_jump_hwm_rejected_in_steady_state():
    """+50% from HWM rejected by 30% HWM guard in steady state."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=82.0, hwm=100.0,
        last_at=_hours_ago_iso(5 / 60),
    )
    pm.update_prices({"INTC": 150.0}, _now_iso())  # +50% from HWM 100
    assert pm.positions["INTC"].current_price == 82.0  # rejected


# ── Stale recovery (the actual fix) ─────────────────────────────

def test_stale_recovery_breaks_old_lockout():
    """Pre-fix: stuck at $66.16 for 8 days while broker quoted $91.
    Post-fix: recovery accepts the new price."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=66.16, hwm=66.16,
        last_at=_hours_ago_iso(186.5),  # 7.77 days — exact reproduction
    )
    pm.update_prices({"INTC": 91.03}, _now_iso())
    # +37.58% jump rejected pre-fix; recovery widens the cap to 100%.
    assert pm.positions["INTC"].current_price == 91.03
    # last_price_at advanced — the lockout is broken.
    assert pm.positions["INTC"].last_price_at != ""


def test_stale_recovery_advances_hwm_when_price_higher():
    """HWM monotonically increases — stale recovery should advance it."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=66.16, hwm=66.16,
        last_at=_hours_ago_iso(186.5),
    )
    pm.update_prices({"INTC": 91.03}, _now_iso())
    assert pm.positions["INTC"].high_watermark == 91.03


def test_stale_recovery_holds_hwm_when_price_lower():
    """HWM monotonically increases — never retracts on recovery."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=120.0, hwm=120.0,
        last_at=_hours_ago_iso(186.5),
    )
    pm.update_prices({"INTC": 91.03}, _now_iso())
    assert pm.positions["INTC"].high_watermark == 120.0
    # And drawdown is correctly measured against the held HWM.
    expected_dd = (91.03 / 120.0 - 1) * 100
    assert pm.positions["INTC"].drawdown_pct == pytest.approx(expected_dd, abs=0.01)


def test_stale_recovery_typo_still_rejected():
    """Recovery widens guard to 100% — but a 10x typo is still rejected."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=66.16, hwm=66.16,
        last_at=_hours_ago_iso(186.5),
    )
    pm.update_prices({"INTC": 661.60}, _now_iso())  # 10x typo
    # Rejected by widened-but-still-bounded jump guard.
    assert pm.positions["INTC"].current_price == 66.16


def test_stale_recovery_zero_typo_still_rejected():
    """price <= 0 always rejected, recovery or not."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=66.16, hwm=66.16,
        last_at=_hours_ago_iso(186.5),
    )
    pm.update_prices({"INTC": 0.0}, _now_iso())
    assert pm.positions["INTC"].current_price == 66.16


def test_recovery_is_one_shot_not_persistent_relaxation():
    """After the FIRST recovery update, subsequent updates run with
    the strict 25% jump guard — recovery does NOT permanently widen
    the tolerance."""
    pm = _make_pm()
    pm.positions["INTC"] = _make_pos(
        "INTC", 78, 80.0, current=66.16, hwm=66.16,
        last_at=_hours_ago_iso(186.5),
    )
    # First update — recovery, widened cap, accepted at +37%.
    pm.update_prices({"INTC": 91.03}, _now_iso())
    assert pm.positions["INTC"].current_price == 91.03
    # Second update — within steady state, +30% from $91 should
    # be rejected by the standard 25% jump guard.
    pm.update_prices({"INTC": 118.34}, _now_iso())  # +30%
    assert pm.positions["INTC"].current_price == 91.03  # rejected


# ── Trail-stop interaction (the load-bearing case) ──────────────

def test_trail_stop_fires_on_stale_recovery_when_real_drop_exceeds_trail():
    """The whole point of this fix: positions that dropped >12% in
    real markets while engine prices were frozen MUST trigger trail
    stops the moment the lockout breaks. This is *correct* behaviour
    and the operator alert (PR #35) gives them advance warning."""
    pm = _make_pm()
    # Position cached at $100 with HWM $100, locked out for 8 days.
    pm.positions["XYZ"] = _make_pos(
        "XYZ", 50, 90.0, current=100.0, hwm=100.0,
        last_at=_hours_ago_iso(186.5),
    )
    # Real broker quote: $86 (-14% from HWM). Trail stop = $100*0.88 = $88.
    pm.update_prices({"XYZ": 86.0}, _now_iso())
    pos = pm.positions["XYZ"]
    assert pos.current_price == 86.0
    assert pos.high_watermark == 100.0           # held
    assert pos.trail_stop_price == pytest.approx(88.0, abs=0.01)
    # check_trail_stops should now flag this position.
    triggered, _ = pm.check_trail_stops()
    assert "XYZ" in triggered


def test_trail_stop_does_not_fire_on_stale_recovery_within_trail():
    """Symmetric case: position dropped only 9% — within trail's
    -12% tolerance. No SELL fires."""
    pm = _make_pm()
    pm.positions["GLW"] = _make_pos(
        "GLW", 29, 130.0, current=164.55, hwm=164.55,
        last_at=_hours_ago_iso(186.5),
    )
    pm.update_prices({"GLW": 150.62}, _now_iso())  # -8.46%
    pos = pm.positions["GLW"]
    assert pos.current_price == 150.62
    assert pos.trail_stop_price == pytest.approx(164.55 * 0.88, abs=0.01)
    triggered, _ = pm.check_trail_stops()
    assert "GLW" not in triggered


# ── DD calculation correctness ──────────────────────────────────

def test_dd_computed_against_held_hwm_not_stale_price():
    """get_equity / DD calculations use post-recovery prices, so
    DD reflects real drawdown, not the stale-cache phantom."""
    pm = _make_pm()
    pm.positions["A"] = _make_pos("A", 100, 50.0, current=100.0, hwm=100.0,
                                   last_at=_hours_ago_iso(186.5))
    pm.positions["B"] = _make_pos("B", 100, 50.0, current=100.0, hwm=100.0,
                                   last_at=_hours_ago_iso(186.5))
    # Real prices: A held flat, B dropped -10%.
    pm.update_prices({"A": 100.0, "B": 90.0}, _now_iso())
    # Equity = cash + 100*100 + 100*90 = 100k + 10k + 9k = 119k.
    assert pm.get_equity() == pytest.approx(119_000.0)


# ── Sanity: no behaviour drift on cold start ────────────────────

def test_first_update_no_stale_recovery_path():
    """A position with empty last_price_at must NOT trigger the
    recovery-path widened jump guard — because there's no prior price
    to anchor against, the standard guards are sufficient."""
    pm = _make_pm()
    pm.positions["NEW"] = _make_pos("NEW", 10, 50.0)  # cold — no last_at
    pm.update_prices({"NEW": 50.0}, _now_iso())
    assert pm.positions["NEW"].current_price == 50.0
    # With empty last_at, _is_stale returns False, so jump guard runs
    # at the strict ratio. 50 → 50 obviously fine.
    pm.update_prices({"NEW": 200.0}, _now_iso())  # +300%
    assert pm.positions["NEW"].current_price == 50.0  # rejected by strict guard
