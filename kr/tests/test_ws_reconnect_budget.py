"""
test_ws_reconnect_budget.py — KiwoomWebSocket reconnect budget
================================================================

Pins the two-tier reconnect budget introduced after the 2026-04-29
15:20:23 KST incident (WS hit MAX_RECONNECT=5 mid-session and flipped
to REST fallback; Surge tick_count froze and Lab realtime ticks
dropped to 0 for the rest of the day).

Market hours (KST 08:00~16:00) → MAX_RECONNECT_MARKET_HOURS = 60
                                  (~5 minutes of retries at 5s each)
Off hours                       → MAX_RECONNECT_OFF_HOURS = 5
                                  (original tight budget; no liquidity
                                  to chase after 16:00 KST anyway)

The decision happens inside ``_run_reconnect_loop``'s while-body, so
unit tests pin the *constants and policy*, plus a static check that
the loop reads the budget through the new conditional rather than
the old fixed ``MAX_RECONNECT`` alone.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_market_hours_budget_is_generous():
    """KST 08:00~16:00 budget must allow at least ~5 minutes of
    retries at the existing 5s delay so a transient API hiccup
    doesn't lose the trading session."""
    from data.rest_websocket import (
        MAX_RECONNECT_MARKET_HOURS,
        RECONNECT_DELAY,
    )
    minutes_of_retry = MAX_RECONNECT_MARKET_HOURS * RECONNECT_DELAY / 60
    assert minutes_of_retry >= 5.0, (
        f"market-hours budget too tight: only ~{minutes_of_retry:.1f} "
        f"minutes of retry. The 2026-04-29 incident burned through "
        f"5 retries in <30s and lost the rest of the trading day."
    )


def test_off_hours_budget_is_tight():
    """Off-hours budget should NOT be infinite — an idle WS shouldn't
    retry forever when there's no liquidity to chase. The original
    5-attempt budget stands here."""
    from data.rest_websocket import MAX_RECONNECT_OFF_HOURS
    assert 0 < MAX_RECONNECT_OFF_HOURS <= 10, (
        f"off-hours budget should be small: {MAX_RECONNECT_OFF_HOURS}"
    )


def test_legacy_max_reconnect_alias_present():
    """External callers may import the legacy ``MAX_RECONNECT`` name.
    Keep it pointing at the market-hours budget for backwards
    compatibility — the actual decision uses the conditional in the
    loop itself."""
    from data.rest_websocket import MAX_RECONNECT, MAX_RECONNECT_MARKET_HOURS
    assert MAX_RECONNECT == MAX_RECONNECT_MARKET_HOURS, (
        "legacy MAX_RECONNECT alias drifted from the market-hours "
        "budget — external readers will see stale numbers."
    )


def test_reconnect_loop_reads_budget_through_conditional():
    """Static guard: the reconnect loop must use the new conditional
    (``budget = MAX_RECONNECT_MARKET_HOURS if _within_market else
    MAX_RECONNECT_OFF_HOURS``) rather than the old fixed
    ``MAX_RECONNECT``. A sloppy revert of the conditional would
    silently restore the 5-attempt cliff that bit us 04-29."""
    src = (
        Path(__file__).resolve().parent.parent
        / "data" / "rest_websocket.py"
    ).read_text(encoding="utf-8")
    # Conditional present
    assert "MAX_RECONNECT_MARKET_HOURS if _within_market" in src, (
        "two-tier conditional missing — reconnect budget reverted "
        "to the single-value cliff."
    )
    # Comparison uses the per-call `budget`, not the constant
    assert "self._reconnect_count > budget" in src, (
        "reconnect comparison must use per-call ``budget``, not "
        "the legacy fixed constant."
    )


def test_reconnect_log_includes_market_hours_flag():
    """The reconnect WARNING and the FALLBACK error log must include
    the ``market_hours`` flag so a postmortem can tell at a glance
    whether the budget that bit us was the tight or generous one."""
    src = (
        Path(__file__).resolve().parent.parent
        / "data" / "rest_websocket.py"
    ).read_text(encoding="utf-8")
    assert "market_hours={_within_market}" in src, (
        "reconnect log should expose market_hours flag for triage."
    )
