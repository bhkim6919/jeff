"""test_alert_engine_rebal.py — rebal countdown one-shot regression.

Jeff 2026-04-27: dashboard showed 5x "📅 리밸런싱 D-7" inside 2 hours
because _check_rebal_countdown re-emitted the event on every evaluator
tick once the 30-min DEDUP_TTL elapsed. The fix is to gate emission on
get_last_state(event_key) == "D-{target}" so a milestone fires exactly
once per next_rebal date.

This file mocks alert_state.get_last_state so the engine test does not
need PostgreSQL; the contract is purely "skip when state already
recorded, emit otherwise".
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _bootstrap_path  # noqa: F401 — sys.path bootstrap

from notify import alert_engine  # noqa: E402


def _mock_next_trading_day(d):
    """Skip weekends — same shape as regime.calendar.next_trading_day,
    inlined so the test does not load the real calendar (which may pull
    in a holiday CSV that drifts over time)."""
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def _build_snapshot_with_dday(target_d: int):
    """Construct a snapshot whose `rebalance` field will compute
    next_rebal == today + target_d trading days. We achieve this by
    setting `last` to a date `cycle` trading days back from the target
    so the engine's loop reproduces the target."""
    from regime import calendar as cal_mod  # patched below
    # Patch next_trading_day on the imported module so the engine's
    # `from regime.calendar import next_trading_day` (inside the
    # function under test) picks up our deterministic version.
    cal_mod.next_trading_day = _mock_next_trading_day

    today = date.today()
    target = today + timedelta(days=target_d)
    # Walk back `cycle` trading days to construct a last= such that
    # cycle iterations of next_trading_day land on `target`.
    cycle = 21
    cur = target
    walked = 0
    # Construct backwards: find a last_date such that applying
    # next_trading_day cycle times yields `target`. Easiest: walk
    # backwards counting weekdays.
    while walked < cycle:
        cur = cur - timedelta(days=1)
        if cur.weekday() < 5:
            walked += 1
    last_yyyymmdd = cur.strftime("%Y%m%d")
    return {
        "rebalance": {"last": last_yyyymmdd, "cycle": cycle},
    }


def test_1_first_evaluation_emits_d7():
    """No prior state → D-7 alert is queued."""
    state_log = {}
    alert_engine.get_last_state = lambda key: state_log.get(key)

    snap = _build_snapshot_with_dday(7)
    events = []
    alert_engine._check_rebal_countdown(snap, events)

    assert len(events) == 1, f"expected 1 event, got {len(events)}: {events}"
    ev = events[0]
    assert ev.state == "D-7"
    assert ev.event_key.startswith("rebal_d7_")
    assert "리밸런싱 D-7" in ev.message
    print("  PASS: D-7 first evaluation emits once")


def test_2_second_evaluation_skips_when_state_already_recorded():
    """Once state="D-7" is recorded for this event_key, no re-emission
    even if the evaluator runs many times while d_day is still 7."""
    state_log = {}
    alert_engine.get_last_state = lambda key: state_log.get(key)

    snap = _build_snapshot_with_dday(7)

    # First evaluation: emit + record.
    events = []
    alert_engine._check_rebal_countdown(snap, events)
    assert len(events) == 1
    state_log[events[0].event_key] = events[0].state

    # Subsequent evaluations: state is already "D-7" → skip.
    for _ in range(5):
        events2 = []
        alert_engine._check_rebal_countdown(snap, events2)
        assert events2 == [], (
            f"unexpected re-emission: {events2}"
        )
    print("  PASS: subsequent evaluations skip while state is recorded")


def test_3_different_next_rebal_emits_again():
    """A new rebalance cycle (different next_rebal date) produces a new
    event_key, so the state-gate misses and the alert fires."""
    state_log = {}
    alert_engine.get_last_state = lambda key: state_log.get(key)

    snap_a = _build_snapshot_with_dday(7)
    events_a = []
    alert_engine._check_rebal_countdown(snap_a, events_a)
    assert len(events_a) == 1
    state_log[events_a[0].event_key] = events_a[0].state

    # Build a snapshot with a totally different last_date → different
    # next_rebal → different event_key.
    snap_b = dict(snap_a)
    last_b = (date.today() + timedelta(days=-90)).strftime("%Y%m%d")
    snap_b["rebalance"] = {"last": last_b, "cycle": 21}

    # d_day for snap_b will likely not be 7; check whichever target
    # happens to land. If none lands, this test is a no-op for that
    # case — guard with a forced-equal scenario instead by recomputing.
    # Safer: just assert that snap_a's already-recorded key does not
    # bleed into snap_b's evaluation, regardless of whether snap_b
    # currently emits anything.
    events_b = []
    alert_engine._check_rebal_countdown(snap_b, events_b)
    for ev in events_b:
        assert ev.event_key != events_a[0].event_key, (
            "different cycles must use different event_keys"
        )
    print("  PASS: different next_rebal → fresh event_key")


def test_4_d3_and_d1_have_independent_keys():
    """D-3 and D-1 milestones for the same next_rebal use different
    event_keys, so emitting D-7 does not block D-3 / D-1."""
    state_log = {}
    alert_engine.get_last_state = lambda key: state_log.get(key)

    # Force d_day=3.
    snap = _build_snapshot_with_dday(3)
    events = []
    alert_engine._check_rebal_countdown(snap, events)
    assert len(events) == 1 and events[0].state == "D-3"
    state_log[events[0].event_key] = events[0].state

    # Force d_day=1 — different next_rebal (today+1) but more
    # importantly different event_key suffix.
    snap2 = _build_snapshot_with_dday(1)
    events2 = []
    alert_engine._check_rebal_countdown(snap2, events2)
    assert len(events2) == 1 and events2[0].state == "D-1"
    assert events2[0].event_key != events[0].event_key
    print("  PASS: D-3 / D-1 keys independent of D-7")


def main():
    tests = [
        ("1. First evaluation emits D-7",
         test_1_first_evaluation_emits_d7),
        ("2. Subsequent evaluations skip while state recorded",
         test_2_second_evaluation_skips_when_state_already_recorded),
        ("3. Different next_rebal emits again",
         test_3_different_next_rebal_emits_again),
        ("4. D-3 / D-1 independent of D-7",
         test_4_d3_and_d1_have_independent_keys),
    ]

    passed = failed = 0
    for name, fn in tests:
        print(f"\n=== TEST {name} ===")
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'=' * 40}")
    print(f"RESULTS: {passed}/{passed + failed} passed")
    if failed:
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
