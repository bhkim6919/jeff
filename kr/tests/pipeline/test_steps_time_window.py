# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.time_window.TimeWindow + StepBase gate."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

import pytest

from pipeline.schema import STATUS_DONE, STATUS_FAILED, STATUS_SKIPPED
from pipeline.state import PipelineState
from pipeline.steps.base import StepBase, StepRunResult
from pipeline.steps.time_window import TimeWindow


# ---------- TimeWindow.check direct tests ----------

def test_window_inside_returns_in_window():
    w = TimeWindow("Asia/Seoul", 16, 5, 3300)
    zi = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 4, 21, 16, 6, tzinfo=zi)  # Tue, 1 min past
    ok, reason = w.check(now)
    assert ok is True
    assert reason == "in_window"


def test_window_exact_start_included():
    w = TimeWindow("Asia/Seoul", 16, 5, 60)
    zi = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 4, 21, 16, 5, 0, tzinfo=zi)
    ok, reason = w.check(now)
    assert ok is True


def test_window_end_exclusive():
    w = TimeWindow("Asia/Seoul", 16, 5, 60)
    zi = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 4, 21, 16, 6, 0, tzinfo=zi)  # exactly 60s past
    ok, reason = w.check(now)
    assert ok is False
    assert "outside_window" in reason


def test_window_before_start_rejected():
    w = TimeWindow("Asia/Seoul", 16, 5, 60)
    zi = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 4, 21, 16, 4, 0, tzinfo=zi)  # 1 min early
    ok, reason = w.check(now)
    assert ok is False
    assert "outside_window" in reason
    assert "delta=-60s" in reason


def test_window_weekend_rejected():
    w = TimeWindow("Asia/Seoul", 16, 5, 3300)
    zi = ZoneInfo("Asia/Seoul")
    # 2026-04-25 is Saturday; still inside the clock window but weekday check
    # must dominate
    now = datetime(2026, 4, 25, 16, 10, tzinfo=zi)
    ok, reason = w.check(now)
    assert ok is False
    assert reason == "weekend"


def test_window_weekday_only_off_accepts_saturday():
    w = TimeWindow("Asia/Seoul", 16, 5, 3300, weekday_only=False)
    zi = ZoneInfo("Asia/Seoul")
    now = datetime(2026, 4, 25, 16, 10, tzinfo=zi)
    ok, _ = w.check(now)
    assert ok is True


def test_window_naive_datetime_interpreted_in_window_tz():
    w = TimeWindow("Asia/Seoul", 16, 5, 60)
    # Naive datetime: 16:05 with no tz; TimeWindow should treat as Asia/Seoul
    now = datetime(2026, 4, 21, 16, 5)
    ok, _ = w.check(now)
    assert ok is True


def test_window_aware_utc_converted_to_window_tz():
    # 07:06 UTC on 2026-04-21 == 16:06 KST (+09:00 offset)
    w = TimeWindow("Asia/Seoul", 16, 5, 3300)
    from datetime import timezone
    now = datetime(2026, 4, 21, 7, 6, tzinfo=timezone.utc)
    ok, reason = w.check(now)
    assert ok is True
    assert reason == "in_window"


def test_us_eastern_window_handles_dst():
    # 2026-07-15 is EDT (UTC-4). 16:05 ET = 20:05 UTC.
    w = TimeWindow("US/Eastern", 16, 5, 60)
    from datetime import timezone
    now_utc = datetime(2026, 7, 15, 20, 5, tzinfo=timezone.utc)  # Wed, 16:05 ET
    ok, reason = w.check(now_utc)
    assert ok is True
    assert reason == "in_window"


# ---------- StepBase integration ----------

class _FixedClock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, **kw):
        self.t = self.t + timedelta(**kw)


class _WindowedOkStep(StepBase):
    name = "windowed_ok"
    time_window = TimeWindow("Asia/Seoul", 16, 5, 60)

    def _execute(self, state):
        return StepRunResult(ok=True, details={"ran": True})


class _WindowedFailStep(StepBase):
    name = "windowed_fail"
    time_window = TimeWindow("Asia/Seoul", 16, 5, 60)

    def _execute(self, state):
        return StepRunResult(ok=False, error="boom")


def _make_state(tmp_path, *, clock=None):
    return PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )


def test_step_inside_window_runs(tmp_path):
    # Inject aware Asia/Seoul time at 16:05
    zi = ZoneInfo("Asia/Seoul")
    clock = _FixedClock(datetime(2026, 4, 21, 16, 5, tzinfo=zi))
    state = _make_state(tmp_path, clock=clock)
    step = _WindowedOkStep(clock=clock)

    result = step.run(state)

    assert result.ok is True
    assert state.step("windowed_ok").status == STATUS_DONE


def test_step_outside_window_skipped_not_failed(tmp_path):
    zi = ZoneInfo("Asia/Seoul")
    # 14:00 KST — an hour before the window
    clock = _FixedClock(datetime(2026, 4, 21, 14, 0, tzinfo=zi))
    state = _make_state(tmp_path, clock=clock)
    step = _WindowedOkStep(clock=clock)

    result = step.run(state)

    assert result.ok is False
    assert result.skipped is True
    assert result.error.startswith("outside_time_window:")
    # Must not mark the step as started — fail_count stays 0 so the
    # retry path doesn't later trigger abandon.
    st = state.step("windowed_ok")
    assert st.fail_count == 0
    assert st.status != STATUS_FAILED


def test_step_weekend_gate_blocks_before_backoff(tmp_path):
    zi = ZoneInfo("Asia/Seoul")
    # Saturday inside the clock window
    clock = _FixedClock(datetime(2026, 4, 25, 16, 10, tzinfo=zi))
    state = _make_state(tmp_path, clock=clock)
    step = _WindowedOkStep(clock=clock)

    result = step.run(state)

    assert result.skipped is True
    assert "weekend" in (result.error or "")


def test_step_retry_bypasses_window_after_first_failure(tmp_path):
    """On retry (fail_count >= 1) the window is ignored; backoff owns timing."""
    zi = ZoneInfo("Asia/Seoul")
    # Start inside the window — first attempt fails
    clock = _FixedClock(datetime(2026, 4, 21, 16, 5, 10, tzinfo=zi))
    state = _make_state(tmp_path, clock=clock)
    step = _WindowedFailStep(clock=clock)

    first = step.run(state)
    assert first.ok is False
    assert state.step("windowed_fail").fail_count == 1

    # Advance past backoff (default 300s) AND out of the window
    clock.advance(seconds=700)  # now 16:16:50 — past the 60s window
    retry = step.run(state)

    # Retry must NOT be blocked by the window — it proceeds and fails again
    assert retry.ok is False
    assert retry.error == "boom"
    assert state.step("windowed_fail").fail_count == 2


def test_step_no_window_unaffected(tmp_path):
    """A step with time_window=None is always eligible (legacy behavior)."""
    class _NoWindowStep(StepBase):
        name = "no_window"
        time_window = None

        def _execute(self, state):
            return StepRunResult(ok=True)

    clock = _FixedClock(datetime(2026, 4, 21, 3, 0))  # middle of the night
    state = _make_state(tmp_path, clock=clock)
    step = _NoWindowStep(clock=clock)

    result = step.run(state)
    assert result.ok is True


def test_step_instance_override_disables_class_window(tmp_path):
    """Passing time_window=None at construction disables the class-level window."""
    zi = ZoneInfo("Asia/Seoul")
    clock = _FixedClock(datetime(2026, 4, 21, 3, 0, tzinfo=zi))  # outside window
    state = _make_state(tmp_path, clock=clock)
    step = _WindowedOkStep(clock=clock, time_window=None)

    result = step.run(state)
    # Window disabled → step runs despite it being 3am
    assert result.ok is True


def test_step_default_now_uses_real_utc_when_no_clock():
    """_now() falls back to datetime.now(timezone.utc) when no clock provided."""
    from datetime import timezone
    step = _WindowedOkStep()
    now = step._now()
    assert now.tzinfo is not None
    assert now.tzinfo.utcoffset(now) == timezone.utc.utcoffset(now)
