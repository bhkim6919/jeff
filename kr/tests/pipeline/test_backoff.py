# -*- coding: utf-8 -*-
"""Unit tests for pipeline.backoff.BackoffTracker."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.backoff import BackoffTracker
from pipeline.state import PipelineState


class MockClock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs):
        self.now += timedelta(**kwargs)


def _fresh_state(tmp_path: Path, clock) -> PipelineState:
    return PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )


def test_initial_call_allowed(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", clock=clock)
    allowed, reason = t.can_run_now(state)
    assert allowed is True
    assert reason == "ok"


def test_blocked_during_backoff(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", min_wait_sec=300, clock=clock)

    t.record_fail(state, "boom")
    clock.advance(seconds=60)  # under 300s window
    allowed, reason = t.can_run_now(state)
    assert allowed is False
    assert reason == "backoff"


def test_allowed_after_backoff_window(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", min_wait_sec=300, clock=clock)

    t.record_fail(state, "boom")
    clock.advance(seconds=301)
    allowed, reason = t.can_run_now(state)
    assert allowed is True
    assert reason == "ok"


def test_abandoned_after_max_fails(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", min_wait_sec=0, max_fails=3, clock=clock)

    for _ in range(3):
        t.record_fail(state, "boom")

    allowed, reason = t.can_run_now(state)
    assert allowed is False
    assert reason == "abandoned"

    # Even after long wait, abandoned stays sticky
    clock.advance(hours=6)
    allowed, reason = t.can_run_now(state)
    assert allowed is False
    assert reason == "abandoned"


def test_record_success_marks_done(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", clock=clock)

    t.record_success(state, details={"target_count": 20})
    allowed, reason = t.can_run_now(state)
    assert allowed is False
    assert reason == "already_done"
    assert state.step("batch").details == {"target_count": 20}


def test_reset_clears_fail_count(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("batch", min_wait_sec=0, max_fails=2, clock=clock)

    t.record_fail(state, "a")
    t.record_fail(state, "b")
    assert t.can_run_now(state)[1] == "abandoned"

    t.reset(state)
    # Status is still FAILED (only fail_count/error cleared), but the
    # abandon gate is lifted and the backoff timestamp is gone too.
    allowed, reason = t.can_run_now(state)
    assert allowed is True
    assert reason == "ok"
    assert state.step("batch").fail_count == 0


def test_skipped_blocks_run(tmp_path: Path):
    clock = MockClock(datetime(2026, 4, 21, 9, 0))
    state = _fresh_state(tmp_path, clock)
    t = BackoffTracker("backup", clock=clock)

    state.mark_skipped("backup", "weekend")
    allowed, reason = t.can_run_now(state)
    assert allowed is False
    assert reason == "already_done"


def test_invalid_args_raise():
    with pytest.raises(ValueError):
        BackoffTracker("")
    with pytest.raises(ValueError):
        BackoffTracker("x", min_wait_sec=-1)
    with pytest.raises(ValueError):
        BackoffTracker("x", max_fails=0)
