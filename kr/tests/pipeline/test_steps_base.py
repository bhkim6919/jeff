# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.base.StepBase."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import (
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_SKIPPED,
)
from pipeline.state import PipelineState
from pipeline.steps.base import StepBase, StepRunResult


# ---------- test doubles ----------

class _FixedClock:
    def __init__(self, t: datetime):
        self.t = t

    def __call__(self) -> datetime:
        return self.t

    def advance(self, **kw):
        self.t = self.t + timedelta(**kw)


class _OkStep(StepBase):
    name = "ok_step"

    def _execute(self, state):
        return StepRunResult(ok=True, details={"rows": 42})


class _FailStep(StepBase):
    name = "fail_step"

    def _execute(self, state):
        return StepRunResult(ok=False, error="boom")


class _CrashStep(StepBase):
    name = "crash_step"

    def _execute(self, state):
        raise RuntimeError("explode")


class _SkipStep(StepBase):
    name = "skip_step"

    def _execute(self, state):
        return StepRunResult(ok=False, skipped=True, error="not_needed")


class _DependsStep(StepBase):
    name = "dep_step"
    preconditions = ("ok_step",)

    def _execute(self, state):
        return StepRunResult(ok=True)


class _BadReturnStep(StepBase):
    name = "bad_return"

    def _execute(self, state):
        return {"oops": True}  # not a StepRunResult


def _make_state(tmp_path: Path, *, clock=None) -> PipelineState:
    return PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )


# ---------- tests ----------

def test_ok_step_marks_done_with_details(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _OkStep(clock=clock)

    result = step.run(state)

    assert result.ok is True
    assert result.details == {"rows": 42}
    st = state.step("ok_step")
    assert st.status == STATUS_DONE
    assert st.fail_count == 0
    assert st.details["rows"] == 42


def test_fail_step_marks_failed_increments_fail_count(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _FailStep(clock=clock)

    result = step.run(state)

    assert result.ok is False
    st = state.step("fail_step")
    assert st.status == STATUS_FAILED
    assert st.fail_count == 1
    assert st.last_error == "boom"


def test_crash_step_does_not_propagate_and_records_fail(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _CrashStep(clock=clock)

    # Must NOT raise — orchestrator never sees exceptions from a step
    result = step.run(state)

    assert result.ok is False
    st = state.step("crash_step")
    assert st.status == STATUS_FAILED
    assert st.fail_count == 1
    assert "explode" in (st.last_error or "")


def test_skip_step_marks_skipped_not_failed(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _SkipStep(clock=clock)

    result = step.run(state)

    assert result.skipped is True
    st = state.step("skip_step")
    assert st.status == STATUS_SKIPPED
    assert st.fail_count == 0
    assert st.details["skip_reason"] == "not_needed"


def test_precondition_blocks_when_dep_not_done(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _DependsStep(clock=clock)

    result = step.run(state)

    assert result.ok is False
    assert result.skipped is True
    assert result.error == "blocked_by:ok_step"
    # Underlying step state never started because precondition failed
    assert "dep_step" not in state.steps or \
        state.step("dep_step").status != STATUS_PENDING


def test_precondition_passes_after_dep_done(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)

    # Run dep first, then dependent
    _OkStep(clock=clock).run(state)
    result = _DependsStep(clock=clock).run(state)

    assert result.ok is True
    assert state.step("dep_step").status == STATUS_DONE


def test_already_done_blocks_rerun(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _OkStep(clock=clock)

    first = step.run(state)
    second = step.run(state)

    assert first.ok is True
    assert second.skipped is True
    assert second.error == "already_done"


def test_backoff_blocks_rapid_retry(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _FailStep(clock=clock)

    # First failure
    step.run(state)
    # Immediate retry — should be blocked by backoff
    clock.advance(seconds=10)
    blocked = step.run(state)

    assert blocked.skipped is True
    assert blocked.error == "backoff"

    # After min_wait_sec (default 300s) the gate reopens — but fails again
    clock.advance(seconds=301)
    retry = step.run(state)
    assert retry.ok is False
    assert state.step("fail_step").fail_count == 2


def test_abandoned_after_max_fails(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _FailStep(clock=clock)

    for _ in range(3):
        step.run(state)
        clock.advance(seconds=301)

    assert state.step("fail_step").fail_count == 3
    # Next call — hard abandoned
    abandoned = step.run(state)
    assert abandoned.skipped is True
    assert abandoned.error == "abandoned"


def test_bad_return_is_treated_as_fail(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _BadReturnStep(clock=clock)

    result = step.run(state)

    assert result.ok is False
    st = state.step("bad_return")
    assert st.status == STATUS_FAILED
    assert "StepRunResult" in (st.last_error or "")


def test_missing_name_raises_typeerror():
    class _NoName(StepBase):
        def _execute(self, state):
            return StepRunResult(ok=True)

    with pytest.raises(TypeError):
        _NoName()


def test_state_persisted_to_disk_after_run(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    state = _make_state(tmp_path, clock=clock)
    step = _OkStep(clock=clock)
    step.run(state)

    # Re-load from disk — should observe terminal status
    reloaded = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )
    assert reloaded.step("ok_step").status == STATUS_DONE
    assert reloaded.step("ok_step").details["rows"] == 42
