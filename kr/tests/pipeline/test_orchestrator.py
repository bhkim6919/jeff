# -*- coding: utf-8 -*-
"""Unit tests for pipeline.orchestrator.Orchestrator."""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.orchestrator import STALE_PENDING_SEC, Orchestrator
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
        self.t += timedelta(**kw)


class _OkStep(StepBase):
    name = "alpha"

    def _execute(self, state):
        return StepRunResult(ok=True, details={"v": 1})


class _DepStep(StepBase):
    name = "beta"
    preconditions = ("alpha",)

    def _execute(self, state):
        return StepRunResult(ok=True, details={"v": 2})


class _FailStep(StepBase):
    name = "gamma"

    def _execute(self, state):
        return StepRunResult(ok=False, error="boom")


def _noop_mirror(state, step_name):
    return True


def _make_orch(tmp_path: Path, steps, clock=None, mirror=_noop_mirror):
    return Orchestrator(
        data_dir=tmp_path,
        steps=steps,
        mode="paper_forward",
        clock=clock,
        pg_mirror_fn=mirror,
        spawn_threads=False,  # deterministic
    )


# ---------- tests ----------

def test_tick_runs_ready_step_sync(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    orch = _make_orch(tmp_path, [_OkStep(clock=clock)], clock=clock)

    summary = orch.tick()

    assert summary["spawned"] == ["alpha"]
    assert summary["trade_date"] == "2026-04-21"
    # State persisted to disk
    state = PipelineState.load_date(
        date(2026, 4, 21), data_dir=tmp_path,
    )
    assert state is not None
    assert state.step("alpha").status == STATUS_DONE


def test_tick_respects_precondition_order(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    orch = _make_orch(
        tmp_path,
        [_OkStep(clock=clock), _DepStep(clock=clock)],
        clock=clock,
    )

    # First tick: only alpha ready, beta precondition unmet
    s1 = orch.tick()
    assert "alpha" in s1["spawned"]
    # In sync mode, alpha completes before beta is evaluated,
    # so beta may or may not run in tick 1 depending on order.
    # Run another tick to be sure.
    s2 = orch.tick()
    # beta should have run in either tick 1 (after alpha done sync) or tick 2
    state = PipelineState.load_date(
        date(2026, 4, 21), data_dir=tmp_path,
    )
    assert state.step("alpha").status == STATUS_DONE
    assert state.step("beta").status == STATUS_DONE


def test_tick_does_not_rerun_done_steps(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    calls = {"n": 0}

    class _CountedStep(StepBase):
        name = "counted"

        def _execute(self, state):
            calls["n"] += 1
            return StepRunResult(ok=True)

    orch = _make_orch(tmp_path, [_CountedStep(clock=clock)], clock=clock)

    orch.tick()
    orch.tick()
    orch.tick()

    assert calls["n"] == 1  # Only first tick ran the step


def test_tick_never_raises_on_step_crash(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))

    class _Crashy(StepBase):
        name = "crashy"

        def _execute(self, state):
            raise RuntimeError("explode")

    orch = _make_orch(tmp_path, [_Crashy(clock=clock)], clock=clock)

    # Must not raise
    summary = orch.tick()
    assert summary is not None
    state = PipelineState.load_date(
        date(2026, 4, 21), data_dir=tmp_path,
    )
    assert state.step("crashy").status == STATUS_FAILED


def test_stale_pending_sweep(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))

    # Pre-seed a stale PENDING state from a prior process crash.
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )
    state.mark_started("alpha")
    state.save()

    # Advance clock past stale threshold
    clock.advance(seconds=STALE_PENDING_SEC + 60)

    orch = _make_orch(tmp_path, [_OkStep(clock=clock)], clock=clock)
    summary = orch.tick()

    assert "alpha" in summary["stale_swept"]
    # After sweep the step is FAILED; backoff gate may or may not allow
    # rerun in same tick. Verify the sweep happened.
    reloaded = PipelineState.load_date(
        date(2026, 4, 21), data_dir=tmp_path,
    )
    # Status is either FAILED (sweep + not rerun) or DONE (sweep + rerun
    # since fail_count=1 and min_wait=300 just passed the clock advance).
    # Either way, fail_count bumped at least to 1.
    assert reloaded.step("alpha").fail_count >= 1


def test_mirror_called_for_each_transition(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    mirror_calls = []

    def _spy(state, step_name):
        mirror_calls.append(step_name)
        return True

    orch = _make_orch(tmp_path, [_OkStep(clock=clock)], clock=clock, mirror=_spy)
    orch.tick()

    # At minimum, alpha should have been mirrored on success
    assert "alpha" in mirror_calls


def test_mirror_failure_does_not_break_tick(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))

    def _broken(state, step_name):
        raise RuntimeError("pg down")

    orch = _make_orch(tmp_path, [_OkStep(clock=clock)], clock=clock, mirror=_broken)

    summary = orch.tick()
    assert summary is not None

    # Step still succeeded despite mirror raising
    state = PipelineState.load_date(
        date(2026, 4, 21), data_dir=tmp_path,
    )
    assert state.step("alpha").status == STATUS_DONE


def test_tick_summary_fields_present(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    orch = _make_orch(tmp_path, [_OkStep(clock=clock)], clock=clock)

    summary = orch.tick()

    for key in ("trade_date", "mode", "evaluated", "spawned",
                "skipped", "stale_swept", "errors"):
        assert key in summary


def test_precondition_unmet_recorded_as_skipped_in_summary(tmp_path):
    clock = _FixedClock(datetime(2026, 4, 21, 16, 0))
    orch = _make_orch(tmp_path, [_DepStep(clock=clock)], clock=clock)

    summary = orch.tick()

    assert summary["spawned"] == []
    # skipped is a list of (name, reason) tuples
    assert any(
        name == "beta" and "precondition" in reason
        for name, reason in summary["skipped"]
    )
