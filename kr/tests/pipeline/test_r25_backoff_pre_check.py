# -*- coding: utf-8 -*-
"""R25 (2026-04-23) — Orchestrator backoff pre-check + mirror transition.

Root cause audit findings:
1. `_evaluate_and_maybe_run` didn't pre-check BackoffTracker → abandoned /
   backoff states still spawned daemon threads every tick (33s interval).
   StepBase.run() would early-exit, but thread spawn + gate log + mirror
   call happened every time. Over 24h = thousands of wasted spawns +
   PG `pipeline_state_history` rows duplicated.
2. `_mirror_safe` called unconditionally after step.run() — even when
   state was unchanged (early-exit paths). PG `pipeline_state_history`
   accumulated NOT_STARTED + repeated-FAILED rows.

Fix: pre-check `step._tracker.can_run_now` before spawn; mirror only on
state transition (status/fail_count/started_at/finished_at change).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from pipeline.orchestrator import Orchestrator
from pipeline.state import PipelineState
from pipeline.steps.base import StepBase, StepRunResult


class _DummyStep(StepBase):
    name = "dummy"
    preconditions = ()
    time_window = None  # disable window gate

    def _execute(self, state) -> StepRunResult:
        return StepRunResult(ok=True, skipped=False)


def _tz_kst(clock_dt: datetime) -> datetime:
    return clock_dt


def test_abandoned_state_not_spawned(tmp_path):
    """FAILED + fail_count >= max_fails → no spawn, no mirror."""
    mirror_calls = []

    def _fake_mirror(state, step_name):
        mirror_calls.append(step_name)
        return True

    step = _DummyStep()
    # Simulate already-abandoned state from earlier in the day
    orch = Orchestrator(
        steps=[step],
        data_dir=tmp_path,
        spawn_threads=False,
        pg_mirror_fn=_fake_mirror,
    )
    # Pre-populate state: batch FAILED with fail_count=3 (max)
    state = PipelineState.load_or_create_today(data_dir=tmp_path, mode="paper_forward")
    state.mark_failed(step.name, "pre-existing abandoned")
    state.mark_failed(step.name, "pre-existing abandoned")
    state.mark_failed(step.name, "pre-existing abandoned")
    state.save()

    summary = orch.tick()

    # Step should be skipped with 'abandoned' reason
    skip_reasons = dict(summary["skipped"])
    assert skip_reasons.get("dummy") == "abandoned", (
        f"expected abandoned, got {skip_reasons}"
    )
    # No mirror write since state didn't change
    assert mirror_calls == []
    # Step not in spawned list
    assert "dummy" not in summary["spawned"]


def test_backoff_state_not_spawned_during_min_wait(tmp_path):
    """FAILED with fail_count < max but recent → backoff → no spawn."""
    mirror_calls = []
    step = _DummyStep()

    def _fake_mirror(state, step_name):
        mirror_calls.append(step_name)
        return True

    orch = Orchestrator(
        steps=[step],
        data_dir=tmp_path,
        spawn_threads=False,
        pg_mirror_fn=_fake_mirror,
    )
    state = PipelineState.load_or_create_today(data_dir=tmp_path, mode="paper_forward")
    state.mark_failed(step.name, "one recent fail")  # fail_count=1
    # last_failed_at is set to now → within min_wait_sec
    state.save()

    summary = orch.tick()
    skip_reasons = dict(summary["skipped"])
    assert skip_reasons.get("dummy") == "backoff", (
        f"expected backoff, got {skip_reasons}"
    )
    assert mirror_calls == []


def test_fresh_state_spawns_and_mirrors(tmp_path):
    """First run — no prior state → spawn + mirror (transition NOT_STARTED → DONE)."""
    mirror_calls = []
    step = _DummyStep()

    def _fake_mirror(state, step_name):
        mirror_calls.append((step_name, state.step(step_name).status))
        return True

    orch = Orchestrator(
        steps=[step],
        data_dir=tmp_path,
        spawn_threads=False,
        pg_mirror_fn=_fake_mirror,
    )
    summary = orch.tick()
    assert "dummy" in summary["spawned"]
    # Mirror called: status changed NOT_STARTED → DONE
    assert mirror_calls == [("dummy", "DONE")], mirror_calls


def test_mirror_suppressed_when_state_unchanged_after_run(tmp_path):
    """If step.run() early-exits (e.g. backoff triggered during run), state
    is unchanged → mirror should be suppressed. Guards against regression
    of PG pollution.
    """
    mirror_calls = []

    class _NoopRun(StepBase):
        name = "noop"
        preconditions = ()
        time_window = None

        def run(self, state):
            # Intentionally NOT mutating state — simulates early-exit path
            return StepRunResult(ok=False, skipped=True, error="simulated_noop")

        def _execute(self, state):
            raise RuntimeError("should not be called")

    step = _NoopRun()

    def _fake_mirror(state, step_name):
        mirror_calls.append(step_name)
        return True

    orch = Orchestrator(
        steps=[step],
        data_dir=tmp_path,
        spawn_threads=False,
        pg_mirror_fn=_fake_mirror,
    )
    summary = orch.tick()
    # spawned happened (since pre-check passes for NOT_STARTED state)
    assert "noop" in summary["spawned"]
    # But mirror suppressed because step.run didn't change state
    assert mirror_calls == [], (
        f"mirror should be suppressed when state unchanged, got {mirror_calls}"
    )
