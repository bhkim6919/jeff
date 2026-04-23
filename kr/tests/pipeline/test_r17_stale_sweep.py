# -*- coding: utf-8 -*-
"""Tests for R17 + R15 — stale sweep with daemon-alive force-sweep + marker sync.

Background (RCA 20260423 §13):
    2026-04-23 KR_BATCH hung in step 5 (Fundamental snapshot). Daemon thread
    alive but stuck. Orchestrator stale sweep SKIPPED it because daemon was
    in self._running. state stayed PENDING for 54+ minutes. lab_eod_kr
    precondition gate blocked. Lifecycle broke.

    R17 — force sweep after 2x STALE_PENDING_SEC even if daemon alive.
    R15 — sync marker state with stale sweep.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.orchestrator import (
    Orchestrator,
    STALE_PENDING_SEC,
    STALE_PENDING_FORCE_SEC,
)
from pipeline.schema import STATUS_FAILED, STATUS_PENDING
from pipeline.state import PipelineState
from pipeline.steps.base import StepBase, StepRunResult


class _FixedClock:
    def __init__(self, t: datetime):
        self.t = t
    def __call__(self): return self.t
    def advance(self, **kw): self.t += timedelta(**kw)


class _SlowBatchStep(StepBase):
    """Simulates a step that hangs indefinitely (like step5 Fundamental)."""
    name = "batch"
    def _execute(self, state):
        import time
        time.sleep(10000)  # effectively forever
        return StepRunResult(ok=True)


class _SlowBootstrapStep(StepBase):
    """Same behavior for bootstrap_env test."""
    name = "bootstrap_env"
    def _execute(self, state):
        import time
        time.sleep(10000)
        return StepRunResult(ok=True)


def _noop_mirror(state, step_name):
    return True


# ---------- R17: force sweep after 2x threshold ----------

def test_daemon_alive_not_swept_before_force_threshold(tmp_path: Path):
    """Daemon alive + age < STALE_PENDING_FORCE_SEC → NOT swept."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    # Simulate batch PENDING for 45 min (>STALE but <FORCE)
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=45)
    state.save()
    # Add batch to _running (daemon alive)
    orch._running.add("batch")

    swept = orch._sweep_stale_pending(state)
    assert swept == [], "daemon-alive at 45min should not be swept yet"
    assert state.steps["batch"].status == STATUS_PENDING


def test_daemon_alive_force_swept_after_60min(tmp_path: Path):
    """R17 core: daemon alive + age > STALE_PENDING_FORCE_SEC → force swept."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=65)  # > 60min
    state.save()
    orch._running.add("batch")

    swept = orch._sweep_stale_pending(state)
    assert "batch" in swept
    assert state.steps["batch"].status == STATUS_FAILED
    assert "force_sweep_daemon_hung" in state.steps["batch"].last_error
    # _running should be cleared to allow retry
    assert "batch" not in orch._running


def test_daemon_dead_tier1_sweep_at_30min(tmp_path: Path):
    """Tier 1 (original behavior): daemon not alive + age > STALE → swept."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=35)  # > 30min
    state.save()
    # NB: batch NOT in orch._running (daemon dead)

    swept = orch._sweep_stale_pending(state)
    assert "batch" in swept
    assert state.steps["batch"].status == STATUS_FAILED
    assert "stale_pending" in state.steps["batch"].last_error
    assert "force_sweep" not in state.steps["batch"].last_error


def test_fresh_pending_not_swept(tmp_path: Path):
    """Fresh PENDING (< STALE_PENDING_SEC) → not swept."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=10)
    state.save()

    swept = orch._sweep_stale_pending(state)
    assert swept == []
    assert state.steps["batch"].status == STATUS_PENDING


def test_no_started_at_treated_as_stale_immediately(tmp_path: Path):
    """Safety: missing started_at → swept immediately."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = None  # safety case
    state.save()

    swept = orch._sweep_stale_pending(state)
    assert "batch" in swept
    assert "no_started_at" in state.steps["batch"].last_error


# ---------- R15: marker sync on stale sweep ----------

def test_marker_synced_on_tier1_sweep(tmp_path: Path):
    """R15: marker KR_BATCH=RUNNING transitions to FAILED on stale sweep."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=35)
    state.save()

    # Pre-seed marker to RUNNING (as if record_start had run)
    from pipeline.completion_marker import CompletionMarker
    from pipeline.completion_schema import (
        RUN_KR_BATCH, STATUS_RUNNING as MARKER_RUNNING,
    )
    marker = CompletionMarker.load_or_create_today(
        data_dir=tmp_path, trade_date=date(2026, 4, 23), clock=clock,
    )
    marker.transition(RUN_KR_BATCH, MARKER_RUNNING)
    marker.save()

    swept = orch._sweep_stale_pending(state)
    assert "batch" in swept

    # Marker should now be FAILED
    marker2 = CompletionMarker.load_date(date(2026, 4, 23), data_dir=tmp_path)
    assert marker2 is not None
    run = marker2.run(RUN_KR_BATCH)
    assert run.status == "FAILED"
    assert run.error is not None
    assert "stale_pending" in run.error.message


def test_marker_synced_on_force_sweep_daemon_hung(tmp_path: Path):
    """R15 + R17: force sweep also syncs marker."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=65)
    state.save()
    orch._running.add("batch")

    from pipeline.completion_marker import CompletionMarker
    from pipeline.completion_schema import RUN_KR_BATCH, STATUS_RUNNING as MARKER_RUNNING
    marker = CompletionMarker.load_or_create_today(
        data_dir=tmp_path, trade_date=date(2026, 4, 23), clock=clock,
    )
    marker.transition(RUN_KR_BATCH, MARKER_RUNNING)
    marker.save()

    swept = orch._sweep_stale_pending(state)
    assert "batch" in swept

    marker2 = CompletionMarker.load_date(date(2026, 4, 23), data_dir=tmp_path)
    run = marker2.run(RUN_KR_BATCH)
    assert run.status == "FAILED"
    assert "force_sweep_daemon_hung" in run.error.message


def test_unmapped_step_stale_sweep_no_marker_op(tmp_path: Path):
    """Stale sweep of unmapped step (bootstrap_env) does not touch marker."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBootstrapStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("bootstrap_env")
    state.steps["bootstrap_env"].started_at = clock() - timedelta(minutes=35)
    state.save()

    swept = orch._sweep_stale_pending(state)
    assert "bootstrap_env" in swept

    # Marker should NOT exist (bootstrap_env unmapped)
    from pipeline.completion_marker import CompletionMarker
    marker = CompletionMarker.load_date(date(2026, 4, 23), data_dir=tmp_path)
    # If marker exists, it should not have entries for unmapped steps
    if marker is not None:
        assert marker.runs == {} or all(
            rt in ("KR_BATCH", "KR_EOD", "US_EOD", "US_BATCH")
            for rt in marker.runs
        )


def test_stale_sweep_idempotent(tmp_path: Path):
    """Calling stale sweep twice doesn't double-fail or crash."""
    clock = _FixedClock(datetime(2026, 4, 23, 15, 0, 0))
    orch = Orchestrator(
        data_dir=tmp_path, steps=[_SlowBatchStep()], clock=clock,
        pg_mirror_fn=_noop_mirror, spawn_threads=False,
    )
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
        trade_date=date(2026, 4, 23), clock=clock,
    )
    state.mark_started("batch")
    state.steps["batch"].started_at = clock() - timedelta(minutes=35)
    state.save()

    from pipeline.completion_marker import CompletionMarker
    from pipeline.completion_schema import RUN_KR_BATCH, STATUS_RUNNING as MARKER_RUNNING
    marker = CompletionMarker.load_or_create_today(
        data_dir=tmp_path, trade_date=date(2026, 4, 23), clock=clock,
    )
    marker.transition(RUN_KR_BATCH, MARKER_RUNNING)
    marker.save()

    swept1 = orch._sweep_stale_pending(state)
    assert "batch" in swept1
    swept2 = orch._sweep_stale_pending(state)
    # Second call: batch now FAILED, not PENDING → skipped
    assert swept2 == []

    # Marker still FAILED (idempotent)
    marker2 = CompletionMarker.load_date(date(2026, 4, 23), data_dir=tmp_path)
    assert marker2.run(RUN_KR_BATCH).status == "FAILED"
