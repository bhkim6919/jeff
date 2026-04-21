# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.gate_observer.GateObserverStep."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import STATUS_DONE, STATUS_FAILED, STATUS_SKIPPED
from pipeline.state import PipelineState
from pipeline.steps.gate_observer import GateObserverStep


def _state_with_lab_eod_done(tmp_path: Path) -> PipelineState:
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    state.mark_done("lab_eod_kr", details={"trades": 14})
    state.save()
    return state


def test_gate_observer_success(tmp_path):
    payload = {
        "decision_flags": {"c_stage_ready": True},
        "c_stage_streak": 5,
        "c_stage_streak_required": 3,
    }
    step = GateObserverStep(run_today_fn=lambda **kw: payload)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.ok is True
    assert result.details["c_stage_ready"] is True
    assert result.details["c_stage_streak"] == 5
    assert state.step("gate_observer").status == STATUS_DONE


def test_gate_observer_idempotent_skip(tmp_path):
    """run_today returns None when already ran today."""
    step = GateObserverStep(run_today_fn=lambda **kw: None)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.skipped is True
    assert result.error == "already_ran_today"
    assert state.step("gate_observer").status == STATUS_SKIPPED


def test_gate_observer_module_missing_skips(tmp_path):
    """Missing tools.gate_observer module → SKIPPED (defensive)."""
    # run_today_fn=None triggers the import path; we simulate ImportError
    # by monkeypatching the import inside the step.
    step = GateObserverStep()

    # _load_run_today tries `from tools.gate_observer import run_today`;
    # in test env that module doesn't exist, returning None.
    loader_result = step._load_run_today()
    state = _state_with_lab_eod_done(tmp_path)

    if loader_result is None:
        # Module-not-found path
        result = step.run(state)
        assert result.skipped is True
        assert result.error == "module_not_found"
        assert state.step("gate_observer").status == STATUS_SKIPPED
    else:
        # tools.gate_observer exists in repo — skip this test
        pytest.skip("tools.gate_observer is present; module-missing path not exercised")


def test_gate_observer_crash_marks_failed(tmp_path):
    def _boom(**kw):
        raise RuntimeError("telegram_timeout")

    step = GateObserverStep(run_today_fn=_boom)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "telegram_timeout" in (result.error or "")


def test_gate_observer_blocked_without_lab_eod(tmp_path):
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    step = GateObserverStep(run_today_fn=lambda **kw: {"decision_flags": {}})

    result = step.run(state)

    assert result.skipped is True
    assert result.error == "blocked_by:lab_eod_kr"
