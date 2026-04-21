# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.batch.BatchStep."""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import (
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_SKIPPED,
)
from pipeline.state import PipelineState
from pipeline.steps.batch import BatchStep


class _FakeConfig:
    pass


def _fake_cfg() -> _FakeConfig:
    return _FakeConfig()


def _make_state(tmp_path: Path, *, bootstrap_done: bool = True):
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    if bootstrap_done:
        state.mark_done("bootstrap_env", details={})
        state.save()
    return state


def test_batch_success_marks_done(tmp_path):
    target = {
        "target_tickers": ["A", "B", "C"],
        "snapshot_version": "2026-04-21:DB:2026-04-21:2500:abc123",
        "selected_source": "DB",
        "data_last_date": "2026-04-21",
        "date": "2026-04-21",
    }
    step = BatchStep(
        config_factory=_fake_cfg,
        run_batch_fn=lambda cfg, fast=True: target,
        time_window=None,
    )
    state = _make_state(tmp_path)

    result = step.run(state)

    assert result.ok is True
    assert result.details["target_count"] == 3
    assert result.details["snapshot_version"].startswith("2026-04-21")
    assert state.step("batch").status == STATUS_DONE


def test_batch_empty_universe_marks_failed(tmp_path):
    step = BatchStep(
        config_factory=_fake_cfg,
        run_batch_fn=lambda cfg, fast=True: None,
        time_window=None,
    )
    state = _make_state(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert result.error == "run_batch_returned_none"
    assert state.step("batch").status == STATUS_FAILED


def test_batch_empty_target_list_marks_failed(tmp_path):
    """A non-None return with empty target_tickers is suspect → FAIL."""
    step = BatchStep(
        config_factory=_fake_cfg,
        run_batch_fn=lambda cfg, fast=True: {"target_tickers": []},
        time_window=None,
    )
    state = _make_state(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert result.error == "empty_target_tickers"


def test_batch_crash_does_not_propagate(tmp_path):
    def _boom(cfg, fast=True):
        raise RuntimeError("db_connect_timeout")

    step = BatchStep(config_factory=_fake_cfg, run_batch_fn=_boom, time_window=None)
    state = _make_state(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "db_connect_timeout" in (result.error or "")
    assert state.step("batch").status == STATUS_FAILED


def test_batch_precondition_blocks_without_bootstrap(tmp_path):
    step = BatchStep(
        config_factory=_fake_cfg,
        run_batch_fn=lambda cfg, fast=True: {"target_tickers": ["A"]},
        time_window=None,
    )
    state = _make_state(tmp_path, bootstrap_done=False)

    result = step.run(state)

    assert result.ok is False
    assert result.skipped is True
    assert result.error == "blocked_by:bootstrap_env"


def test_batch_fast_flag_passed_through(tmp_path):
    captured = {}

    def _spy(cfg, fast=True):
        captured["fast"] = fast
        return {"target_tickers": ["A"], "date": "2026-04-21"}

    step = BatchStep(
        config_factory=_fake_cfg,
        run_batch_fn=_spy,
        fast=False,
        time_window=None,
    )
    state = _make_state(tmp_path)

    step.run(state)

    assert captured["fast"] is False
