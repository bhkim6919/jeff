# -*- coding: utf-8 -*-
"""Unit tests for pipeline.state.PipelineState."""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import (
    SCHEMA_VERSION,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_NOT_STARTED,
    STATUS_PENDING,
    STATUS_SKIPPED,
)
from pipeline.state import PipelineState, StepState


def _make(tmp_path: Path, *, clock=None) -> PipelineState:
    return PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
        clock=clock,
    )


def test_create_today_fresh(tmp_path: Path):
    state = _make(tmp_path)
    assert state.trade_date == date(2026, 4, 21)
    assert state.mode == "paper_forward"
    assert state.tz == "Asia/Seoul"
    assert state.schema_version == SCHEMA_VERSION
    assert state.steps == {}


def test_roundtrip_save_load(tmp_path: Path):
    state = _make(tmp_path)
    state.mark_started("batch")
    state.mark_done("batch", details={"target_count": 20})
    state.save()

    path = tmp_path / "state_20260421.json"
    assert path.exists()

    loaded = PipelineState.load_date(date(2026, 4, 21), data_dir=tmp_path)
    assert loaded is not None
    assert loaded.is_done("batch")
    assert loaded.step("batch").details == {"target_count": 20}
    assert loaded.schema_version == SCHEMA_VERSION


def test_atomic_write_leaves_no_tmp(tmp_path: Path):
    state = _make(tmp_path)
    state.mark_started("ohlcv_sync")
    state.save()

    leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == [], f"tmp leftovers: {leftovers}"


def test_schema_version_mismatch_raises(tmp_path: Path):
    path = tmp_path / "state_20260421.json"
    path.write_text(
        json.dumps({
            "schema_version": 99,
            "trade_date": "2026-04-21",
            "tz": "Asia/Seoul",
            "mode": "paper_forward",
            "last_update": "2026-04-21T09:00:00",
            "steps": {},
        }),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="schema_version"):
        PipelineState.load_date(date(2026, 4, 21), data_dir=tmp_path)


def test_mark_failed_increments_fail_count(tmp_path: Path):
    state = _make(tmp_path)
    state.mark_failed("batch", "connection refused")
    state.mark_failed("batch", "still down")
    st = state.step("batch")
    assert st.status == STATUS_FAILED
    assert st.fail_count == 2
    assert st.last_error == "still down"
    assert st.last_failed_at is not None


def test_mark_done_sets_finished_and_details(tmp_path: Path):
    state = _make(tmp_path)
    state.mark_done("backup", details={"file": "backup_20260421.zip"})
    st = state.step("backup")
    assert st.status == STATUS_DONE
    assert st.finished_at is not None
    assert st.details == {"file": "backup_20260421.zip"}


def test_mark_skipped_is_terminal(tmp_path: Path):
    state = _make(tmp_path)
    state.mark_skipped("gate_observer", "precondition unmet")
    assert state.is_done("gate_observer")
    assert state.step("gate_observer").status == STATUS_SKIPPED
    assert state.step("gate_observer").details["skip_reason"] == "precondition unmet"


def test_load_yesterday_returns_none_when_absent(tmp_path: Path):
    """Open issue #5 enforcement: no catch-up, no auto-create for old dates."""
    result = PipelineState.load_date(date(2026, 4, 20), data_dir=tmp_path)
    assert result is None


def test_load_yesterday_returns_state_when_present(tmp_path: Path):
    """Historical read-only access is allowed (advisor/reports)."""
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="live",
        trade_date=date(2026, 4, 20),
    )
    state.mark_done("batch")
    state.save()

    loaded = PipelineState.load_date(date(2026, 4, 20), data_dir=tmp_path)
    assert loaded is not None
    assert loaded.mode == "live"
    assert loaded.is_done("batch")


def test_reopen_preserves_state(tmp_path: Path):
    """load_or_create_today on an existing file must NOT wipe it."""
    s1 = _make(tmp_path)
    s1.mark_done("ohlcv_sync")
    s1.save()

    s2 = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    assert s2.is_done("ohlcv_sync")


def test_unknown_status_coerced_to_not_started(tmp_path: Path):
    path = tmp_path / "state_20260421.json"
    path.write_text(
        json.dumps({
            "schema_version": SCHEMA_VERSION,
            "trade_date": "2026-04-21",
            "tz": "Asia/Seoul",
            "mode": "paper_forward",
            "last_update": "2026-04-21T09:00:00",
            "steps": {
                "batch": {"status": "WEIRD", "fail_count": 0, "details": {}},
            },
        }),
        encoding="utf-8",
    )
    loaded = PipelineState.load_date(date(2026, 4, 21), data_dir=tmp_path)
    assert loaded is not None
    assert loaded.step("batch").status == STATUS_NOT_STARTED


def test_step_state_default_fields():
    s = StepState()
    assert s.status == STATUS_NOT_STARTED
    assert s.fail_count == 0
    assert s.details == {}
    assert s.last_error is None
