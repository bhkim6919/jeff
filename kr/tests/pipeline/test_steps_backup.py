# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.backup.BackupStep."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import STATUS_DONE, STATUS_FAILED
from pipeline.state import PipelineState
from pipeline.steps.backup import BackupStep


def _state_with_lab_eod_done(tmp_path: Path) -> PipelineState:
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    state.mark_done("lab_eod_kr", details={"trades": 14})
    state.save()
    return state


def test_backup_success(tmp_path):
    def _run():
        return (True, "[BACKUP_OK] 20260421 completed in 30s\n  pg_dump: OK")

    step = BackupStep(run_backup_fn=_run)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.ok is True
    assert "BACKUP_OK" in result.details["summary_head"]
    assert state.step("backup").status == STATUS_DONE


def test_backup_failure(tmp_path):
    def _run():
        return (False, "[BACKUP_FAIL] 20260421 — failed: pg_dump")

    step = BackupStep(run_backup_fn=_run)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "BACKUP_FAIL" in (result.error or "")
    assert state.step("backup").status == STATUS_FAILED


def test_backup_crash(tmp_path):
    def _run():
        raise RuntimeError("pg_dump_binary_missing")

    step = BackupStep(run_backup_fn=_run)
    state = _state_with_lab_eod_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "pg_dump_binary_missing" in (result.error or "")


def test_backup_blocked_without_lab_eod(tmp_path):
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    step = BackupStep(run_backup_fn=lambda: (True, "ok"))

    result = step.run(state)

    assert result.skipped is True
    assert result.error == "blocked_by:lab_eod_kr"
