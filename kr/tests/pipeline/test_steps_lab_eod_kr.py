# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.lab_eod_kr.LabEodKrStep."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import STATUS_DONE, STATUS_FAILED, STATUS_SKIPPED
from pipeline.state import PipelineState
from pipeline.steps.lab_eod_kr import LabEodKrStep


class _FakeResp:
    def __init__(self, body=None, status_code=200, ok=True):
        self._body = body or {}
        self.status_code = status_code
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._body


def _state_with_batch_done(tmp_path: Path) -> PipelineState:
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    state.mark_done("batch", details={"target_count": 20})
    state.save()
    return state


def test_lab_eod_kr_success(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    post_calls = []

    def _post(url, json=None, timeout=None):
        post_calls.append((url, json))
        if url.endswith("/api/lab/live/start"):
            return _FakeResp()
        if url.endswith("/api/lab/live/run-daily"):
            return _FakeResp(body={
                "ok": True, "trades": 14,
                "strategies": 9, "trade_date": "2026-04-21",
            })
        pytest.fail(f"unexpected POST {url}")

    step = LabEodKrStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_batch_done(tmp_path)

    result = step.run(state)

    assert result.ok is True
    assert result.details["trades"] == 14
    assert result.details["strategies"] == 9
    assert len(post_calls) == 2  # start + run-daily
    assert state.step("lab_eod_kr").status == STATUS_DONE


def test_lab_eod_kr_skipped(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        if url.endswith("/run-daily"):
            return _FakeResp(body={"skipped": True, "reason": "already_ran"})
        return _FakeResp()

    step = LabEodKrStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_batch_done(tmp_path)

    result = step.run(state)

    assert result.skipped is True
    assert state.step("lab_eod_kr").status == STATUS_SKIPPED


def test_lab_eod_kr_health_fail(tmp_path):
    class _BadGet:
        def __init__(self):
            self.called = False

        def __call__(self, url, timeout):
            self.called = True
            raise ConnectionError("refused")

    bad_get = _BadGet()
    step = LabEodKrStep(http_get=bad_get, http_post=lambda *a, **kw: _FakeResp(), time_window=None)
    state = _state_with_batch_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "health_fail" in (result.error or "")
    assert bad_get.called
    assert state.step("lab_eod_kr").status == STATUS_FAILED


def test_lab_eod_kr_http_error_code(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        if url.endswith("/run-daily"):
            return _FakeResp(status_code=500, ok=False)
        return _FakeResp()

    step = LabEodKrStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_batch_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "http_500" in (result.error or "")


def test_lab_eod_kr_blocked_without_batch(tmp_path):
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )

    step = LabEodKrStep(
        http_get=lambda *a, **kw: _FakeResp(),
        http_post=lambda *a, **kw: _FakeResp(),
        time_window=None,
    )
    result = step.run(state)

    assert result.skipped is True
    assert result.error == "blocked_by:batch"


def test_lab_eod_kr_explicit_failure_payload(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        if url.endswith("/run-daily"):
            return _FakeResp(body={"ok": False, "error": "snapshot_mismatch"})
        return _FakeResp()

    step = LabEodKrStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_batch_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert result.error == "snapshot_mismatch"
