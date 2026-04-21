# -*- coding: utf-8 -*-
"""Unit tests for pipeline.steps.lab_eod_us.LabEodUsStep."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.schema import STATUS_DONE, STATUS_FAILED
from pipeline.state import PipelineState
from pipeline.steps.lab_eod_us import LabEodUsStep


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


def _state_with_bootstrap_done(tmp_path: Path) -> PipelineState:
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    state.mark_done("bootstrap_env", details={})
    state.save()
    return state


def test_lab_eod_us_success(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    post_urls = []

    def _post(url, json=None, timeout=None):
        post_urls.append((url, json))
        return _FakeResp(body={
            "strategies_processed": ["momentum", "meanrev", "quality"],
            "trade_date": "2026-04-21",
        })

    step = LabEodUsStep(
        http_get=_get,
        http_post=_post,
        eod_date="2026-04-21",
        time_window=None,
    )
    state = _state_with_bootstrap_done(tmp_path)

    result = step.run(state)

    assert result.ok is True
    assert result.details["strategy_count"] == 3
    assert post_urls[0][1] == {"date": "2026-04-21", "force": False}
    assert state.step("lab_eod_us").status == STATUS_DONE


def test_lab_eod_us_error_in_body(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        return _FakeResp(body={"error": "alpaca_rate_limit"})

    step = LabEodUsStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_bootstrap_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "alpaca_rate_limit" in (result.error or "")
    assert state.step("lab_eod_us").status == STATUS_FAILED


def test_lab_eod_us_health_fail_blocks_eod(tmp_path):
    post_called = []

    def _get(url, timeout):
        raise ConnectionError("no us server")

    def _post(url, json=None, timeout=None):
        post_called.append(url)
        return _FakeResp()

    step = LabEodUsStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_bootstrap_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "health_fail" in (result.error or "")
    assert not post_called  # never attempts EOD when health fails


def test_lab_eod_us_http_error_code(tmp_path):
    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        return _FakeResp(status_code=503, ok=False)

    step = LabEodUsStep(http_get=_get, http_post=_post, time_window=None)
    state = _state_with_bootstrap_done(tmp_path)

    result = step.run(state)

    assert result.ok is False
    assert "http_503" in (result.error or "")


def test_lab_eod_us_force_flag_passed_through(tmp_path):
    captured = {}

    def _get(url, timeout):
        return _FakeResp()

    def _post(url, json=None, timeout=None):
        captured["json"] = json
        return _FakeResp(body={"strategies_processed": []})

    step = LabEodUsStep(
        http_get=_get, http_post=_post,
        force=True, eod_date="2026-04-21",
        time_window=None,
    )
    state = _state_with_bootstrap_done(tmp_path)
    step.run(state)

    assert captured["json"]["force"] is True


def test_lab_eod_us_blocked_without_bootstrap(tmp_path):
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path,
        mode="paper_forward",
        trade_date=date(2026, 4, 21),
    )
    step = LabEodUsStep(
        http_get=lambda *a, **kw: _FakeResp(),
        http_post=lambda *a, **kw: _FakeResp(),
        time_window=None,
    )
    result = step.run(state)

    assert result.skipped is True
    assert result.error == "blocked_by:bootstrap_env"
