# -*- coding: utf-8 -*-
"""Unit tests for pipeline.tray_integration."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline import tray_integration


@pytest.fixture(autouse=True)
def _reset_holder():
    """Each test starts with a fresh holder so env changes take effect."""
    tray_integration.HOLDER._orch = None
    tray_integration.HOLDER._last_tick_summary = None
    yield
    tray_integration.HOLDER._orch = None
    tray_integration.HOLDER._last_tick_summary = None


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv(tray_integration.ENV_TOGGLE, raising=False)
    assert tray_integration.is_enabled() is False
    assert tray_integration.is_primary() is False


def test_enabled_when_env_set_to_1(monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "1")
    assert tray_integration.is_enabled() is True
    assert tray_integration.is_primary() is False  # shadow only


def test_primary_when_env_set_to_2(monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "2")
    assert tray_integration.is_enabled() is True
    assert tray_integration.is_primary() is True


def test_zero_is_disabled(monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "0")
    assert tray_integration.is_enabled() is False


def test_empty_is_disabled(monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "")
    assert tray_integration.is_enabled() is False


def test_tick_returns_none_when_disabled(monkeypatch):
    monkeypatch.delenv(tray_integration.ENV_TOGGLE, raising=False)
    assert tray_integration.tick_if_enabled() is None


def test_tick_never_raises_when_orchestrator_build_fails(monkeypatch):
    """If bootstrap or build throws, tick_if_enabled must swallow it."""
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "1")

    # Force build failure by making default_data_dir raise
    def _broken_dir(*a, **kw):
        raise RuntimeError("filesystem blew up")

    monkeypatch.setattr(tray_integration, "default_data_dir", _broken_dir)

    # Must NOT raise
    result = tray_integration.tick_if_enabled()
    # Either None (build failed, disabled) or a summary dict
    assert result is None or isinstance(result, dict)


def test_default_steps_has_known_names():
    steps = tray_integration.default_steps()
    names = [s.name for s in steps]
    assert "batch" in names
    assert "lab_eod_kr" in names
    assert "lab_eod_us" in names
    assert "backup" in names
    assert "gate_observer" in names


def test_default_data_dir_creates_path(tmp_path, monkeypatch):
    custom = tmp_path / "fake_repo"
    custom.mkdir()
    path = tray_integration.default_data_dir(repo_root=custom)
    assert path.exists()
    assert path.name == "pipeline"
    assert path.parent.name == "data"
    assert path.parent.parent.name == "kr"


# ─── Regression: date-rollover bootstrap idempotency (2026-04-22) ────────
#
# The prior implementation cached bootstrap_env completion in a per-process
# `_bootstrap_recorded` boolean. When the tray process survived midnight KST,
# a new trade-date state file was created with NO bootstrap_env record, but
# `_record_bootstrap` short-circuited via the in-memory flag. Every
# downstream step whose `preconditions` include `bootstrap_env` (notably
# lab_eod_us, which had a 60-second time window at 16:05 US/Eastern = 05:05
# KST) then failed its precondition and was skipped — the whole EOD missed.
#
# These tests pin the new behavior: `_record_bootstrap` makes its decision
# against today's state file, so a fresh file (new trade date) re-runs
# bootstrap_env regardless of prior ticks in the same process.


class _FakeOrch:
    """Minimal Orchestrator stand-in for _record_bootstrap tests."""

    def __init__(self, data_dir, mode="paper_forward"):
        self._data_dir = data_dir
        self._mode = mode


def _count_bootstrap_calls(monkeypatch) -> list[int]:
    """Patch bootstrap_env with a call counter; return the counter list."""
    calls: list[int] = []

    def _fake_bootstrap(data_dir, strict=False):
        calls.append(1)
        return {"tzdata": True, "zoneinfo_seoul": True, "data_dir_writable": True}

    monkeypatch.setattr(tray_integration, "bootstrap_env", _fake_bootstrap)
    return calls


def test_bootstrap_skips_when_already_done_same_day(tmp_path, monkeypatch):
    """If today's state already has bootstrap_env=DONE, skip the call."""
    calls = _count_bootstrap_calls(monkeypatch)
    orch = _FakeOrch(tmp_path)
    holder = tray_integration.OrchestratorHolder()

    holder._record_bootstrap(orch)  # first: should call
    holder._record_bootstrap(orch)  # second: state has DONE, should skip
    holder._record_bootstrap(orch)  # third: still skip

    assert len(calls) == 1


def test_bootstrap_reruns_on_new_trade_date(tmp_path, monkeypatch):
    """Regression for 2026-04-22: midnight rollover must re-run bootstrap.

    Simulates a long-lived tray process crossing midnight by swapping in
    a new (empty) state file under the same holder instance, mimicking
    what happens on 00:00 KST when `load_or_create_today` returns a
    fresh file for the new trade_date.
    """
    import json

    calls = _count_bootstrap_calls(monkeypatch)
    orch = _FakeOrch(tmp_path)
    holder = tray_integration.OrchestratorHolder()

    # Day 1: bootstrap runs, state file gets bootstrap_env=DONE.
    holder._record_bootstrap(orch)
    assert len(calls) == 1

    # Day 1 repeat tick: already done, skipped.
    holder._record_bootstrap(orch)
    assert len(calls) == 1

    # Simulate midnight rollover: replace today's state file with a
    # fresh one (empty steps), as `load_or_create_today` would on a
    # new trade_date. We find the most recent pipeline state JSON.
    state_files = sorted(tmp_path.glob("state_*.json"))
    assert state_files, "expected a state file to have been written"
    latest = state_files[-1]
    fresh = json.loads(latest.read_text(encoding="utf-8"))
    fresh["steps"] = {}  # no bootstrap_env record → new day behavior
    latest.write_text(json.dumps(fresh), encoding="utf-8")

    # Next tick after rollover MUST run bootstrap again.
    holder._record_bootstrap(orch)
    assert len(calls) == 2, (
        "bootstrap_env must re-run on new trade_date; per-process flag "
        "regression would leave this at 1 (and silently block lab_eod_us)"
    )
