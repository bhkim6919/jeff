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
    tray_integration.HOLDER._bootstrap_recorded = False
    tray_integration.HOLDER._last_tick_summary = None
    yield
    tray_integration.HOLDER._orch = None
    tray_integration.HOLDER._bootstrap_recorded = False
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
