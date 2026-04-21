# -*- coding: utf-8 -*-
"""Unit tests for pipeline.bootstrap.bootstrap_env."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.bootstrap import BootstrapError, bootstrap_env


def test_normal_env_passes(tmp_path: Path):
    checks = bootstrap_env(data_dir=tmp_path, strict=True)
    assert checks["tzdata"] is True
    assert checks["zoneinfo_seoul"] is True
    assert checks["data_dir_writable"] is True


def test_no_data_dir_optional(tmp_path: Path):
    checks = bootstrap_env(strict=True)
    assert checks["tzdata"] is True
    assert "data_dir_writable" not in checks


def test_strict_false_returns_dict_without_raising(tmp_path: Path, monkeypatch):
    # Force tzdata failure by hiding the module from importlib
    import importlib

    real_import = importlib.import_module

    def fake_import(name, *a, **kw):
        if name == "tzdata":
            raise ImportError("mocked missing tzdata")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    checks = bootstrap_env(data_dir=tmp_path, strict=False)
    assert checks["tzdata"] is False
    # zoneinfo_seoul may still pass (tzdata import fail ≠ ZoneInfo lookup fail)
    # so we only assert on the tzdata bit


def test_strict_true_raises_on_tzdata_missing(tmp_path: Path, monkeypatch):
    import importlib

    real_import = importlib.import_module

    def fake_import(name, *a, **kw):
        if name == "tzdata":
            raise ImportError("mocked missing tzdata")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    with pytest.raises(BootstrapError, match="tzdata"):
        bootstrap_env(data_dir=tmp_path, strict=True)


def test_unwritable_data_dir_reports_failure(tmp_path: Path):
    # Point at a file (not dir) — mkdir(exist_ok=True) will fail
    blocker = tmp_path / "blocker.txt"
    blocker.write_text("x")

    checks = bootstrap_env(data_dir=blocker, strict=False)
    assert checks["data_dir_writable"] is False
