# -*- coding: utf-8 -*-
"""Tests for R1 — preflight.check_universe_healthy.

2-stage design:
  1st lightweight: CSV file count + history row sampling
  2nd strict: actual build_universe_from_ohlcv call

Jeff R1 gate (2026-04-23):
  - non-KR-batch run_type → skipped (None)
  - healthy state (full CSV + universe >= min) → True
  - CSV count < 2500 → 1st gate False
  - history sample ratio < 80% → 1st gate False
  - build_universe returns < min_count → 2nd gate False
  - universe_builder crash → 2nd gate False
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

# Opt into real preflight execution
_preflight_enabled = True

from pipeline import preflight
from pipeline.completion_schema import (
    RUN_KR_BATCH,
    RUN_KR_EOD,
    RUN_US_EOD,
)
from pipeline.preflight import CheckResult, check_universe_healthy


class _FakeState:
    def __init__(self, tmp_path: Path):
        self.data_dir = tmp_path


def _write_csv(path: Path, n_rows: int, latest_date: str = "2026-04-22") -> None:
    """Write a CSV with n_rows of OHLCV data, ending on latest_date."""
    from datetime import datetime, timedelta
    path.parent.mkdir(parents=True, exist_ok=True)
    last = datetime.strptime(latest_date, "%Y-%m-%d")
    lines = ["date,open,high,low,close,volume"]
    for i in range(n_rows):
        d = last - timedelta(days=n_rows - 1 - i)
        lines.append(f"{d.strftime('%Y-%m-%d')},100,110,95,105,1000000")
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------- Run type dispatch ----------

def test_non_kr_batch_returns_none(tmp_path: Path):
    state = _FakeState(tmp_path)
    r = check_universe_healthy(RUN_KR_EOD, state)
    assert r.ok is None
    assert "skipped" in (r.detail or {})


def test_us_eod_returns_none(tmp_path: Path):
    state = _FakeState(tmp_path)
    r = check_universe_healthy(RUN_US_EOD, state)
    assert r.ok is None


# ---------- 1st gate — CSV count ----------

def test_missing_ohlcv_dir_fails(tmp_path: Path):
    """If config points to non-existent OHLCV dir → 1st gate fail."""
    state = _FakeState(tmp_path)
    fake_dir = tmp_path / "nonexistent"  # not created
    cfg = _make_mock_config(fake_dir)
    r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is False
    assert "missing" in (r.error or "").lower()


# Config mock helper
def _make_mock_config(ohlcv_dir, min_history=260, min_universe=500,
                     min_close=2000, min_amount=2_000_000_000,
                     markets=("KOSPI", "KOSDAQ")):
    class _MockConfig:
        UNIV_MIN_HISTORY = min_history
        UNIV_MIN_COUNT = min_universe
        UNIV_MIN_CLOSE = min_close
        UNIV_MIN_AMOUNT = min_amount
        MARKETS = list(markets)
        @property
        def OHLCV_DIR(self):
            return ohlcv_dir
    return _MockConfig()


def _run_check_with_config(run_type, state, mock_cfg):
    """Invoke check_universe_healthy with a mocked Gen4Config."""
    import config as _config_mod
    with mock.patch.object(_config_mod, "Gen4Config", return_value=mock_cfg):
        return check_universe_healthy(run_type, state)


def test_csv_count_too_low_fails(tmp_path: Path):
    """CSV count < 2500 → 1st gate fail."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    # Write only 10 CSVs (below 2500)
    for i in range(10):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 300)

    cfg = _make_mock_config(ohlcv_dir)
    state = _FakeState(tmp_path)
    r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is False
    assert r.detail.get("stage") == "1st"
    assert r.detail.get("csv_count") == 10


# ---------- 1st gate — history sampling ----------

def test_history_ratio_too_low_fails(tmp_path: Path):
    """CSVs with only 30 rows → history ratio 0 → 1st gate fail."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    # 2600 CSVs but all with only 30 rows (below min_history=260)
    for i in range(2600):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 30)  # too short

    cfg = _make_mock_config(ohlcv_dir, min_history=260)
    state = _FakeState(tmp_path)
    r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is False
    assert r.detail.get("stage") == "1st"
    assert r.detail.get("history_ratio", 1.0) < 0.8


def test_full_history_passes_1st_gate(tmp_path: Path):
    """All CSVs with 500+ rows → 1st gate passes. 2nd gate may still fail."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    # 2600 CSVs with 500 rows each
    for i in range(2600):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 500)

    cfg = _make_mock_config(ohlcv_dir, min_history=260, min_universe=500)
    state = _FakeState(tmp_path)
    r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    # 1st gate passes; 2nd gate depends on actual universe_builder
    # With synthetic uniform CSVs, universe likely passes too (default filters).
    # Test just verifies 1st gate didn't block.
    if r.ok is False:
        # If it failed, must be 2nd gate
        assert r.detail.get("stage") == "2nd"


# ---------- 2nd gate — real build_universe ----------

def test_universe_builder_crash_returns_false(tmp_path: Path):
    """universe_builder crash → 2nd gate fail with 'build_crash'."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    for i in range(2600):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 500)

    cfg = _make_mock_config(ohlcv_dir, min_history=260, min_universe=500)
    state = _FakeState(tmp_path)

    # Mock build_universe_from_ohlcv to crash
    from data import universe_builder as ub
    with mock.patch.object(ub, "build_universe_from_ohlcv",
                          side_effect=RuntimeError("synthetic crash")):
        r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is False
    assert r.detail.get("stage") == "2nd"
    assert "build_crash" in str(r.detail.get("issue", ""))


def test_universe_too_small_fails(tmp_path: Path):
    """build_universe returns < min_count → 2nd gate fail."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    for i in range(2600):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 500)

    cfg = _make_mock_config(ohlcv_dir, min_history=260, min_universe=500)
    state = _FakeState(tmp_path)

    # Mock build_universe to return empty
    from data import universe_builder as ub
    with mock.patch.object(ub, "build_universe_from_ohlcv",
                          return_value=[]):
        r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is False
    assert r.detail.get("stage") == "2nd"
    assert r.detail.get("universe_count") == 0


def test_universe_healthy_passes(tmp_path: Path):
    """All stages pass → ok=True with rich detail."""
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    for i in range(2600):
        _write_csv(ohlcv_dir / f"{i:06d}.csv", 500)

    cfg = _make_mock_config(ohlcv_dir, min_history=260, min_universe=500)
    state = _FakeState(tmp_path)

    # Mock build_universe to return above min_count
    from data import universe_builder as ub
    fake_universe = [f"{i:06d}" for i in range(600)]
    with mock.patch.object(ub, "build_universe_from_ohlcv",
                          return_value=fake_universe):
        r = _run_check_with_config(RUN_KR_BATCH, state, cfg)
    assert r.ok is True
    assert r.detail.get("universe_count") == 600
    assert r.detail.get("csv_count") == 2600
    assert r.detail.get("history_ratio", 0) >= 0.8


# ---------- ChecksBlock integration ----------

def test_checks_block_contains_universe_healthy():
    from pipeline.completion_marker import ChecksBlock
    cb = ChecksBlock(universe_healthy=True)
    d = cb.to_dict()
    assert "universe_healthy" in d
    assert d["universe_healthy"] is True


def test_any_false_detects_universe_healthy_false():
    """Invariant I1: SUCCESS forbids any False, including universe_healthy."""
    from pipeline.completion_marker import ChecksBlock
    cb = ChecksBlock(universe_healthy=False)
    assert cb.any_false() is True


def test_roundtrip_preserves_universe_healthy():
    from pipeline.completion_marker import ChecksBlock
    cb1 = ChecksBlock(universe_healthy=True, imports_ok=True)
    d = cb1.to_dict()
    cb2 = ChecksBlock.from_dict(d)
    assert cb2.universe_healthy is True
    assert cb2.imports_ok is True


# ---------- CHECKS_FOR_RUN registry ----------

def test_kr_batch_includes_universe_healthy():
    """KR_BATCH must have universe_healthy; other run_types must NOT."""
    assert any(name == "universe_healthy"
               for name, _ in preflight.CHECKS_FOR_RUN[RUN_KR_BATCH])
    assert not any(name == "universe_healthy"
                   for name, _ in preflight.CHECKS_FOR_RUN[RUN_KR_EOD])
    assert not any(name == "universe_healthy"
                   for name, _ in preflight.CHECKS_FOR_RUN[RUN_US_EOD])
