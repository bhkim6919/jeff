# -*- coding: utf-8 -*-
"""Tests for R11 (2026-04-23) — regime MA200 sanity check.

Root cause recap:
    KOSPI.csv had mixed-scale historical data (2019-2025 at ~70000 scale from
    some unknown ticker, 2026 rows appended at real KOSPI ~6400 scale).
    MA200 = mean of last 200 closes = 198 bad + 2 good → ~74674.
    kospi_close (6418) / ma200 (74674) = 0.086 → false BEAR classification.

Fix: _compute_regime_snapshot returns ("SIDE", 0.0, 0.5) when
kospi_close/ma200 ratio is outside [0.5, 2.0] — CSV is treated as corrupted.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from lifecycle.utils import _compute_regime_snapshot


class _FakeConfig:
    def __init__(self, index_file: Path):
        self.INDEX_FILE = index_file


def _write_kospi_csv(path: Path, closes: list[float]) -> None:
    """Write minimal KOSPI.csv with just date+close."""
    dates = pd.date_range("2019-01-01", periods=len(closes), freq="D")
    df = pd.DataFrame({"date": dates, "close": closes})
    df.to_csv(path, index=False)


def test_normal_csv_returns_real_regime(tmp_path):
    """Clean CSV (all values at same scale) → regime computed normally."""
    csv = tmp_path / "KOSPI.csv"
    # 250 days of values near 2500 (realistic KOSPI scale)
    closes = [2500.0 + i * 0.5 for i in range(250)]
    _write_kospi_csv(csv, closes)
    regime, ma200, breadth = _compute_regime_snapshot(_FakeConfig(csv))
    # close ~ 2624, ma200 ~ 2537 → ratio ~ 1.03 → inside band
    assert regime in ("BULL", "SIDE", "BEAR")  # whatever calc_regime decides
    assert ma200 > 0
    assert breadth == 0.5


def test_corrupted_mixed_scale_triggers_side_fallback(tmp_path):
    """Reproduces today's bug: 198 bad + 2 good → ratio 0.086 → SIDE."""
    csv = tmp_path / "KOSPI.csv"
    # 198 rows at ~75000 (wrong ticker data), 2 rows at ~6400 (real KOSPI)
    closes = [75000.0] * 198 + [6417.0, 6465.0]
    _write_kospi_csv(csv, closes)
    regime, ma200, breadth = _compute_regime_snapshot(_FakeConfig(csv))
    # Ratio ~ 0.086 — outside [0.5, 2.0] → sanity fallback
    assert regime == "SIDE"
    assert ma200 == 0.0
    assert breadth == 0.5


def test_ratio_just_below_threshold_triggers_fallback(tmp_path):
    """ratio = 0.49 → below 0.5 threshold → fallback."""
    csv = tmp_path / "KOSPI.csv"
    # 200 values — mix to get ratio ~0.49
    # ma200 = avg of 200 values; kospi_close = last value
    # Easy case: 199 at 10000, 1 at 4900 → ma = 9974.5, close = 4900 → ratio 0.491
    closes = [10000.0] * 199 + [4900.0]
    _write_kospi_csv(csv, closes)
    regime, ma200, _ = _compute_regime_snapshot(_FakeConfig(csv))
    assert regime == "SIDE"
    assert ma200 == 0.0


def test_ratio_just_above_threshold_triggers_fallback(tmp_path):
    """ratio = 2.01 → above 2.0 threshold → fallback."""
    csv = tmp_path / "KOSPI.csv"
    # 199 values at 2500, 1 value at 5030 → ma ~ 2512.6, close = 5030 → ratio 2.002
    closes = [2500.0] * 199 + [5030.0]
    _write_kospi_csv(csv, closes)
    regime, ma200, _ = _compute_regime_snapshot(_FakeConfig(csv))
    assert regime == "SIDE"
    assert ma200 == 0.0


def test_ratio_at_exactly_1_0_passes_sanity(tmp_path):
    """Flat series — ratio exactly 1.0 — sanity passes, regime computed."""
    csv = tmp_path / "KOSPI.csv"
    closes = [2500.0] * 200
    _write_kospi_csv(csv, closes)
    regime, ma200, _ = _compute_regime_snapshot(_FakeConfig(csv))
    # Should not be stamped SIDE by sanity check (ma200 == 2500.0 proves it)
    assert ma200 == 2500.0


def test_insufficient_history_unchanged(tmp_path):
    """< 200 rows → unchanged behavior (SIDE, ma200=0, breadth=0.5)."""
    csv = tmp_path / "KOSPI.csv"
    closes = [2500.0] * 100
    _write_kospi_csv(csv, closes)
    regime, ma200, breadth = _compute_regime_snapshot(_FakeConfig(csv))
    assert regime == "SIDE"
    assert ma200 == 0.0
    assert breadth == 0.5


def test_read_error_returns_safe_default(tmp_path):
    """Missing file → SIDE default, no crash."""
    missing = tmp_path / "nonexistent.csv"
    regime, ma200, breadth = _compute_regime_snapshot(_FakeConfig(missing))
    assert regime == "SIDE"
    assert ma200 == 0.0
    assert breadth == 0.5
