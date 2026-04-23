# -*- coding: utf-8 -*-
"""Tests for R3 (2026-04-23) — update_ohlcv_incremental truncation guard.

Root cause recap: 2026-04-22 batch saw universe=0 because per-stock CSVs
had been truncated to ~30 rows, failing min_history=260. Previously the
function would silently overwrite an existing (even corrupted) CSV with
just the 30-day fetch result, cascading the damage.

Guard: when len(combined) < len(existing) → refuse write, log
[R3_TRUNCATE_GUARD], operator-driven restore via restore_ohlcv_from_db.py.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from data.pykrx_provider import update_ohlcv_incremental


def _daily_range(start: str, n_days: int) -> list[str]:
    """Generate n consecutive YYYY-MM-DD strings starting from start."""
    d0 = date.fromisoformat(start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(n_days)]


def _make_df(dates: list[str]) -> pd.DataFrame:
    n = len(dates)
    return pd.DataFrame({
        "date": pd.to_datetime(dates),
        "open": [1000.0] * n,
        "high": [1100.0] * n,
        "low": [900.0] * n,
        "close": [1050.0] * n,
        "volume": [10000] * n,
    })


def _write_csv(path: Path, dates: list[str]) -> None:
    _make_df(dates).to_csv(path, index=False)


def test_happy_path_appends_new_days(tmp_path):
    """Existing 100 days + fetch with 30 recent (20 overlap, 10 new) →
    combined 110 days (superset)."""
    code = "005930"
    path = tmp_path / f"{code}.csv"
    existing_dates = _daily_range("2026-01-01", 100)  # 100 unique
    _write_csv(path, existing_dates)

    # New fetch: last 20 of existing + 10 brand new
    new_dates = existing_dates[-20:] + _daily_range("2026-04-11", 10)
    new_df = _make_df(new_dates)

    with mock.patch("data.pykrx_provider.get_stock_ohlcv",
                    return_value=new_df):
        updated = update_ohlcv_incremental(tmp_path, [code], days=30)

    assert updated == 1
    result = pd.read_csv(path, parse_dates=["date"])
    # Union: 100 existing ∪ 30 new = 110 unique dates
    assert len(result) == 110


def test_truncate_guard_blocks_write_when_combined_smaller(tmp_path, caplog):
    """Simulate pandas concat returning fewer rows than existing.
    Guard must refuse write and leave disk file untouched.
    """
    import logging
    code = "005930"
    path = tmp_path / f"{code}.csv"
    # 500 real rows on disk
    existing_dates = _daily_range("2019-01-01", 500)
    _write_csv(path, existing_dates)
    assert len(pd.read_csv(path)) == 500

    new_df = _make_df(_daily_range("2026-04-01", 30))

    # Force pathological concat behavior (simulates silent pandas corruption)
    def _bad_concat(*args, **kwargs):
        return _make_df([])  # 0 rows — less than 500 existing

    with mock.patch("data.pykrx_provider.get_stock_ohlcv",
                    return_value=new_df):
        with mock.patch("data.pykrx_provider.pd.concat",
                        side_effect=_bad_concat):
            with caplog.at_level(logging.ERROR, logger="gen4.data"):
                updated = update_ohlcv_incremental(
                    tmp_path, [code], days=30
                )

    # Guard fired → skip this code
    assert updated == 0
    # File still has 500 rows (not overwritten)
    result = pd.read_csv(path)
    assert len(result) == 500
    assert any("R3_TRUNCATE_GUARD" in r.message for r in caplog.records)


def test_no_existing_file_writes_new(tmp_path):
    """New listing (no CSV yet) → write new_df, no guard."""
    code = "900100"
    path = tmp_path / f"{code}.csv"
    assert not path.exists()

    new_df = _make_df(_daily_range("2026-04-01", 9))
    with mock.patch("data.pykrx_provider.get_stock_ohlcv",
                    return_value=new_df):
        updated = update_ohlcv_incremental(tmp_path, [code], days=30)

    assert updated == 1
    assert path.exists()
    assert len(pd.read_csv(path)) == 9


def test_empty_fetch_skips_stock(tmp_path):
    """get_stock_ohlcv returning None → skip (not counted as update)."""
    code = "005930"
    path = tmp_path / f"{code}.csv"
    _write_csv(path, _daily_range("2026-01-01", 300))
    pre_rows = len(pd.read_csv(path))

    with mock.patch("data.pykrx_provider.get_stock_ohlcv",
                    return_value=None):
        updated = update_ohlcv_incremental(tmp_path, [code], days=30)

    assert updated == 0
    # File untouched
    assert len(pd.read_csv(path)) == pre_rows


def test_guard_does_not_block_equal_or_larger_combined(tmp_path):
    """Combined ≥ existing → write proceeds normally. Guard only fires
    on strict less-than."""
    code = "005930"
    path = tmp_path / f"{code}.csv"
    _write_csv(path, _daily_range("2026-01-01", 100))

    # new_df has 30 entirely new days → combined = 130 > 100
    new_df = _make_df(_daily_range("2026-05-01", 30))
    with mock.patch("data.pykrx_provider.get_stock_ohlcv",
                    return_value=new_df):
        updated = update_ohlcv_incremental(tmp_path, [code], days=30)

    assert updated == 1
    assert len(pd.read_csv(path)) == 130
