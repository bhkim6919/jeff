# -*- coding: utf-8 -*-
"""Tests for R4 Stage 1 — universe_builder DB shadow mode.

Jeff R4 원칙 (변경 금지, work_plan_20260423.md §R4 원칙):
    - Default 전환 금지 (shadow only)
    - CSV 실사용 유지, DB 는 diff 로그만
    - Diff metric: csv_count, db_count, only_csv_count, only_db_count, diff_pct
    - 3영업일 diff_pct < 1% + 극단치 없음 → JUG 검토 후 전환

Shadow mode scope:
    - `build_universe_from_db(db_provider, ...)` new function
    - `compare_universes(csv, db)` diff helper
    - batch.py step2: runs both; logs diff; uses CSV for downstream
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from data.universe_builder import (
    build_universe_from_db,
    compare_universes,
    is_preferred_stock,
)


# ---------- compare_universes — diff metric shape ----------

def test_compare_identical_sets_zero_diff():
    csv_list = ["005930", "000660", "035720"]
    db_list = ["005930", "000660", "035720"]
    d = compare_universes(csv_list, db_list)
    assert d["csv_count"] == 3
    assert d["db_count"] == 3
    assert d["only_csv_count"] == 0
    assert d["only_db_count"] == 0
    assert d["diff_pct"] == 0.0


def test_compare_disjoint_sets():
    csv_list = ["005930", "000660"]
    db_list = ["035720", "051910"]
    d = compare_universes(csv_list, db_list)
    assert d["csv_count"] == 2
    assert d["db_count"] == 2
    assert d["only_csv_count"] == 2
    assert d["only_db_count"] == 2
    # total union 4, diff 4 → 100%
    assert d["diff_pct"] == 100.0


def test_compare_partial_overlap():
    csv_list = ["A", "B", "C", "D"]
    db_list = ["B", "C", "D", "E"]
    d = compare_universes(csv_list, db_list)
    assert d["csv_count"] == 4
    assert d["db_count"] == 4
    assert d["only_csv_count"] == 1  # A
    assert d["only_db_count"] == 1   # E
    # union = 5, diff = 2 → 40%
    assert d["diff_pct"] == 40.0


def test_compare_empty_both():
    d = compare_universes([], [])
    assert d["csv_count"] == 0
    assert d["db_count"] == 0
    assert d["only_csv_count"] == 0
    assert d["only_db_count"] == 0
    assert d["diff_pct"] == 0.0


def test_compare_empty_csv_some_db():
    d = compare_universes([], ["A", "B"])
    assert d["csv_count"] == 0
    assert d["db_count"] == 2
    assert d["only_csv_count"] == 0
    assert d["only_db_count"] == 2
    assert d["diff_pct"] == 100.0


def test_compare_returns_all_required_fields():
    """Jeff spec: metric must include all 5 fields."""
    d = compare_universes(["A"], ["B"])
    required = {"csv_count", "db_count", "only_csv_count",
                "only_db_count", "diff_pct"}
    assert required.issubset(d.keys())


def test_compare_samples_bounded_to_10():
    """Only_csv/db_sample lists capped at 10 entries."""
    big_csv = [f"C{i:04d}" for i in range(50)]
    big_db = [f"D{i:04d}" for i in range(50)]
    d = compare_universes(big_csv, big_db)
    assert len(d["only_csv_sample"]) == 10
    assert len(d["only_db_sample"]) == 10


# ---------- build_universe_from_db — SQL + post-filter ----------

class _MockCursor:
    def __init__(self, rows):
        self._rows = rows
        self.last_query = None
        self.last_params = None
    def execute(self, query, params=None):
        self.last_query = query
        self.last_params = params
    def fetchall(self):
        return self._rows
    def __enter__(self): return self
    def __exit__(self, *a): return False


class _MockConn:
    def __init__(self, rows):
        self._rows = rows
        self.closed = False
    def cursor(self):
        return _MockCursor(self._rows)
    def close(self):
        self.closed = True


class _MockDbProvider:
    def __init__(self, rows):
        self._rows = rows
    def _conn(self):
        return _MockConn(self._rows)


def test_db_builder_returns_sql_rows():
    """SQL returns 3 rows → all pass post-filter (non-preferred)."""
    rows = [
        ("005930", 1794, 68000.0, 5.0e11),   # Samsung common
        ("000660", 1800, 120000.0, 2.5e11),  # SK Hynix
        ("035720", 1700, 45000.0, 1.0e11),   # Kakao
    ]
    db = _MockDbProvider(rows)
    universe = build_universe_from_db(db)
    assert len(universe) == 3
    assert "005930" in universe
    assert "035720" in universe


def test_db_builder_filters_preferred():
    """Preferred stocks (code ending 5-9) excluded in post-filter."""
    rows = [
        ("005930", 1794, 68000.0, 5e11),   # common — pass
        ("005935", 1000, 60000.0, 2e10),   # preferred (5) — filter out
        ("005938", 1000, 60000.0, 2e10),   # preferred (8) — filter out
    ]
    db = _MockDbProvider(rows)
    universe = build_universe_from_db(db)
    assert universe == ["005930"]


def test_db_builder_market_filter_via_sector_map():
    """sector_map with market info filters non-allowed markets."""
    rows = [
        ("100000", 500, 5000.0, 5e9),  # KOSPI
        ("200000", 500, 5000.0, 5e9),  # KOSDAQ
        ("300000", 500, 5000.0, 5e9),  # KONEX
    ]
    db = _MockDbProvider(rows)
    sector_map = {
        "100000": {"market": "KOSPI", "sector": "tech"},
        "200000": {"market": "KOSDAQ", "sector": "bio"},
        "300000": {"market": "KONEX", "sector": "small"},
    }
    universe = build_universe_from_db(
        db, allowed_markets=["KOSPI", "KOSDAQ"], sector_map=sector_map,
    )
    assert "100000" in universe
    assert "200000" in universe
    assert "300000" not in universe


def test_db_builder_sector_map_without_market_dict_skipped():
    """sector_map with str values (sector names only, no market) → skip filter.

    Matches CSV builder behavior (line 72 of universe_builder.py).
    """
    rows = [("100000", 500, 5000.0, 5e9)]
    db = _MockDbProvider(rows)
    # sector_map values are strings (sector names, no market info)
    sector_map = {"100000": "IT서비스"}
    universe = build_universe_from_db(
        db, allowed_markets=["KOSPI"], sector_map=sector_map,
    )
    # Not filtered because entry is not dict → universe contains code
    assert "100000" in universe


def test_db_builder_empty_rows():
    """Empty SQL result → empty universe, no crash."""
    db = _MockDbProvider([])
    universe = build_universe_from_db(db, min_count=0)
    assert universe == []


def test_db_builder_query_contains_all_filters():
    """SQL must include min_history, min_close, min_amount filters."""
    captured = {}

    class _CapturingCursor(_MockCursor):
        def execute(self, query, params=None):
            captured["query"] = query
            captured["params"] = params

    class _CapturingConn(_MockConn):
        def cursor(self):
            return _CapturingCursor([])

    class _CapturingDb(_MockDbProvider):
        def _conn(self):
            return _CapturingConn([])

    db = _CapturingDb([])
    build_universe_from_db(
        db, min_close=2500, min_amount=3e9, min_history=300,
    )
    q = captured["query"]
    assert "hist_count >= %(min_history)s" in q
    assert "last_close >= %(min_close)s" in q
    assert "avg_amount_20d >= %(min_amount)s" in q
    assert captured["params"] == {
        "min_history": 300, "min_close": 2500.0, "min_amount": 3e9,
    }


def test_db_builder_query_crash_returns_empty():
    """DB query exception → return [] gracefully."""
    class _CrashingDb:
        def _conn(self):
            raise RuntimeError("DB down")

    universe = build_universe_from_db(_CrashingDb())
    assert universe == []


# ---------- Shadow mode integration principle ----------

def test_shadow_is_lossless_for_production():
    """R4 원칙 — compare_universes NEVER mutates inputs.

    Shadow must never alter the primary (CSV) universe fed downstream.
    """
    csv_list = ["005930", "000660"]
    csv_list_copy = list(csv_list)
    db_list = ["005930", "035720"]
    db_list_copy = list(db_list)
    compare_universes(csv_list, db_list)
    # Lists unchanged
    assert csv_list == csv_list_copy
    assert db_list == db_list_copy


def test_is_preferred_stock_contract_unchanged():
    """R4 must not change preferred-stock detection (CSV+DB must agree)."""
    assert is_preferred_stock("005930") is False  # common
    assert is_preferred_stock("005935") is True   # preferred 5
    assert is_preferred_stock("005939") is True   # preferred 9
    assert is_preferred_stock("005934") is False  # common 4
    assert is_preferred_stock("NOT_DIGIT") is False
    assert is_preferred_stock("12345") is False   # too short
