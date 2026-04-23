# -*- coding: utf-8 -*-
"""Tests for R26 (C) (2026-04-24) — shared/db/sync_guard helper.

This covers the 9-case truth table for CSV × DB × fetch outcomes:

                      CSV exists     CSV missing (fetch OK)   fetch None
    DB fresh          reused/skip    fetched/skip             missing/skip
    DB stale          reused/upsert  fetched/upsert           missing/skip
    db_writer fails   reused/error   fetched/error            missing/skip

Plus resilience cases: CSV read crash → refetch, db_fresh check crash,
fetcher crash → report.failed=True.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from shared.db.sync_guard import SyncReport, db_sync_guard


class _Stubs:
    """Factory for mock fetcher / csv / db callables. Counts calls."""
    def __init__(self, *, fetcher_result="df_new", csv_read_result="df_csv",
                 db_fresh_result=False, db_writer_rows=1,
                 fetcher_raises=False, csv_read_raises=False,
                 db_fresh_raises=False, db_writer_raises=False,
                 csv_write_raises=False):
        self.fetcher = MagicMock(side_effect=(
            RuntimeError("fetch boom") if fetcher_raises
            else (lambda: fetcher_result)
        ))
        self.csv_reader = MagicMock(side_effect=(
            OSError("read boom") if csv_read_raises
            else (lambda p: csv_read_result)
        ))
        self.csv_writer = MagicMock(side_effect=(
            OSError("write boom") if csv_write_raises
            else (lambda df, p: None)
        ))
        self.db_fresh = MagicMock(side_effect=(
            RuntimeError("fresh boom") if db_fresh_raises
            else (lambda: db_fresh_result)
        ))
        self.db_writer = MagicMock(side_effect=(
            RuntimeError("write boom") if db_writer_raises
            else (lambda df: db_writer_rows)
        ))


def _call(stubs: _Stubs, csv_path: Path, **kw) -> SyncReport:
    return db_sync_guard(
        resource="fundamental",
        identity="20260424",
        csv_path=csv_path,
        fetcher=stubs.fetcher,
        csv_writer=stubs.csv_writer,
        csv_reader=stubs.csv_reader,
        db_fresh=stubs.db_fresh,
        db_writer=stubs.db_writer,
        **kw,
    )


# ── 6-cell truth table (CSV×DB×fetch) ───────────────────────────────

def test_csv_exists_db_stale_upserts(tmp_path):
    """2026-04-23 actual incident: CSV reused + DB upsert required."""
    csv = tmp_path / "fund_20260424.csv"
    csv.write_text("header\nrow\n")
    stubs = _Stubs(db_fresh_result=False, db_writer_rows=42)
    report = _call(stubs, csv)

    assert report.csv_source == "reused"
    assert report.db_action == "upserted"
    assert report.rows == 42
    assert not report.failed
    stubs.fetcher.assert_not_called()
    stubs.csv_reader.assert_called_once()
    stubs.db_writer.assert_called_once()


def test_csv_exists_db_fresh_skips_upsert(tmp_path):
    csv = tmp_path / "fund_20260424.csv"
    csv.write_text("header\nrow\n")
    stubs = _Stubs(db_fresh_result=True)
    report = _call(stubs, csv)

    assert report.csv_source == "reused"
    assert report.db_action == "fresh_skip"
    stubs.fetcher.assert_not_called()
    stubs.db_writer.assert_not_called()


def test_csv_missing_fetch_ok_db_stale_upserts(tmp_path):
    csv = tmp_path / "fund_20260424.csv"
    stubs = _Stubs(fetcher_result="df_new", db_fresh_result=False,
                   db_writer_rows=10)
    report = _call(stubs, csv)

    assert report.csv_source == "fetched"
    assert report.db_action == "upserted"
    assert report.rows == 10
    stubs.fetcher.assert_called_once()
    stubs.csv_writer.assert_called_once()
    stubs.db_writer.assert_called_once()


def test_csv_missing_fetch_ok_db_fresh_skips_upsert(tmp_path):
    """Rare but valid: DB beat CSV to it (e.g. manual ingest)."""
    csv = tmp_path / "fund_20260424.csv"
    stubs = _Stubs(fetcher_result="df_new", db_fresh_result=True)
    report = _call(stubs, csv)

    assert report.csv_source == "fetched"
    assert report.db_action == "fresh_skip"
    stubs.db_writer.assert_not_called()


def test_csv_missing_fetch_none_everything_skipped(tmp_path):
    """Fetcher returns None → no data at all, neither CSV nor DB touched."""
    csv = tmp_path / "fund_20260424.csv"
    stubs = _Stubs(fetcher_result=None)
    report = _call(stubs, csv)

    assert report.csv_source == "missing"
    assert report.db_action == "skipped_no_data"
    assert not report.failed
    stubs.csv_writer.assert_not_called()
    stubs.db_fresh.assert_not_called()
    stubs.db_writer.assert_not_called()
    assert not csv.exists()


def test_csv_exists_db_writer_fails_reported(tmp_path):
    csv = tmp_path / "fund_20260424.csv"
    csv.write_text("x")
    stubs = _Stubs(db_fresh_result=False, db_writer_raises=True)
    report = _call(stubs, csv)

    assert report.csv_source == "reused"
    assert report.db_action == "error"
    assert report.failed
    assert "db_writer_error" in report.reason


# ── Resilience / edge cases ───────────────────────────────────────────

def test_csv_read_crash_falls_back_to_fetch(tmp_path):
    csv = tmp_path / "fund_20260424.csv"
    csv.write_bytes(b"\x00corrupt")
    stubs = _Stubs(csv_read_raises=True, fetcher_result="df_new",
                   db_fresh_result=False, db_writer_rows=3)
    report = _call(stubs, csv)

    # Read failed → fetch invoked → CSV rewritten
    assert report.csv_source == "fetched"
    assert report.db_action == "upserted"
    stubs.fetcher.assert_called_once()
    stubs.csv_writer.assert_called_once()


def test_fetcher_crash_reports_failed(tmp_path):
    """Fetcher raising propagates as report.failed, DB never touched."""
    csv = tmp_path / "fund_20260424.csv"
    stubs = _Stubs(fetcher_raises=True)
    report = _call(stubs, csv)

    assert report.failed
    assert "fetcher_crash" in report.reason
    assert report.db_action == "error"
    stubs.db_fresh.assert_not_called()
    stubs.db_writer.assert_not_called()


def test_db_fresh_crash_reports_failed(tmp_path):
    """has_fundamental_for blowing up must not corrupt the DB."""
    csv = tmp_path / "fund_20260424.csv"
    csv.write_text("x")
    stubs = _Stubs(db_fresh_raises=True)
    report = _call(stubs, csv)

    assert report.failed
    assert "db_fresh_crash" in report.reason
    stubs.db_writer.assert_not_called()


def test_csv_write_soft_fail_still_tries_db(tmp_path):
    """CSV write failure (e.g. disk full) must not block DB upsert —
    DB is the canonical truth; CSV is just a cache."""
    csv = tmp_path / "fund_20260424.csv"
    stubs = _Stubs(fetcher_result="df_new", csv_write_raises=True,
                   db_fresh_result=False, db_writer_rows=7)
    report = _call(stubs, csv)

    assert report.csv_source == "fetched"
    assert report.db_action == "upserted"
    assert report.rows == 7
    assert not report.failed  # CSV is soft-fail only
    assert "csv_write_soft_fail" in (report.reason or "")


def test_report_carries_resource_and_identity(tmp_path):
    """Caller relies on report.resource / .identity for alert payloads."""
    csv = tmp_path / "fund_20260424.csv"
    csv.write_text("x")
    stubs = _Stubs(db_fresh_result=True)
    report = _call(stubs, csv)

    assert report.resource == "fundamental"
    assert report.identity == "20260424"
    assert report.csv_path == csv
