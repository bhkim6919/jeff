# -*- coding: utf-8 -*-
"""Tests for R26 (2026-04-24) — CSV/DB independence in batch.py.

Root cause recap (2026-04-23 night):
    batch.py Step 5 (fast) / Step 6 (full) / Step 1 all followed the pattern
    `if csv_path.exists(): skip_everything`. When CSV survived from an earlier
    partial run but the DB upsert had failed or never ran, the next batch
    skipped the upsert entirely — leaving DB stale (fundamental lagged 3 days).

Fix: CSV presence and DB freshness are now judged independently.
    1. CSV exists → reuse DataFrame from CSV.
    2. CSV missing → fetch + write CSV.
    3. DB fresh for date → skip upsert.
    4. DB stale → upsert using whichever DataFrame we have.

This file tests the two helpers in batch.py and a Step 5 smoke flow.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

from lifecycle import batch as batch_mod


class _StubLogger:
    def __init__(self):
        self.msgs = []

    def info(self, msg, *a, **kw):
        self.msgs.append(("info", msg))

    def warning(self, msg, *a, **kw):
        self.msgs.append(("warning", msg))

    def critical(self, msg, *a, **kw):
        self.msgs.append(("critical", msg))

    def has(self, substr: str) -> bool:
        return any(substr in m for _, m in self.msgs)


def _sample_fund_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ticker": ["005930", "000660"],
        "per": [12.5, 8.3],
        "pbr": [1.5, 0.9],
        "eps": [5000, 7000],
        "bps": [50000, 60000],
        "div_yield": [2.1, 1.8],
        "market_cap": [500000000000, 100000000000],
        "foreign_ratio": [52.3, 45.1],
    })


# ── _ensure_fundamental_csv ──────────────────────────────────────────

def test_ensure_csv_reuses_existing_file(tmp_path, monkeypatch):
    """CSV 이미 존재하면 fetch 호출 없이 그대로 읽어 DataFrame 반환."""
    fund_path = tmp_path / "fundamental_20260424.csv"
    _sample_fund_df().to_csv(fund_path, index=False)

    fetch_spy = MagicMock()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout", fetch_spy)

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)

    assert df is not None
    assert len(df) == 2
    assert fetch_spy.call_count == 0, "CSV 존재 시 fetch 호출 금지"
    assert logger.has("CSV reused")


def test_ensure_csv_fetches_when_missing(tmp_path, monkeypatch):
    """CSV 없으면 fetch → write CSV → return DataFrame."""
    fund_path = tmp_path / "fundamental_20260424.csv"
    assert not fund_path.exists()

    sample = _sample_fund_df()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout",
                        lambda _logger: sample)

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)

    assert df is not None
    assert len(df) == 2
    assert fund_path.exists(), "fetch 후 CSV 저장 필수"
    assert logger.has("fetched + saved")


def test_ensure_csv_returns_none_when_fetch_fails(tmp_path, monkeypatch):
    """CSV 없고 fetch timeout/None → None 반환, CSV 생성 안됨."""
    fund_path = tmp_path / "fundamental_20260424.csv"

    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout",
                        lambda _logger: None)

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)

    assert df is None
    assert not fund_path.exists()
    assert logger.has("returned None")


def test_ensure_csv_refetches_on_read_error(tmp_path, monkeypatch):
    """CSV 파일이 깨진 경우 → fetch fallback."""
    fund_path = tmp_path / "fundamental_20260424.csv"
    # 의도적 깨진 CSV (pandas parse error 를 위해 바이너리 쓰기)
    fund_path.write_bytes(b"\x00\x01\x02not_a_csv\xff\xfe")

    sample = _sample_fund_df()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout",
                        lambda _logger: sample)

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)

    # 읽기 실패 시에도 fetch 로 복구
    assert df is not None
    assert len(df) == 2


# ── _ensure_fundamental_db ──────────────────────────────────────────

def test_ensure_db_upserts_when_stale(monkeypatch):
    """DB 에 해당 date 행 없음 → upsert_fundamental 호출됨."""
    calls = {"has": 0, "upsert": 0, "upsert_args": None}

    class _StubDb:
        def has_fundamental_for(self, date_str):
            calls["has"] += 1
            return False

        def upsert_fundamental(self, date_str, df):
            calls["upsert"] += 1
            calls["upsert_args"] = (date_str, len(df))
            return len(df)

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    batch_mod._ensure_fundamental_db("20260424", _sample_fund_df(), logger)

    assert calls["has"] == 1
    assert calls["upsert"] == 1, "DB stale → upsert 필수 (R26 핵심)"
    assert calls["upsert_args"] == ("20260424", 2)
    assert logger.has("upsert: 2 rows")


def test_ensure_db_skips_when_fresh(monkeypatch):
    """DB 에 이미 해당 date 행 있음 → upsert_fundamental 호출 금지."""
    calls = {"has": 0, "upsert": 0}

    class _StubDb:
        def has_fundamental_for(self, date_str):
            calls["has"] += 1
            return True

        def upsert_fundamental(self, date_str, df):
            calls["upsert"] += 1
            return 0

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    batch_mod._ensure_fundamental_db("20260424", _sample_fund_df(), logger)

    assert calls["has"] == 1
    assert calls["upsert"] == 0, "DB fresh 면 중복 upsert 금지"
    assert logger.has("already fresh")


def test_ensure_db_swallows_connection_error(monkeypatch):
    """DB 연결 실패 시 batch 를 abort 하지 않고 warning 만 로그."""
    class _StubDb:
        def has_fundamental_for(self, date_str):
            raise ConnectionError("PG down")

        def upsert_fundamental(self, date_str, df):
            pytest.fail("upsert 호출되면 안됨")

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    # 예외가 helper 를 뚫고 나오면 테스트 실패
    batch_mod._ensure_fundamental_db("20260424", _sample_fund_df(), logger)

    assert logger.has("DB save failed")


# ── R26 핵심 시나리오 (regression) ────────────────────────────────────

def test_r26_csv_exists_db_stale_still_upserts(tmp_path, monkeypatch):
    """★ 2026-04-23 오늘 3일 stale 사건 재현 테스트.

    조건: CSV 는 이미 존재 (어제 fetch 성공), DB 에는 아직 없음.
    기존 버그: csv_path.exists() → 전체 skip → DB stale 3일 방치.
    R26 수정: CSV 재사용 + DB 독립 판정 → upsert 실행.
    """
    fund_path = tmp_path / "fundamental_20260424.csv"
    _sample_fund_df().to_csv(fund_path, index=False)

    fetch_spy = MagicMock()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout", fetch_spy)

    upsert_called = {"n": 0, "date": None, "rows": 0}

    class _StubDb:
        def has_fundamental_for(self, date_str):
            return False  # DB stale

        def upsert_fundamental(self, date_str, df):
            upsert_called["n"] += 1
            upsert_called["date"] = date_str
            upsert_called["rows"] = len(df)
            return len(df)

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)
    batch_mod._ensure_fundamental_db("20260424", df, logger)

    # 재사용(fetch 호출 없음) + upsert 실행 — 두 독립 경로
    assert fetch_spy.call_count == 0
    assert upsert_called["n"] == 1, "CSV 재사용해도 DB stale 이면 반드시 upsert"
    assert upsert_called["date"] == "20260424"
    assert upsert_called["rows"] == 2


def test_r26_csv_missing_db_stale_full_pipeline(tmp_path, monkeypatch):
    """CSV 없음 + DB stale → fetch + CSV write + DB upsert 세 경로 모두 실행."""
    fund_path = tmp_path / "fundamental_20260424.csv"
    assert not fund_path.exists()

    sample = _sample_fund_df()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout",
                        lambda _logger: sample)

    upsert_count = {"n": 0}

    class _StubDb:
        def has_fundamental_for(self, date_str):
            return False

        def upsert_fundamental(self, date_str, df):
            upsert_count["n"] += 1
            return len(df)

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)
    batch_mod._ensure_fundamental_db("20260424", df, logger)

    assert df is not None
    assert fund_path.exists(), "CSV 저장 필수"
    assert upsert_count["n"] == 1, "DB upsert 필수"


def test_r26_csv_exists_db_fresh_no_duplicate_work(tmp_path, monkeypatch):
    """CSV 존재 + DB fresh (어제 정상 완료) → fetch/upsert 둘 다 호출 금지."""
    fund_path = tmp_path / "fundamental_20260424.csv"
    _sample_fund_df().to_csv(fund_path, index=False)

    fetch_spy = MagicMock()
    monkeypatch.setattr(batch_mod, "_fetch_daily_snapshot_with_timeout", fetch_spy)

    upsert_spy = MagicMock()

    class _StubDb:
        def has_fundamental_for(self, date_str):
            return True

        def upsert_fundamental(self, date_str, df):
            upsert_spy(date_str, df)

    monkeypatch.setattr("data.db_provider.DbProvider", lambda: _StubDb())

    logger = _StubLogger()
    df = batch_mod._ensure_fundamental_csv(fund_path, "20260424", logger)
    batch_mod._ensure_fundamental_db("20260424", df, logger)

    assert fetch_spy.call_count == 0
    assert upsert_spy.call_count == 0, "모두 fresh → 중복 작업 없음"
