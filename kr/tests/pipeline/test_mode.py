# -*- coding: utf-8 -*-
"""Unit tests for pipeline.mode."""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest

from pipeline.mode import detect_mode, resolve_trade_date
from pipeline.schema import MODE_LAB, MODE_LIVE, MODE_PAPER_FORWARD


def test_env_var_wins(monkeypatch):
    monkeypatch.setenv("QTRON_MODE", "live")
    assert detect_mode() == MODE_LIVE


def test_env_var_uppercase_tolerated(monkeypatch):
    monkeypatch.setenv("QTRON_MODE", "LIVE")
    assert detect_mode() == MODE_LIVE


def test_env_var_invalid_falls_back_to_default(monkeypatch, caplog):
    monkeypatch.setenv("QTRON_MODE", "martian")
    with caplog.at_level("WARNING"):
        assert detect_mode() == MODE_PAPER_FORWARD
    assert any("PIPELINE_MODE_INVALID" in r.message for r in caplog.records)


def test_default_is_paper_forward(monkeypatch):
    monkeypatch.delenv("QTRON_MODE", raising=False)
    assert detect_mode() == MODE_PAPER_FORWARD


def test_default_override(monkeypatch):
    monkeypatch.delenv("QTRON_MODE", raising=False)
    assert detect_mode(default=MODE_LAB) == MODE_LAB


def test_invalid_default_raises(monkeypatch):
    monkeypatch.delenv("QTRON_MODE", raising=False)
    with pytest.raises(ValueError):
        detect_mode(default="zzz")


# ---------- resolve_trade_date ----------

def test_trade_date_on_weekday_fallback_or_pykrx():
    """Tuesday 2026-04-21 should resolve to itself.

    Accepts pykrx result or Mon-Fri fallback — both should return the
    same day because it's a regular KR business day.
    """
    now = datetime(2026, 4, 21, 10, 0)  # Tuesday
    assert resolve_trade_date(now) == date(2026, 4, 21)


def test_trade_date_on_saturday_resolves_to_friday():
    now = datetime(2026, 4, 25, 10, 0)  # Saturday
    td = resolve_trade_date(now)
    assert td <= date(2026, 4, 24)  # Friday or earlier (e.g. holiday Friday)
    assert td.weekday() < 5


def test_trade_date_on_sunday_resolves_to_prior_weekday():
    now = datetime(2026, 4, 26, 10, 0)  # Sunday
    td = resolve_trade_date(now)
    assert td <= date(2026, 4, 24)  # Friday or earlier
    assert td.weekday() < 5


def test_trade_date_never_future():
    now = datetime(2026, 4, 21, 10, 0)
    td = resolve_trade_date(now)
    assert td <= now.date()
