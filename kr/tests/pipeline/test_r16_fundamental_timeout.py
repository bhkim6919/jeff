# -*- coding: utf-8 -*-
"""Tests for R16 — Fundamental snapshot hard timeout wrapper.

Background (RCA 20260423 §13 + Phase D):
    2026-04-23 batch hung in Step 5 Fundamental snapshot for 60+ minutes.
    Root cause: fetch_daily_fundamental_naver makes 2770 HTTP requests
    with requests.get(timeout=10) + sleep(0.35). No outer timeout.

    R16 wraps the call with concurrent.futures.ThreadPoolExecutor.result(
    timeout=300) so the batch never blocks > 5 minutes on fundamentals.
    On timeout, returns None — downstream handling already tolerates this.
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from unittest import mock

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

import kr._bootstrap_path  # noqa: F401 — enable shared.* imports


def test_timeout_returns_none_after_threshold():
    """When fetch_daily_snapshot sleeps longer than timeout, wrapper returns None."""
    from lifecycle.batch import _fetch_daily_snapshot_with_timeout
    from data import fundamental_collector

    def _slow_fetch():
        time.sleep(10)  # longer than our test timeout
        return "should_never_return"

    logger = logging.getLogger("test.r16")
    with mock.patch.object(
        fundamental_collector, "fetch_daily_snapshot", _slow_fetch,
    ):
        t0 = time.monotonic()
        result = _fetch_daily_snapshot_with_timeout(logger, timeout_sec=2)
        elapsed = time.monotonic() - t0

    assert result is None
    assert elapsed < 4  # timeout + small overhead


def test_fast_fetch_returns_data():
    """When fetch_daily_snapshot is fast, returns the actual result."""
    from lifecycle.batch import _fetch_daily_snapshot_with_timeout
    from data import fundamental_collector

    def _fast_fetch():
        return "synthetic_df"

    logger = logging.getLogger("test.r16")
    with mock.patch.object(
        fundamental_collector, "fetch_daily_snapshot", _fast_fetch,
    ):
        result = _fetch_daily_snapshot_with_timeout(logger, timeout_sec=10)

    assert result == "synthetic_df"


def test_exception_propagates_to_future_result():
    """Exception in fetch_daily_snapshot surfaces through future.result."""
    from lifecycle.batch import _fetch_daily_snapshot_with_timeout
    from data import fundamental_collector

    class _Boom(RuntimeError):
        pass

    def _bad_fetch():
        raise _Boom("synthetic")

    logger = logging.getLogger("test.r16")
    with mock.patch.object(
        fundamental_collector, "fetch_daily_snapshot", _bad_fetch,
    ):
        with pytest.raises(_Boom):
            _fetch_daily_snapshot_with_timeout(logger, timeout_sec=5)


def test_default_timeout_is_6000_sec():
    """R16 contract: default timeout 100 min (normal ~68min + 30min buffer).

    Measured on 2026-04-23: 1.48s/stock × 2770 stocks = 4099s (68 min).
    Jeff-approved spec: normal + 30 min → round to 100 min (6000s).
    """
    from lifecycle.batch import FUNDAMENTAL_SNAPSHOT_TIMEOUT_SEC
    assert FUNDAMENTAL_SNAPSHOT_TIMEOUT_SEC == 6000


def test_timeout_logs_warning(caplog):
    """Timeout path logs a warning so observable."""
    from lifecycle.batch import _fetch_daily_snapshot_with_timeout
    from data import fundamental_collector

    def _slow():
        time.sleep(10)

    logger = logging.getLogger("test.r16.log")
    with caplog.at_level(logging.WARNING, logger="test.r16.log"):
        with mock.patch.object(
            fundamental_collector, "fetch_daily_snapshot", _slow,
        ):
            _fetch_daily_snapshot_with_timeout(logger, timeout_sec=1)

    assert any(
        "FUND_TIMEOUT" in record.getMessage()
        for record in caplog.records
    )
