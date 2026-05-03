"""Tests for PR 2 (AUD-P1-E): get_current_price failure logging + streak.

Verifies:
  - 0.0 return semantics PRESERVED on failure (backward compat for all
    existing callers' `if p > 0` skip pattern)
  - _price_fail_streak increments on every failure
  - _price_fail_streak resets on success
  - [PRICE_FAIL] log emitted on first failure + every 5th
  - [PRICE_RECOVER] log emitted when streak clears
  - get_price_fail_streak / get_price_fail_total accessors

Caller backward-compat assertion: this PR explicitly does NOT change the
return type. Callers in:
  - kr/lifecycle/eod_phase.py:149 (if p > 0)
  - kr/web/rebalance_api.py:360 (if p > 0)
  - kr/web/rebalance_api.py:629 (if price <= 0)
  - kr/runtime/order_executor.py:73 (returns directly to wrappers)
must continue to behave identically.

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_rest_provider_price_fail_streak.py -v
"""
from __future__ import annotations

import logging
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))


@pytest.fixture
def fake_provider():
    """Build a KiwoomRestProvider instance with auth + dependencies stubbed."""
    from data import rest_provider as rp_mod

    # Stub TokenManager + load_dotenv to avoid env file requirement
    with patch.object(rp_mod, "load_dotenv", lambda *a, **kw: None), \
         patch.object(rp_mod, "TokenManager") as TM, \
         patch.dict("os.environ", {
             "KIWOOM_APP_KEY": "k",
             "KIWOOM_APP_SECRET": "s",
             "KIWOOM_ACCOUNT": "1234567890",
             "KIWOOM_API_URL": "http://stub",
         }, clear=False):
        TM.return_value = MagicMock()
        prov = rp_mod.KiwoomRestProvider(server_type="MOCK")
    # Stub _request so get_current_price doesn't actually hit network
    prov._request = MagicMock()
    # Force propagation so pytest caplog can observe
    prev_propagate = rp_mod.logger.propagate
    rp_mod.logger.propagate = True
    yield prov
    rp_mod.logger.propagate = prev_propagate


# ── Backward-compat: 0.0 return on failure ────────────────────────────


def test_get_current_price_returns_zero_on_return_code_failure(fake_provider):
    fake_provider._request.return_value = {"return_code": -1}
    out = fake_provider.get_current_price("005930")
    assert out == 0.0


def test_get_current_price_returns_zero_on_empty_orderbook(fake_provider):
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "0",
        "sel_fpr_bid": "0",
    }
    out = fake_provider.get_current_price("005930")
    assert out == 0.0


def test_get_current_price_returns_midpoint_on_success(fake_provider):
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "70000",
        "sel_fpr_bid": "70200",
    }
    out = fake_provider.get_current_price("005930")
    assert out == 70100.0


def test_get_current_price_returns_buy_when_only_buy(fake_provider):
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "70000",
        "sel_fpr_bid": "0",
    }
    out = fake_provider.get_current_price("005930")
    assert out == 70000.0


# ── Streak counter behavior ───────────────────────────────────────────


def test_streak_starts_zero(fake_provider):
    assert fake_provider.get_price_fail_streak() == 0
    assert fake_provider.get_price_fail_total() == 0


def test_streak_increments_on_failure(fake_provider):
    fake_provider._request.return_value = {"return_code": -1}
    for _ in range(3):
        fake_provider.get_current_price("005930")
    assert fake_provider.get_price_fail_streak() == 3
    assert fake_provider.get_price_fail_total() == 3


def test_streak_resets_on_success(fake_provider):
    # 2 failures
    fake_provider._request.return_value = {"return_code": -1}
    fake_provider.get_current_price("005930")
    fake_provider.get_current_price("005930")
    assert fake_provider.get_price_fail_streak() == 2
    # Then success
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "100",
        "sel_fpr_bid": "100",
    }
    fake_provider.get_current_price("005930")
    assert fake_provider.get_price_fail_streak() == 0
    # But total persists
    assert fake_provider.get_price_fail_total() == 2


def test_total_does_not_decrement_on_success(fake_provider):
    fake_provider._request.return_value = {"return_code": -1}
    fake_provider.get_current_price("005930")
    fake_provider.get_current_price("005930")
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "100",
        "sel_fpr_bid": "100",
    }
    fake_provider.get_current_price("005930")
    assert fake_provider.get_price_fail_total() == 2


# ── Logging behavior (throttled) ──────────────────────────────────────


def test_logs_on_first_failure(fake_provider, caplog):
    caplog.set_level(logging.WARNING, logger="gen4.rest")
    fake_provider._request.return_value = {"return_code": -1}
    fake_provider.get_current_price("005930")
    fail_logs = [r for r in caplog.records if "[PRICE_FAIL]" in r.message]
    assert len(fail_logs) == 1
    assert "streak=1" in fail_logs[0].message
    assert "total=1" in fail_logs[0].message


def test_logs_throttled_to_every_fifth(fake_provider, caplog):
    caplog.set_level(logging.WARNING, logger="gen4.rest")
    fake_provider._request.return_value = {"return_code": -1}
    for _ in range(11):
        fake_provider.get_current_price("005930")
    # Expect logs at streak=1, 6, 11
    fail_logs = [r for r in caplog.records if "[PRICE_FAIL]" in r.message]
    assert len(fail_logs) == 3
    assert "streak=1" in fail_logs[0].message
    assert "streak=6" in fail_logs[1].message
    assert "streak=11" in fail_logs[2].message


def test_logs_recovery_after_streak(fake_provider, caplog):
    caplog.set_level(logging.INFO, logger="gen4.rest")
    fake_provider._request.return_value = {"return_code": -1}
    fake_provider.get_current_price("005930")
    fake_provider.get_current_price("005930")
    # Recover
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "100",
        "sel_fpr_bid": "100",
    }
    fake_provider.get_current_price("005930")
    recover_logs = [r for r in caplog.records if "[PRICE_RECOVER]" in r.message]
    assert len(recover_logs) == 1
    assert "streak_cleared=2" in recover_logs[0].message


def test_no_recover_log_if_no_prior_streak(fake_provider, caplog):
    caplog.set_level(logging.INFO, logger="gen4.rest")
    fake_provider._request.return_value = {
        "return_code": 0,
        "buy_fpr_bid": "100",
        "sel_fpr_bid": "100",
    }
    fake_provider.get_current_price("005930")
    fake_provider.get_current_price("005930")
    recover_logs = [r for r in caplog.records if "[PRICE_RECOVER]" in r.message]
    assert recover_logs == []


# ── Thread-safety smoke test ──────────────────────────────────────────


def test_concurrent_calls_do_not_corrupt_counter(fake_provider):
    fake_provider._request.return_value = {"return_code": -1}

    def _hammer():
        for _ in range(100):
            fake_provider.get_current_price("005930")

    threads = [threading.Thread(target=_hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Streak == total == 400 (all failures)
    assert fake_provider.get_price_fail_streak() == 400
    assert fake_provider.get_price_fail_total() == 400
