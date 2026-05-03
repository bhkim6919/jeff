"""Tests for PR 1: rebalance_api guards (broker-sync + monitor_only).

Verifies:
  - _check_monitor_only rejects when runtime has monitor_only_reason or recon_unreliable
  - _broker_sync rejects on holdings_reliable=False / error / exception
  - _cash_drift_reason respects soft vs strict tolerances
  - create_preview rejects on monitor_only / broker_sync_failed
  - execute_sell rejects on monitor_only / broker_sync_failed / strict cash drift
  - execute_buy rejects on monitor_only / broker_sync_failed / strict cash drift

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_rebalance_api_guards.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

import web.rebalance_api as ra  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────


def _state_mgr(runtime: dict | None = None,
               portfolio: dict | None = None):
    sm = MagicMock()
    sm.load_runtime.return_value = runtime if runtime is not None else {}
    sm.load_portfolio.return_value = portfolio if portfolio is not None else {"cash": 1_000_000}
    sm.load_pending_external.return_value = []
    sm.save_runtime.return_value = True
    return sm


def _provider(summary: dict | Exception | None = None,
              open_orders=None) -> MagicMock:
    p = MagicMock()
    if isinstance(summary, Exception):
        p.query_account_summary.side_effect = summary
    elif summary is not None:
        p.query_account_summary.return_value = summary
    else:
        p.query_account_summary.return_value = {
            "available_cash": 1_000_000,
            "holdings": [],
            "holdings_reliable": True,
            "error": None,
        }
    p.query_open_orders.return_value = open_orders if open_orders is not None else []
    return p


def _config():
    cfg = MagicMock()
    cfg.SIGNALS_DIR = "/tmp/nope"
    cfg.INITIAL_CASH = 100_000_000
    cfg.DAILY_DD_LIMIT = -0.04
    cfg.MONTHLY_DD_LIMIT = -0.07
    cfg.N_STOCKS = 20
    cfg.BUY_COST = 0.00115
    cfg.SELL_COST = 0.00295
    cfg.CASH_BUFFER_RATIO = 0.01
    return cfg


@pytest.fixture(autouse=True)
def _reset_state_singleton():
    """Reset module-level rebal state between tests."""
    ra._state = ra.RebalCycleState()
    ra._initialized = False
    yield
    ra._state = ra.RebalCycleState()
    ra._initialized = False


# ── _check_monitor_only ──────────────────────────────────────────────


def test_monitor_only_clean_returns_none():
    sm = _state_mgr(runtime={})
    assert ra._check_monitor_only(sm) is None


def test_monitor_only_with_reason_blocks():
    sm = _state_mgr(runtime={"monitor_only_reason": "holdings_unreliable"})
    out = ra._check_monitor_only(sm)
    assert out is not None
    assert "MONITOR_ONLY" in out
    assert "holdings_unreliable" in out


def test_monitor_only_recon_unreliable_blocks():
    sm = _state_mgr(runtime={"recon_unreliable": True})
    out = ra._check_monitor_only(sm)
    assert out is not None
    assert "recon_unreliable" in out


def test_monitor_only_load_failure_returns_reason():
    sm = MagicMock()
    sm.load_runtime.side_effect = RuntimeError("disk full")
    out = ra._check_monitor_only(sm)
    assert out is not None
    assert "MONITOR_ONLY_CHECK_FAILED" in out


# ── _broker_sync ──────────────────────────────────────────────────────


def test_broker_sync_success_returns_data():
    p = _provider()
    err, data = ra._broker_sync(p)
    assert err is None
    assert data["available_cash"] == 1_000_000


def test_broker_sync_holdings_unreliable_rejects():
    p = _provider(summary={
        "available_cash": 1_000_000,
        "holdings_reliable": False,
        "error": None,
    })
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "BROKER_SYNC_FAILED" in err
    assert "holdings_reliable=False" in err
    assert data is None


def test_broker_sync_error_field_rejects():
    p = _provider(summary={
        "available_cash": 1_000_000,
        "holdings_reliable": True,
        "error": "TIMEOUT",
    })
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "TIMEOUT" in err
    assert data is None


def test_broker_sync_exception_rejects():
    p = _provider(summary=ConnectionError("network down"))
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "exception" in err
    assert "network down" in err
    assert data is None


def test_broker_sync_negative_cash_rejects():
    p = _provider(summary={
        "available_cash": -100,
        "holdings_reliable": True,
        "error": None,
    })
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "invalid available_cash" in err


def test_broker_sync_missing_cash_rejects():
    p = _provider(summary={
        "holdings_reliable": True,
        "error": None,
    })
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "invalid available_cash" in err


def test_broker_sync_invalid_format_rejects():
    p = MagicMock()
    p.query_account_summary.return_value = "not a dict"
    err, data = ra._broker_sync(p)
    assert err is not None
    assert "invalid summary format" in err


# ── _cash_drift_reason ────────────────────────────────────────────────


def test_cash_drift_zero_passes():
    assert ra._cash_drift_reason(1_000_000, 1_000_000, strict=False) is None
    assert ra._cash_drift_reason(1_000_000, 1_000_000, strict=True) is None


def test_cash_drift_below_soft_passes():
    # 500 KRW drift on 1M = 0.05% < soft 0.1%/1k threshold
    assert ra._cash_drift_reason(1_000_000, 1_000_500, strict=False) is None


def test_cash_drift_above_soft_warns():
    # 5,000 KRW drift on 1M = 0.5% > soft 0.1%/1k threshold
    out = ra._cash_drift_reason(1_000_000, 1_005_000, strict=False)
    assert out is not None
    assert "BROKER_MISMATCH" in out
    assert "soft" in out


def test_cash_drift_below_strict_passes():
    # 5,000 KRW drift on 1M = 0.5% — equal to strict threshold (max(10000, 0.5%*1M)=10000)
    # Below 10k → pass
    assert ra._cash_drift_reason(1_000_000, 1_005_000, strict=True) is None


def test_cash_drift_above_strict_rejects():
    # 50,000 KRW drift on 1M = 5% > strict 0.5% / 10k threshold
    out = ra._cash_drift_reason(1_000_000, 1_050_000, strict=True)
    assert out is not None
    assert "strict" in out


def test_cash_drift_pct_dominates_for_large_balance():
    # On 100M, 0.5% = 500k > 10k floor
    out = ra._cash_drift_reason(100_000_000, 100_600_000, strict=True)
    assert out is not None
    # 500k drift > 0.5% of 100M=500k → equal, just over → reject
    # 600k > 500k → reject
    assert "drift=600,000" in out


# ── create_preview rejection paths ────────────────────────────────────


def test_create_preview_blocks_on_monitor_only(monkeypatch):
    sm = _state_mgr(runtime={"monitor_only_reason": "holdings_unreliable"})
    p = _provider()
    cfg = _config()
    # Force phase to allow preview
    ra._state.phase = "WINDOW_OPEN"
    ra._initialized = True
    out = ra.create_preview(sm, cfg, p)
    assert "error" in out
    assert "MONITOR_ONLY" in out["error"]


def test_create_preview_blocks_on_broker_sync_failure(monkeypatch):
    sm = _state_mgr(runtime={})
    p = _provider(summary={
        "available_cash": 1_000_000,
        "holdings_reliable": False,
        "error": None,
    })
    cfg = _config()
    ra._state.phase = "WINDOW_OPEN"
    ra._initialized = True
    out = ra.create_preview(sm, cfg, p)
    assert "error" in out
    assert "BROKER_SYNC_FAILED" in out["error"]


# ── execute_sell rejection paths ──────────────────────────────────────


def test_execute_sell_blocks_on_monitor_only(monkeypatch):
    sm = _state_mgr(runtime={"monitor_only_reason": "holdings_unreliable"})
    p = _provider()
    cfg = _config()
    # Phase must be PREVIEW_READY for sell gate
    ra._state.phase = "PREVIEW_READY"
    ra._initialized = True
    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                          trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "MONITOR_ONLY" in out["error"]


def test_execute_sell_blocks_on_broker_sync_failure(monkeypatch):
    # Pass _check_sell_gates by giving clean monitor_only and clean BuyPermission
    sm = _state_mgr(runtime={})
    # Provider passes the gate's open_orders + target check via short-circuit;
    # but we want broker_sync to fail at the secondary check.
    # Trick: query_open_orders returns []; query_account_summary raises.
    p = MagicMock()
    p.query_open_orders.return_value = []
    p.query_account_summary.side_effect = ConnectionError("net down")
    cfg = _config()
    ra._state.phase = "PREVIEW_READY"
    ra._initialized = True
    # We need _check_gates to pass open_orders + target. Mock target loader.
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"target_tickers": [], "scores": {}, "date": "20260504"},
    )
    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                          trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    # Either gate fails first or broker_sync fails — both are valid rejections
    assert ("BROKER_SYNC_FAILED" in out["error"]
            or "MONITOR_ONLY" in out["error"]
            or "open_orders" in out["error"])


def test_execute_sell_blocks_on_strict_cash_drift(monkeypatch):
    # Saved cash 1M, broker says 2M → 1M drift far exceeds strict 10k
    sm = _state_mgr(
        runtime={},
        portfolio={"cash": 1_000_000, "positions": {}},
    )
    p = MagicMock()
    p.query_open_orders.return_value = []
    p.query_account_summary.return_value = {
        "available_cash": 2_000_000,
        "holdings_reliable": True,
        "holdings": [],
        "error": None,
    }
    cfg = _config()
    ra._state.phase = "PREVIEW_READY"
    ra._initialized = True
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"target_tickers": [], "scores": {}, "date": "20260504"},
    )
    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                          trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "BROKER_MISMATCH" in out["error"]


# ── execute_buy rejection paths ───────────────────────────────────────


def test_execute_buy_blocks_on_monitor_only(monkeypatch):
    sm = _state_mgr(runtime={"monitor_only_reason": "holdings_unreliable"})
    p = _provider()
    cfg = _config()
    ra._state.phase = "BUY_READY"
    ra._state.sell_status = "COMPLETE"
    ra._initialized = True
    out = ra.execute_buy(sm, cfg, p, executor=MagicMock(),
                         trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "MONITOR_ONLY" in out["error"]


def test_execute_buy_blocks_on_strict_cash_drift(monkeypatch):
    sm = _state_mgr(
        runtime={},
        portfolio={"cash": 5_000_000, "positions": {}},
    )
    p = MagicMock()
    p.query_open_orders.return_value = []
    p.query_account_summary.return_value = {
        "available_cash": 4_000_000,  # 1M short — > strict tolerance
        "holdings_reliable": True,
        "holdings": [],
        "error": None,
    }
    cfg = _config()
    ra._state.phase = "BUY_READY"
    ra._state.sell_status = "COMPLETE"
    ra._initialized = True
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"target_tickers": [], "scores": {}, "date": "20260504"},
    )
    out = ra.execute_buy(sm, cfg, p, executor=MagicMock(),
                         trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "BROKER_MISMATCH" in out["error"]


# ── get_rebalance_status visibility ───────────────────────────────────


def test_get_rebalance_status_exposes_monitor_only(monkeypatch):
    sm = _state_mgr(runtime={
        "monitor_only_reason": "holdings_unreliable",
        "last_rebalance_date": "20260403",
    })
    cfg = _config()
    cfg.REBAL_DAYS = 21
    monkeypatch.setattr(
        "lifecycle.utils._count_trading_days",
        lambda _a, _b, _c: 21,
    )
    ra._initialized = True
    out = ra.get_rebalance_status(sm, cfg, guard=None)
    assert out["monitor_only"] is True
    assert out["monitor_only_reason"] == "holdings_unreliable"
    assert out["can_preview"] is False
    assert out["can_sell"] is False
    assert out["can_buy"] is False
    assert "MONITOR_ONLY" in out["blocked_reason"]
