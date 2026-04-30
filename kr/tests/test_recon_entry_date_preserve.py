"""Tests for ``_reconcile_with_broker`` BROKER_ONLY ``entry_date`` preservation.

Pin the 2026-04-30 fix: when the engine temporarily lost
``portfolio_state_live.json`` and the disk snapshot was somehow
preserved (or recovered before RECON), the BROKER_ONLY branch should
restore each position's original ``entry_date`` instead of stamping
every position with ``today``.

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_recon_entry_date_preserve.py -v
"""
from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

from lifecycle.reconcile import _reconcile_with_broker  # noqa: E402


# ── Test doubles ─────────────────────────────────────────────────────


def _make_provider(holdings: list[dict],
                   cash: float = 1_000_000) -> MagicMock:
    """Mimic ``provider.query_account_summary()`` shape."""
    provider = MagicMock()
    provider.query_account_summary.return_value = {
        "cash": cash,
        "holdings": holdings,
        "holdings_reliable": True,
        "_status": "COMPLETE",
        "_consistency": "CLEAN",
    }
    return provider


def _make_portfolio(cash: float = 5_000_000):
    """Empty portfolio matching the 04-30 incident scenario where the
    engine started with default cash and zero positions."""
    from core.portfolio_manager import PortfolioManager

    pm = PortfolioManager(
        initial_cash=cash,
        daily_dd_limit=-0.04,
        monthly_dd_limit=-0.07,
        max_positions=20,
    )
    return pm


def _make_holding(code: str, qty: int, avg: float,
                   cur: float | None = None) -> dict:
    return {
        "code": code,
        "name": code,
        "qty": qty,
        "quantity": qty,
        "avg_price": avg,
        "cur_price": cur if cur is not None else avg,
        "pnl": 0.0,
        "pnl_pct": 0.0,
        "market_value": qty * (cur or avg),
    }


# ── Pre-fix behaviour preserved when ``saved_state=None`` ────────────


def test_broker_only_uses_today_when_no_saved_state(caplog):
    """Backward-compat: callers that don't pass ``saved_state`` get the
    pre-2026-04-30 behaviour (entry_date=today for every BROKER_ONLY
    add)."""
    portfolio = _make_portfolio()
    provider = _make_provider([_make_holding("005930", 10, 70_000)])
    logger = logging.getLogger("test")
    today = str(date.today())

    result = _reconcile_with_broker(
        portfolio, provider, logger, saved_state=None,
    )
    assert result["corrections"] >= 1
    pos = portfolio.positions["005930"]
    assert pos.entry_date == today


# ── Post-fix: preserve entry_date from saved_state ───────────────────


def test_broker_only_preserves_entry_date_from_saved_state():
    """The 04-30 reproduction. Engine starts with empty portfolio,
    broker still has 17 positions, but the operator handed RECON the
    last-known-good disk snapshot. Each BROKER_ONLY add should use the
    snapshot's ``entry_date`` instead of today."""
    portfolio = _make_portfolio()
    provider = _make_provider([
        _make_holding("005930", 10, 70_000),
        _make_holding("000660", 5, 130_000),
    ])
    logger = logging.getLogger("test")

    saved_state = {
        "cash": 1_171_709,
        "positions": {
            "005930": {
                "code": "005930",
                "quantity": 10,
                "avg_price": 70_000,
                "entry_date": "2026-04-15",
                "high_watermark": 75_000,
            },
            "000660": {
                "code": "000660",
                "quantity": 5,
                "avg_price": 130_000,
                "entry_date": "2026-04-08",
                "high_watermark": 140_000,
            },
        },
    }

    _reconcile_with_broker(
        portfolio, provider, logger, saved_state=saved_state,
    )

    assert portfolio.positions["005930"].entry_date == "2026-04-15"
    assert portfolio.positions["000660"].entry_date == "2026-04-08"


def test_broker_only_falls_back_to_today_for_unseen_codes():
    """If ``saved_state`` doesn't contain a code (genuine new buy or
    manual broker add), fall back to today — the original semantic."""
    portfolio = _make_portfolio()
    provider = _make_provider([
        _make_holding("005930", 10, 70_000),
        _make_holding("999999", 1, 50_000),  # not in saved_state
    ])
    logger = logging.getLogger("test")
    today = str(date.today())

    saved_state = {
        "cash": 1_171_709,
        "positions": {
            "005930": {
                "code": "005930",
                "quantity": 10,
                "avg_price": 70_000,
                "entry_date": "2026-04-15",
            },
        },
    }

    _reconcile_with_broker(
        portfolio, provider, logger, saved_state=saved_state,
    )
    assert portfolio.positions["005930"].entry_date == "2026-04-15"
    assert portfolio.positions["999999"].entry_date == today


def test_broker_only_rejects_garbage_entry_date():
    """If the saved snapshot is corrupted (empty / non-ISO / wrong
    type), don't poison the new position — fall back to today."""
    portfolio = _make_portfolio()
    provider = _make_provider([
        _make_holding("AAA", 1, 100),
        _make_holding("BBB", 1, 100),
        _make_holding("CCC", 1, 100),
    ])
    logger = logging.getLogger("test")
    today = str(date.today())

    saved_state = {
        "positions": {
            "AAA": {"code": "AAA", "entry_date": ""},          # empty
            "BBB": {"code": "BBB", "entry_date": "garbage"},   # not ISO
            "CCC": {"code": "CCC", "entry_date": None},        # null
        },
    }

    _reconcile_with_broker(
        portfolio, provider, logger, saved_state=saved_state,
    )
    for code in ("AAA", "BBB", "CCC"):
        assert portfolio.positions[code].entry_date == today


def test_broker_only_handles_malformed_saved_state():
    """``saved_state`` should never crash the RECON. Missing
    ``positions`` key, list instead of dict, etc., all fall through to
    today."""
    portfolio = _make_portfolio()
    provider = _make_provider([_make_holding("005930", 10, 70_000)])
    logger = logging.getLogger("test")
    today = str(date.today())

    # Missing 'positions' key.
    _reconcile_with_broker(
        portfolio, provider, logger,
        saved_state={"cash": 100},
    )
    assert portfolio.positions["005930"].entry_date == today


def test_broker_only_handles_non_dict_position_record():
    """Tolerate per-position record being a non-dict (string, list)."""
    portfolio = _make_portfolio()
    provider = _make_provider([
        _make_holding("AAA", 1, 100),
        _make_holding("BBB", 1, 100),
    ])
    logger = logging.getLogger("test")
    today = str(date.today())
    saved_state = {
        "positions": {
            "AAA": "not a dict",
            "BBB": ["also", "not", "a", "dict"],
        },
    }
    _reconcile_with_broker(
        portfolio, provider, logger, saved_state=saved_state,
    )
    assert portfolio.positions["AAA"].entry_date == today
    assert portfolio.positions["BBB"].entry_date == today


# ── Multi-position 04-30 reproduction ────────────────────────────────


def test_broker_only_04_30_full_reproduction():
    """Exact 04-30 incident shape: 17 BROKER_ONLY positions, all
    present in the saved snapshot with varied entry_dates spanning
    several weeks. After RECON, every position has its disk-side
    entry_date preserved — none are reset to today."""
    portfolio = _make_portfolio(cash=5_000_000)  # default initial cash
    today = str(date.today())
    week_ago = str(date.today() - timedelta(days=7))
    month_ago = str(date.today() - timedelta(days=30))

    holdings = [_make_holding(f"{i:06d}", 1, 1000) for i in range(17)]
    provider = _make_provider(holdings, cash=1_171_709)
    logger = logging.getLogger("test")

    # Half of the positions opened a week ago, half a month ago.
    saved_state = {
        "cash": 1_171_709,
        "positions": {
            f"{i:06d}": {
                "code": f"{i:06d}",
                "quantity": 1,
                "avg_price": 1000,
                "entry_date": week_ago if i % 2 == 0 else month_ago,
            }
            for i in range(17)
        },
    }

    _reconcile_with_broker(
        portfolio, provider, logger, saved_state=saved_state,
    )

    # All 17 added.
    assert len(portfolio.positions) == 17
    # Every entry_date matches the snapshot, NONE is today.
    today_count = sum(
        1 for p in portfolio.positions.values() if p.entry_date == today
    )
    week_count = sum(
        1 for p in portfolio.positions.values() if p.entry_date == week_ago
    )
    month_count = sum(
        1 for p in portfolio.positions.values() if p.entry_date == month_ago
    )
    assert today_count == 0, "no position should have entry_date=today"
    # 17 positions: indices 0,2,4,...,16 = 9 even = 9 week_ago
    #                       1,3,5,...,15 = 8 odd  = 8 month_ago
    assert week_count == 9
    assert month_count == 8
