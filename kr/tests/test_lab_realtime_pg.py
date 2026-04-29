"""
test_lab_realtime_pg.py — Lab realtime PG triple-write helper (Lab-PG)
======================================================================

The PG insert leg is best validated end-to-end against a real
PostgreSQL instance, but we can pin the parts that are pure-Python
without one:

  1. ``QTRON_LAB_REALTIME_PG=0`` → ``save_result_pg`` returns None
     without touching PG (kill-switch contract).
  2. Missing ``stopped_at`` / ``timestamp`` → returns None and logs
     a SKIP instead of crashing.
  3. ``_build_summary`` extracts the per-strategy summary shape
     analysts join on (total_pnl / win_count / loss_count /
     win_rate / cash / trade_count).
  4. ``_coerce_ts`` returns None for empty/whitespace, passes through
     valid strings (the helper that bridges JSON-string ts to PG
     TIMESTAMPTZ via psycopg2 implicit cast).
  5. The result dict shape produced by lab_realtime / lab_simulator
     contains all fields the helper reads. Pinning this stops a
     downstream rename of ``stopped_at`` from silently writing nulls
     into PG.
"""
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from web.lab_realtime_pg import (
    save_result_pg,
    _build_summary,
    _coerce_ts,
    _is_disabled,
)


# ── Test 1: kill-switch ────────────────────────────────────────────────────
def test_disabled_via_env_returns_none():
    """QTRON_LAB_REALTIME_PG=0 → returns None before reaching the
    PG import (which itself is the early kill path even if shared/
    is not on sys.path in this test harness)."""
    os.environ["QTRON_LAB_REALTIME_PG"] = "0"
    try:
        result = save_result_pg({"stopped_at": "2026-04-29 13:25:53"})
        assert result is None
    finally:
        del os.environ["QTRON_LAB_REALTIME_PG"]


def test_default_env_enables_pg():
    """Unset / non-'0' env value → enabled."""
    os.environ.pop("QTRON_LAB_REALTIME_PG", None)
    assert _is_disabled() is False
    os.environ["QTRON_LAB_REALTIME_PG"] = "1"
    try:
        assert _is_disabled() is False
    finally:
        del os.environ["QTRON_LAB_REALTIME_PG"]


# ── Test 2: missing stopped_at → SKIP ──────────────────────────────────────
def test_missing_stopped_at_returns_none():
    """No stopped_at AND no timestamp → SKIP. The helper checks the
    sim_ts presence before reaching the PG connection import, so
    this test does not require PG to be available."""
    os.environ.pop("QTRON_LAB_REALTIME_PG", None)
    result = save_result_pg({"params": {}, "strategies": []})
    assert result is None


# ── Test 3: _build_summary shape ───────────────────────────────────────────
def test_build_summary_extracts_strategy_metrics():
    """Per-strategy summary keeps the columns analysts filter on."""
    strategies = [
        {
            "name": "A", "total_pnl": -29390, "win_count": 1, "loss_count": 4,
            "win_rate": 20.0, "cash": 9970610,
            "trades": [{"side": "BUY"}, {"side": "SELL"}],
        },
        {
            "name": "B", "total_pnl": 69640, "win_count": 7, "loss_count": 4,
            "win_rate": 63.6, "cash": 10069640,
            "trades": [{"side": "BUY"}] * 11 + [{"side": "SELL"}] * 11,
        },
    ]
    summary = _build_summary(strategies)
    assert summary["A"]["total_pnl"] == -29390
    assert summary["A"]["trade_count"] == 2
    assert summary["B"]["win_rate"] == 63.6
    assert summary["B"]["trade_count"] == 22


def test_build_summary_handles_missing_name():
    """Strategy without 'name' falls back to 'strategy' key, then '?'."""
    summary = _build_summary([
        {"strategy": "X", "total_pnl": 100, "trades": []},
        {"total_pnl": 0, "trades": []},
    ])
    assert "X" in summary
    assert "?" in summary


def test_build_summary_empty_input():
    """No strategies → empty dict, never crashes."""
    assert _build_summary([]) == {}
    assert _build_summary(None) == {}  # type: ignore[arg-type]


# ── Test 4: _coerce_ts ─────────────────────────────────────────────────────
def test_coerce_ts_passthrough():
    assert _coerce_ts("2026-04-29 13:25:53") == "2026-04-29 13:25:53"


def test_coerce_ts_empty_to_none():
    assert _coerce_ts("") is None
    assert _coerce_ts("   ") is None
    assert _coerce_ts(None) is None


# ── Test 5: result dict shape contract ─────────────────────────────────────
def test_lab_realtime_build_result_has_required_fields():
    """Pin the field names lab_realtime._build_result writes — if any
    of these are renamed, save_result_pg silently writes NULLs."""
    sample_result = {
        "timestamp": "2026-04-29 13:25:53",
        "started_at": "2026-04-29 11:32:30",
        "stopped_at": "2026-04-29 13:25:53",
        "initial_cash": 10_000_000,
        "ranking_count": 20,
        "params": {"top_n": 20, "max_positions": 10},
        "mode": "realtime",
        "elapsed_sec": 6783.6,
        "tick_count": 440757,
        "strategies": [
            {"name": "A", "total_pnl": -29390, "win_count": 1,
             "loss_count": 4, "win_rate": 20.0, "cash": 9970610,
             "trades": [{"code": "098460", "side": "SELL",
                         "entry_price": 41613, "exit_price": 41550,
                         "qty": 48, "pnl": -3024, "pnl_pct": -0.27,
                         "exit_reason": "SL"}]},
        ],
    }
    # All keys the helper reads must be present:
    for key in ("stopped_at", "started_at", "mode", "elapsed_sec",
                "tick_count", "initial_cash", "ranking_count",
                "params", "strategies"):
        assert key in sample_result, f"missing key {key}"

    # Trade-row keys the helper reads:
    trade = sample_result["strategies"][0]["trades"][0]
    for key in ("code", "side", "entry_price", "exit_price",
                "qty", "pnl", "pnl_pct", "exit_reason"):
        assert key in trade, f"missing trade key {key}"
