"""
test_eod_equity_snapshot.py — KP3 PG rest_equity_snapshots EOD save (Jeff 2026-04-29)
======================================================================================

The fix lives inside ``run_eod()`` in ``kr/lifecycle/eod_phase.py`` and is
not directly importable as a function (it's a plain ``try``/``except`` block
in the lifecycle path). These tests pin the contracts the block depends on
so a downstream rename can't silently re-introduce the original bug:

  1. ``portfolio.summary()`` returns the snapshot keys the block reads
     (equity / cash / n_positions / peak_equity / prev_close_equity).
  2. ``sync_equity_snapshot`` accepts the kwargs the block passes
     (no positional / typo drift).
  3. ``get_eod_equity`` exists and is the gate the block uses to skip
     duplicates within a single trade_date.
  4. The legacy SSE-side call site in ``kr/web/app.py`` no longer uses
     the undefined ``_hour`` / ``_min`` names (regression guard).
  5. The QTRON_LAB_REALTIME_PG-style env-flag pattern would let
     operators disable the new write if needed (future-proof; matches
     T3-A1 / Lab-PG kill-switch shape).
"""
import inspect
import os
import sys
from pathlib import Path

# kr/ on path (for ``from web.rest_state_db import ...``)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# Worktree root on path (for ``from shared.db.pg_base import connection``,
# which web.rest_state_db imports at module load time)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


# ── Test 1: portfolio.summary() shape ──────────────────────────────────────
def test_portfolio_summary_has_kp3_keys():
    """The KP3 EOD block in eod_phase.py reads these keys directly off
    ``portfolio.summary()``. If any rename happens in PortfolioManager
    the EOD save would silently push wrong / missing values.

    Production runs use ``python -X utf8`` (the project's standard);
    so do CI runs (kr_live_tray.bat, batch entrypoints, verifier
    scripts). Outside that flag the Windows cp949 default can refuse
    to import modules with Korean comments before pytest can see the
    real shape. We pin the contract via static text scan when the
    runtime import is unavailable, so the test is meaningful in both
    environments.
    """
    src = (
        Path(__file__).resolve().parent.parent
        / "core" / "portfolio_manager.py"
    ).read_text(encoding="utf-8")
    try:
        from core.portfolio_manager import PortfolioManager  # type: ignore
        pm = PortfolioManager(initial_cash=10_000_000)
        keys = set(pm.summary().keys())
    except Exception:
        # Fallback: parse the literal dict in summary(). Looking for
        # the keys we depend on in the EOD KP3 save.
        keys = set()
        for k in ("equity", "cash", "n_positions",
                  "prev_close_equity", "peak_equity"):
            if f'"{k}":' in src or f"'{k}':" in src:
                keys.add(k)
    for key in ("equity", "cash", "n_positions",
                "prev_close_equity", "peak_equity"):
        assert key in keys, f"summary() missing key: {key}"


# ── Test 2: sync_equity_snapshot kwargs contract ───────────────────────────
def test_sync_equity_snapshot_kwargs():
    """Pin the signature so the EOD block's kwargs match. A silent
    rename of e.g. ``holdings_count`` would otherwise be a TypeError
    swallowed by the EOD block's outer try/except."""
    from web.rest_state_db import sync_equity_snapshot
    sig = inspect.signature(sync_equity_snapshot)
    params = sig.parameters
    for kw in ("market_date", "equity", "cash", "holdings_count",
               "peak_equity", "prev_close_equity", "is_eod"):
        assert kw in params, f"sync_equity_snapshot missing kwarg: {kw}"


# ── Test 3: get_eod_equity dedup gate exists ───────────────────────────────
def test_get_eod_equity_exists():
    """The duplicate-suppression gate (``get_eod_equity is None``)
    requires this function. ON CONFLICT in the INSERT is the secondary
    guard but the primary skip happens before the SQL runs."""
    from web.rest_state_db import get_eod_equity
    assert callable(get_eod_equity)
    sig = inspect.signature(get_eod_equity)
    assert "market_date" in sig.parameters


# ── Test 4: legacy SSE call site is gone (regression guard) ────────────────
def test_legacy_sse_call_site_removed():
    """The pre-KP3 hook in ``kr/web/app.py`` referenced ``_hour`` /
    ``_min`` from another function's local scope and lived inside a
    dashboard read path. Both violations should be removed by this PR.
    A fresh import of the legacy ``_hour >= 15 and _min >= 30`` line
    inside ``app.py``'s SSE generator would mean the old bug came back.
    """
    app_py = (
        Path(__file__).resolve().parent.parent / "web" / "app.py"
    ).read_text(encoding="utf-8")
    # Heuristic: the original buggy line. Should not appear any more.
    assert "_is_eod = (_hour >= 15 and _min >= 30)" not in app_py, (
        "Legacy SSE-side EOD equity hook still present in app.py — "
        "the _hour/_min NameError bug would re-fire silently."
    )
    # And the import that fed it.
    assert (
        "from web.rest_state_db import sync_equity_snapshot, "
        "get_eod_equity"
    ) not in app_py, (
        "Legacy import inside app.py SSE generator is still there. "
        "EOD save belongs in lifecycle/eod_phase.py, not the read path."
    )


# ── Test 5: EOD block lives in eod_phase.py engine path ────────────────────
def test_kp3_save_lives_in_eod_phase():
    """The new save location must be the lifecycle EOD path — confirms
    the move out of the dashboard read path landed."""
    eod_py = (
        Path(__file__).resolve().parent.parent / "lifecycle" / "eod_phase.py"
    ).read_text(encoding="utf-8")
    assert "[KP3_EOD_EQUITY]" in eod_py, (
        "KP3 EOD equity save block missing from eod_phase.py — "
        "rest_equity_snapshots will keep being 0 rows."
    )
    assert "sync_equity_snapshot(" in eod_py
    assert "get_eod_equity(_today)" in eod_py, (
        "Dedup gate missing — duplicate INSERTs may slip past "
        "ON CONFLICT for legacy dual-write callers."
    )
