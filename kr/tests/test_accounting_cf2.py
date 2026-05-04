"""PR-CF2 tests — Modified Dietz cashflow-aware return + DD engine.

Covers Jeff's mandatory stop-condition case:
  initial 5,000,000
  deposit +1,000,000
  withdrawal -500,000
  trading pnl +200,000
  expected broker equity = 5,700,000
  expected trading pnl = +200,000
  DD must NOT change due to deposit/withdrawal alone

Plus structural tests (validation, idempotent replay, raw equity untouched).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from accounting import (  # noqa: E402
    CashflowEvent,
    EventType,
    ModifiedDietzResult,
    compute_modified_dietz_returns,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


# ─── Mandatory stop-condition case (Jeff specified) ──────────────


def test_mandatory_case_5m_deposit_1m_withdrawal_500k_trading_200k():
    """The exact case Jeff specified as the merge stop condition.

    Day 0  open: 5,000,000   (baseline)
    Day 1: +1,000,000 deposit, equity 5,000,000 -> 6,000,000
    Day 2: trading +100,000, equity 6,000,000 -> 6,100,000
    Day 3: -500,000 withdrawal, equity 6,100,000 -> 5,600,000
    Day 4: trading +100,000, equity 5,600,000 -> 5,700,000

    Expected:
      final raw equity = 5,700,000
      cumulative trading PnL = +200,000
      DD must NOT register a drawdown from the deposit/withdrawal alone
        (cumulative return monotonically increases from trading-only
         days, so max_drawdown == 0)
    """
    equity_series = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 6_000_000),  # +1M deposit, 0 trading
        ("2026-04-17", 6_100_000),  # +100K trading
        ("2026-04-18", 5_600_000),  # -500K withdrawal, 0 trading
        ("2026-04-19", 5_700_000),  # +100K trading
    ]
    cashflows = [
        CashflowEvent(
            event_date="2026-04-16", type=EventType.DEPOSIT,
            amount=1_000_000, source="kakao_bank",
        ),
        CashflowEvent(
            event_date="2026-04-18", type=EventType.WITHDRAWAL,
            amount=500_000, source="kakao_bank",
        ),
    ]

    result = compute_modified_dietz_returns(
        equity_series=equity_series,
        cashflow_events=cashflows,
        initial_capital=5_000_000,
    )

    # Final raw equity matches input (broker truth)
    assert result.final_raw_equity == 5_700_000

    # Cumulative trading PnL = exactly the trading additions (NOT the cashflows)
    assert result.cumulative_trading_pnl == 200_000, (
        f"trading PnL must equal +200,000 (sum of trading-only days), "
        f"got {result.cumulative_trading_pnl}"
    )

    # Net external flow recorded
    assert result.net_external_flow == 500_000  # +1M - 500K

    # Invested capital
    assert result.final_invested_capital == 5_500_000  # 5M + 500K net

    # DD must be 0 — cumulative return curve only goes up (trading days),
    # cashflow days have 0 return → no peak retraced
    assert result.max_drawdown == 0.0, (
        f"DD must NOT register on deposit/withdrawal-only days; "
        f"got max_drawdown={result.max_drawdown}"
    )

    # Daily breakdown sanity
    assert len(result.daily) == 4
    # Day 1: 1M deposit, 0 trading
    d1 = result.daily[0]
    assert d1.cashflow == 1_000_000
    assert d1.trading_pnl == 0
    assert d1.daily_return == 0.0
    # Day 2: 0 cashflow, 100K trading
    d2 = result.daily[1]
    assert d2.cashflow == 0
    assert d2.trading_pnl == 100_000
    # Day 3: -500K withdrawal, 0 trading
    d3 = result.daily[2]
    assert d3.cashflow == -500_000
    assert d3.trading_pnl == 0
    assert d3.daily_return == 0.0
    # Day 4: 0 cashflow, 100K trading
    d4 = result.daily[3]
    assert d4.cashflow == 0
    assert d4.trading_pnl == 100_000


# ─── Drawdown computed on cumulative-return curve, not raw ─────────


def test_drawdown_computed_on_cashflow_aware_cumulative_return():
    """A pure-trading drawdown must register; a deposit-only equity
    move must NOT register as DD."""
    equity_series = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 5_500_000),  # +500K trading (+10%)
        ("2026-04-17", 5_000_000),  # -500K trading (~-9.09%)
        ("2026-04-18", 6_000_000),  # +1M deposit, 0 trading
        ("2026-04-19", 6_000_000),  # 0 trading (flat)
    ]
    cashflows = [
        CashflowEvent(
            event_date="2026-04-18", type=EventType.DEPOSIT, amount=1_000_000, source="x",
        ),
    ]
    result = compute_modified_dietz_returns(
        equity_series=equity_series,
        cashflow_events=cashflows,
        initial_capital=5_000_000,
    )
    # Trading PnL = 500K - 500K + 0 + 0 = 0
    assert result.cumulative_trading_pnl == 0
    # DD: peak +10% on day 2, retraced to ~0% on day 3 → DD ~= -9%
    assert result.max_drawdown < -0.05, f"expected real DD, got {result.max_drawdown}"
    assert result.max_drawdown_date == "2026-04-17"


def test_pure_deposit_does_not_create_dd():
    """If trading PnL is exactly 0 every day, DD must be exactly 0
    regardless of how many deposits/withdrawals happen."""
    equity_series = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 6_000_000),  # +1M deposit
        ("2026-04-17", 4_000_000),  # -2M withdrawal
        ("2026-04-18", 4_000_000),  # flat
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000, source="x"),
        CashflowEvent(event_date="2026-04-17", type=EventType.WITHDRAWAL, amount=2_000_000, source="x"),
    ]
    result = compute_modified_dietz_returns(
        equity_series=equity_series,
        cashflow_events=cashflows,
        initial_capital=5_000_000,
    )
    assert result.cumulative_trading_pnl == 0
    assert result.cumulative_return == 0.0
    assert result.max_drawdown == 0.0


# ─── Replay determinism ─────────────────────────────────────────


def test_replay_returns_deterministic_result():
    equity_series = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 6_000_000),
        ("2026-04-17", 6_100_000),
        ("2026-04-18", 5_600_000),
        ("2026-04-19", 5_700_000),
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000, source="x"),
        CashflowEvent(event_date="2026-04-18", type=EventType.WITHDRAWAL, amount=500_000, source="x"),
    ]
    r1 = compute_modified_dietz_returns(equity_series, cashflows, 5_000_000)
    r2 = compute_modified_dietz_returns(list(equity_series), list(cashflows), 5_000_000)
    assert r1 == r2


# ─── Validation (fail-closed) ────────────────────────────────────


def test_empty_series_raises():
    with pytest.raises(ValueError, match="at least one"):
        compute_modified_dietz_returns([], [], 5_000_000)


def test_unsorted_dates_rejected():
    with pytest.raises(ValueError, match="ascending"):
        compute_modified_dietz_returns(
            [("2026-04-15", 5_000_000), ("2026-04-14", 4_000_000)],
            [], 5_000_000,
        )


def test_duplicate_dates_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        compute_modified_dietz_returns(
            [("2026-04-15", 5_000_000), ("2026-04-15", 5_100_000)],
            [], 5_000_000,
        )


def test_negative_equity_rejected():
    with pytest.raises(ValueError, match="negative"):
        compute_modified_dietz_returns(
            [("2026-04-15", -1)],
            [], 5_000_000,
        )


def test_invalid_initial_capital_rejected():
    with pytest.raises(ValueError, match="positive"):
        compute_modified_dietz_returns([("2026-04-15", 5_000_000)], [], 0)


# ─── Single-point series (degenerate but legal) ──────────────────


def test_single_point_series_yields_zero_metrics():
    """One equity point = baseline only. No daily returns to compute."""
    result = compute_modified_dietz_returns(
        [("2026-04-15", 5_000_000)],
        [],
        5_000_000,
    )
    assert result.daily == []
    assert result.cumulative_return == 0.0
    assert result.cumulative_trading_pnl == 0
    assert result.max_drawdown == 0.0
    assert result.final_raw_equity == 5_000_000
    assert result.final_invested_capital == 5_000_000


# ─── Hard restrictions still hold ────────────────────────────────


def test_raw_equity_files_untouched_by_cf2_engine(tmp_path: Path):
    """Computing returns must not mutate any raw equity / state files."""
    raw_paths = [
        REPO_ROOT / "kr" / "data" / "lab_live" / "equity.json",
        REPO_ROOT / "kr" / "data" / "lab_live" / "head.json",
    ]
    snapshots: dict[Path, bytes] = {}
    for p in raw_paths:
        if p.exists():
            snapshots[p] = p.read_bytes()

    compute_modified_dietz_returns(
        [("2026-04-15", 5_000_000), ("2026-04-16", 5_100_000)],
        [],
        5_000_000,
    )
    for p, snap in snapshots.items():
        assert p.read_bytes() == snap, f"CF2 engine mutated raw equity file: {p}"


def test_cf2_module_does_not_import_deprecated_capital_events():
    import re
    accounting_dir = REPO_ROOT / "kr" / "accounting"
    forbidden_import_patterns = [
        re.compile(r"^\s*from\s+(?:kr\.)?finance\.capital_events\b", re.MULTILINE),
        re.compile(r"^\s*from\s+(?:kr\.)?finance\._deprecated_capital_events\b", re.MULTILINE),
        re.compile(r"^\s*import\s+(?:kr\.)?finance\.(?:_deprecated_)?capital_events\b", re.MULTILINE),
    ]
    offenders: list[str] = []
    for f in accounting_dir.rglob("*.py"):
        text = f.read_text(encoding="utf-8")
        for pat in forbidden_import_patterns:
            if pat.search(text):
                offenders.append(f"{f.relative_to(REPO_ROOT)}")
    assert not offenders, f"CF2 imported deprecated module: {offenders}"


def test_anti_pattern_regression_still_passes():
    """Re-run CF0 regression as child process — CF2 must not reintroduce
    `adjust_equity` or related anti-patterns."""
    result = subprocess.run(
        [
            sys.executable,
            "-m", "pytest",
            "tests/test_no_raw_minus_cashflow_pattern.py",
            "-q", "--tb=short",
        ],
        cwd=str(REPO_ROOT / "kr"),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"CF0 regression failed after CF2 changes.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_cf2_does_not_modify_existing_dd_path():
    """exposure_guard.py and other production DD paths must remain
    untouched by CF2. CF2 is a separate engine, not a replacement.

    Static check: no actual imports of exposure_guard / risk_management
    from inside kr/accounting/. Doctrine docstrings that mention these
    names by way of explanation are allowed.
    """
    import re
    accounting_dir = REPO_ROOT / "kr" / "accounting"
    forbidden_import = re.compile(
        r"^\s*(?:from\s+\S*(?:exposure_guard|risk_management)\b|"
        r"import\s+\S*(?:exposure_guard|risk_management)\b)",
        re.MULTILINE,
    )
    offenders: list[str] = []
    for f in accounting_dir.rglob("*.py"):
        text = f.read_text(encoding="utf-8")
        for m in forbidden_import.finditer(text):
            line_no = text[: m.start()].count("\n") + 1
            offenders.append(f"{f.relative_to(REPO_ROOT)}:{line_no}")
    assert not offenders, (
        "CF2 must not import exposure_guard / risk_management.\n"
        f"Offenders: {offenders}"
    )

    # Sanity: production DD path file is not mutated by this PR
    # (verified at PR review level — the diff only touches kr/accounting/
    # and kr/tests/test_accounting_cf2.py). Static-only check here.
    risk_files = [
        REPO_ROOT / "kr" / "risk" / "exposure_guard.py",
        REPO_ROOT / "kr" / "risk" / "risk_management.py",
    ]
    for rf in risk_files:
        assert rf.exists(), f"Pre-existing risk file missing: {rf}"
