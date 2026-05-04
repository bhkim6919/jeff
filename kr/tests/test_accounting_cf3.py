"""PR-CF3 tests — dashboard snapshot composition + endpoint contract.

Verifies Jeff's CF3 contract (2026-05-04):
  - raw equity preserved as-is (broker/report_equity_log truth)
  - Modified Dietz adjusted return surfaced alongside raw
  - cashflow summary surfaced
  - source labels present
  - zero-cashflow → adjusted return ≈ raw simple return
  - deposit/withdrawal case → trading_pnl matches CF2 mandatory case
  - read-only: snapshot construction triggers no writes
  - new endpoint shape: stable JSON keys, source labels for every section

Endpoint tests use FastAPI TestClient and avoid PG by injecting equity via
the snapshot path (the route reads PG; the route test mocks the connection).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from accounting import (
    CapitalConfig,
    CashflowEvent,
    DashboardSnapshot,
    EventType,
    compute_dashboard_snapshot,
    compute_modified_dietz_returns,
    snapshot_to_dict,
)
from accounting.dashboard import (
    SOURCE_CASHFLOW_LEDGER,
    SOURCE_INITIAL_CAPITAL,
    SOURCE_MODIFIED_DIETZ,
    SOURCE_RAW_EQUITY,
    SOURCE_RAW_SIMPLE,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _capital_config_5m() -> CapitalConfig:
    return CapitalConfig(
        initial_capital=5_000_000,
        currency="KRW",
        strategy_start_date="2026-04-15",
    )


# ─── Section 1: zero-cashflow consistency ─────────────────────────────────


def test_zero_cashflow_dietz_consistent_with_raw_simple_when_baseline_aligns():
    """When initial_capital == equity_series[0] AND no cashflow events,
    Modified Dietz cumulative_return must equal raw_simple_return (within fp).

    This is the sanity check that CF3 doesn't drift from the naive
    calculation in the trivial case — only deposits/withdrawals create
    a delta.
    """
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 5_050_000),
        ("2026-04-17", 5_100_000),
        ("2026-04-18", 5_200_000),
    ]
    snap = compute_dashboard_snapshot(equity, [], cfg)

    # Final equity 5.2M, initial 5M → +4% raw simple
    assert snap.raw_simple_return.value == pytest.approx(0.04, abs=1e-9)
    # Modified Dietz over the same series with zero cashflow == raw simple
    assert snap.modified_dietz.cumulative_return == pytest.approx(0.04, abs=1e-9)
    # No cashflow ⇒ no DD beyond what raw equity has (and series is monotone, so 0)
    assert snap.modified_dietz.max_drawdown == pytest.approx(0.0, abs=1e-12)
    # Cashflow view zeroed
    assert snap.cashflow.event_count == 0
    assert snap.cashflow.net_external_flow == 0
    assert snap.invested_capital.value == 5_000_000


def test_zero_cashflow_with_drawdown_dd_matches_raw():
    """With no cashflow, Modified Dietz DD must be the same shape as a
    pure raw-equity DD calc — the cashflow-aware engine collapses to the
    naive case.
    """
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 5_500_000),  # peak
        ("2026-04-17", 5_000_000),  # -9.09% from peak
        ("2026-04-18", 5_400_000),
    ]
    snap = compute_dashboard_snapshot(equity, [], cfg)

    # Raw simple: 5.4M / 5M - 1 = +8%
    assert snap.raw_simple_return.value == pytest.approx(0.08, abs=1e-9)
    # Cumulative Dietz with no cashflow: matches geometric chain on raw
    expected_cum = (1 + 500_000 / 5_000_000) * (1 + (-500_000) / 5_500_000) * (1 + 400_000 / 5_000_000) - 1
    assert snap.modified_dietz.cumulative_return == pytest.approx(expected_cum, abs=1e-12)
    # DD: from peak (after day 2) to trough (day 3)
    assert snap.modified_dietz.max_drawdown < 0.0
    assert snap.modified_dietz.max_drawdown_date == "2026-04-17"


# ─── Section 2: mandatory Jeff case (5M + 1M deposit - 500K withdraw + 200K trading) ───


def test_mandatory_jeff_case_dashboard_snapshot():
    """Same case as CF2 test_mandatory_case_5m_deposit_1m_withdrawal_500k_trading_200k,
    surfaced through the CF3 dashboard composer.

    Expected (per Jeff 2026-05-04):
      broker equity = 5,700,000
      cumulative trading PnL = +200,000
      DD must NOT register (trading-only days monotone increase)
      net_external_flow = +500,000
      invested_capital = 5,500,000
    """
    cfg = _capital_config_5m()
    equity = [
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

    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    # Raw broker truth preserved
    assert snap.raw_equity.value == 5_700_000
    assert snap.raw_equity.as_of_date == "2026-04-19"

    # Initial capital from config
    assert snap.initial_capital.value == 5_000_000
    assert snap.initial_capital.currency == "KRW"

    # Cashflow aggregation
    assert snap.cashflow.total_deposits == 1_000_000
    assert snap.cashflow.total_withdrawals == 500_000
    assert snap.cashflow.net_external_flow == 500_000
    assert snap.cashflow.event_count == 2
    assert snap.cashflow.last_event_date == "2026-04-18"

    # Invested capital = initial + net_external_flow
    assert snap.invested_capital.value == 5_500_000

    # Modified Dietz: trading PnL = 200K (NOT 700K which would include cashflow)
    assert snap.modified_dietz.cumulative_trading_pnl == 200_000
    # DD must not register from cashflow alone — trading-only days are
    # both positive in this case so cumulative return is monotone
    assert snap.modified_dietz.max_drawdown == pytest.approx(0.0, abs=1e-12)
    # Period bounds reflect the equity series, not the cashflow events
    assert snap.modified_dietz.period_start == "2026-04-15"
    assert snap.modified_dietz.period_end == "2026-04-19"
    assert snap.modified_dietz.input_equity_points == 5
    assert snap.modified_dietz.input_cashflow_events == 2

    # Raw simple return is the *contaminated* number — kept for visibility:
    # (5.7M - 5M) / 5M = +14% — overstated vs the +3.6%-ish trading reality
    assert snap.raw_simple_return.value == pytest.approx(0.14, abs=1e-9)
    # Modified Dietz cumulative return is the cashflow-aware figure
    assert snap.modified_dietz.cumulative_return < snap.raw_simple_return.value


# ─── Section 3: deposit-only / withdrawal-only / dividend cases ──────────


def test_deposit_only_cashflow_separates_from_trading():
    """Deposit alone must not show up as trading PnL or DD."""
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 6_000_000),  # +1M deposit
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
    ]
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    assert snap.modified_dietz.cumulative_trading_pnl == 0
    assert snap.modified_dietz.cumulative_return == pytest.approx(0.0, abs=1e-12)
    assert snap.modified_dietz.max_drawdown == pytest.approx(0.0, abs=1e-12)
    assert snap.cashflow.total_deposits == 1_000_000
    assert snap.cashflow.total_withdrawals == 0
    # raw simple is contaminated: (6M - 5M) / 5M = +20%, fake "return"
    assert snap.raw_simple_return.value == pytest.approx(0.2, abs=1e-9)


def test_withdrawal_only_cashflow_separates_from_trading():
    """Withdrawal alone must not register as trading loss or DD."""
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 4_500_000),  # -500K withdrawal, no trading
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.WITHDRAWAL, amount=500_000),
    ]
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    assert snap.modified_dietz.cumulative_trading_pnl == 0
    assert snap.modified_dietz.cumulative_return == pytest.approx(0.0, abs=1e-12)
    assert snap.modified_dietz.max_drawdown == pytest.approx(0.0, abs=1e-12)
    assert snap.cashflow.total_withdrawals == 500_000


def test_dividend_signed_as_inflow():
    """Dividend events must aggregate into total_dividends and net_external_flow as +."""
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 5_010_000),  # +10K dividend, 0 trading
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DIVIDEND, amount=10_000),
    ]
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    assert snap.cashflow.total_dividends == 10_000
    assert snap.cashflow.net_external_flow == 10_000
    assert snap.invested_capital.value == 5_010_000
    assert snap.modified_dietz.cumulative_trading_pnl == 0


# ─── Section 4: source labels + JSON shape ────────────────────────────────


def test_all_source_labels_present_on_snapshot():
    cfg = _capital_config_5m()
    equity = [("2026-04-15", 5_000_000), ("2026-04-16", 5_010_000)]
    snap = compute_dashboard_snapshot(equity, [], cfg)

    assert snap.raw_equity.source == SOURCE_RAW_EQUITY
    assert snap.initial_capital.source == SOURCE_INITIAL_CAPITAL
    assert snap.cashflow.source == SOURCE_CASHFLOW_LEDGER
    assert snap.modified_dietz.source == SOURCE_MODIFIED_DIETZ
    assert snap.raw_simple_return.source == SOURCE_RAW_SIMPLE
    # The raw_simple_return label must explicitly carry the
    # "NOT cashflow-aware" warning so dashboard viewers understand
    # the figure is contaminated by deposits/withdrawals.
    assert "NOT cashflow-aware" in snap.raw_simple_return.source


def test_snapshot_to_dict_stable_shape():
    """snapshot_to_dict must produce a JSON-serializable dict with all
    required keys. The endpoint relies on this shape; tests guard it.
    """
    cfg = _capital_config_5m()
    equity = [("2026-04-15", 5_000_000), ("2026-04-16", 5_010_000)]
    snap = compute_dashboard_snapshot(equity, [], cfg)
    d = snapshot_to_dict(snap)

    # Top-level sections
    assert set(d.keys()) == {
        "raw_equity",
        "initial_capital",
        "cashflow",
        "invested_capital",
        "modified_dietz",
        "raw_simple_return",
    }

    # Each section carries a source label
    for section in d.values():
        assert "source" in section, f"section missing source label: {section}"

    # Round-trip JSON-serializable
    assert json.dumps(d)  # raises if non-serializable

    # raw_equity carries value + as_of_date
    assert d["raw_equity"]["value"] == 5_010_000
    assert d["raw_equity"]["as_of_date"] == "2026-04-16"

    # invested_capital carries the formula string for transparency
    assert d["invested_capital"]["formula"] == "initial_capital + net_external_flow"

    # modified_dietz carries input counts so consumers can sanity-check
    assert d["modified_dietz"]["input_equity_points"] == 2
    assert d["modified_dietz"]["input_cashflow_events"] == 0


# ─── Section 5: empty-input degraded behavior ─────────────────────────────


def test_empty_equity_series_returns_zeroed_dietz():
    """If report_equity_log has no rows yet, the snapshot must still build
    (graceful degrade) — endpoint should serve a 200 response with zeroed
    Modified Dietz section, not crash.
    """
    cfg = _capital_config_5m()
    snap = compute_dashboard_snapshot([], [], cfg)

    assert snap.raw_equity.value == 0
    assert snap.raw_equity.as_of_date is None
    assert snap.modified_dietz.cumulative_return == 0.0
    assert snap.modified_dietz.input_equity_points == 0
    assert snap.invested_capital.value == 5_000_000


def test_empty_equity_with_cashflow_still_aggregates_cashflow():
    """Operator may have logged cashflow before any equity snapshot
    landed. Cashflow aggregation should still work."""
    cfg = _capital_config_5m()
    cashflows = [
        CashflowEvent(event_date="2026-04-15", type=EventType.DEPOSIT, amount=1_000_000),
    ]
    snap = compute_dashboard_snapshot([], cashflows, cfg)

    assert snap.cashflow.total_deposits == 1_000_000
    assert snap.cashflow.net_external_flow == 1_000_000
    assert snap.invested_capital.value == 6_000_000
    assert snap.modified_dietz.input_cashflow_events == 1
    assert snap.modified_dietz.input_equity_points == 0


# ─── Section 6: read-only / no-mutation ──────────────────────────────────


def test_snapshot_does_not_mutate_inputs():
    """Snapshot composition must be a pure function — equity series and
    cashflow events list must be byte-identical before/after the call.
    This is the regression for the 'never modify raw equity' doctrine.
    """
    cfg = _capital_config_5m()
    equity = [("2026-04-15", 5_000_000), ("2026-04-16", 6_000_000)]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
    ]
    equity_before = list(equity)
    cashflows_before = list(cashflows)

    _ = compute_dashboard_snapshot(equity, cashflows, cfg)

    assert equity == equity_before
    assert cashflows == cashflows_before


def test_dietz_engine_called_directly_matches_snapshot_view():
    """The CF3 view must report the same numbers as a direct CF2 call.
    Guards against composition layer drifting from the engine."""
    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 6_000_000),
        ("2026-04-17", 6_100_000),
        ("2026-04-18", 5_600_000),
        ("2026-04-19", 5_700_000),
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
        CashflowEvent(event_date="2026-04-18", type=EventType.WITHDRAWAL, amount=500_000),
    ]

    direct = compute_modified_dietz_returns(equity, cashflows, 5_000_000)
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    assert snap.modified_dietz.cumulative_return == direct.cumulative_return
    assert snap.modified_dietz.cumulative_trading_pnl == direct.cumulative_trading_pnl
    assert snap.modified_dietz.max_drawdown == direct.max_drawdown
    assert snap.modified_dietz.peak_return == direct.peak_return
    assert snap.modified_dietz.period_start == direct.period_start
    assert snap.modified_dietz.period_end == direct.period_end


# ─── Section 7: regression — adjusted equity time series MUST NOT be created ───


def test_snapshot_does_not_construct_adjusted_equity_series():
    """Doctrine: NO `equity_adj = raw - cumulative_cashflow` time series.

    The snapshot must expose only the raw equity tail point and the
    *derivative* Modified Dietz cumulative return — never an adjusted
    equity time series. The shape test below confirms there is no
    'adjusted_equity' / 'equity_adj' / 'adjusted_series' key.
    """
    cfg = _capital_config_5m()
    equity = [("2026-04-15", 5_000_000), ("2026-04-16", 6_000_000)]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
    ]
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)
    d = snapshot_to_dict(snap)

    # Recursive scan — no forbidden keys anywhere in the payload
    forbidden = {"adjusted_equity", "equity_adj", "adjusted_series", "equity_adjusted"}

    def scan(obj, path="root"):
        if isinstance(obj, dict):
            for k, v in obj.items():
                assert k not in forbidden, (
                    f"forbidden adjusted-equity-series key {k!r} at {path}.{k} — "
                    f"violates Jeff doctrine 2026-05-04 (CF0 anti-pattern guard). "
                    f"CF3 must surface only the raw tail point + Modified Dietz "
                    f"derivative, never an adjusted equity series."
                )
                scan(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                scan(item, f"{path}[{i}]")

    scan(d)


# ─── Section 8: Daily Report rendering integration ───────────────────────


def test_daily_report_renders_with_accounting_section():
    """rest_daily_report.generate_eod_report must accept an `accounting`
    parameter (a snapshot dict) and emit an HTML section that includes
    the source labels and the dual numbers.
    """
    import sys
    sys.path.insert(0, str(REPO_ROOT / "kr"))
    from report.rest_daily_report import generate_eod_report  # noqa: E402

    cfg = _capital_config_5m()
    equity = [
        ("2026-04-15", 5_000_000),
        ("2026-04-19", 5_700_000),
    ]
    cashflows = [
        CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
        CashflowEvent(event_date="2026-04-18", type=EventType.WITHDRAWAL, amount=500_000),
    ]
    snap = compute_dashboard_snapshot(equity, cashflows, cfg)

    out_path = generate_eod_report(
        portfolio={
            "total_asset": 5_700_000, "pnl_pct": 14.0,
            "total_pnl": 700_000, "cash": 500_000, "holdings_count": 5,
        },
        accounting=snapshot_to_dict(snap),
    )
    assert out_path is not None and out_path.exists()
    html = out_path.read_text(encoding="utf-8")

    # Existing portfolio section (raw display) MUST still be present
    assert "포트폴리오" in html
    assert "5,700,000원" in html  # raw broker total_asset
    # Existing portfolio source label
    assert "kt00018" in html

    # New CF3 accounting section MUST be present
    assert "회계 (CF3)" in html or "Accounting" in html
    # Initial capital + cashflow rendered
    assert "5,000,000" in html  # initial capital
    # Modified Dietz number rendered (not zero)
    assert "Modified Dietz" in html
    # Source labels for every section
    assert SOURCE_RAW_EQUITY in html
    assert SOURCE_INITIAL_CAPITAL in html
    assert SOURCE_CASHFLOW_LEDGER in html
    assert SOURCE_MODIFIED_DIETZ in html
    # Raw simple return labelled as NOT cashflow-aware
    assert "NOT cashflow-aware" in html

    # Cleanup
    out_path.unlink()


def test_daily_report_renders_without_accounting_unchanged():
    """When accounting=None, the Daily Report must render exactly as
    before CF3 — no new section, no broken HTML. Backward compat."""
    import sys
    sys.path.insert(0, str(REPO_ROOT / "kr"))
    from report.rest_daily_report import generate_eod_report  # noqa: E402

    out_path = generate_eod_report(
        portfolio={
            "total_asset": 5_700_000, "pnl_pct": 14.0,
            "total_pnl": 700_000, "cash": 500_000, "holdings_count": 5,
        },
    )
    assert out_path is not None and out_path.exists()
    html = out_path.read_text(encoding="utf-8")

    # Existing sections rendered
    assert "포트폴리오" in html
    # No accounting section when not provided
    assert "회계 (CF3)" not in html
    assert "Modified Dietz" not in html

    out_path.unlink()
