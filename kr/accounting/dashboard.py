"""Dashboard snapshot composer (PR-CF3).

CF3 dual display layer. Composes CF1 (capital_state, ledger, summary) + CF2
(Modified Dietz engine) into a single read-only snapshot for surfacing on:
  - the Daily Report HTML
  - the new /api/accounting/summary endpoint
  - downstream CF4 verifier (re-uses the same composition)

Hard restrictions (Jeff doctrine 2026-05-04, CF3 minimal scope):
  - this module is **read-only**. It never writes to capital_state, ledger,
    report_equity_log, exposure_guard, or any equity history file.
  - the raw equity series is NOT modified. The snapshot only reports it back
    so the dashboard can show raw and adjusted side by side with explicit
    source labels.
  - DD guard / peak / drawdown computation in `kr/risk/exposure_guard.py`
    is NOT touched. CF3 surfaces an *additional* Modified Dietz DD metric
    derived from the cumulative-return curve; the existing guard input
    pipeline keeps using broker equity peak.
  - NO `equity_adj = raw - cashflow` time series is constructed or stored
    anywhere. Modified Dietz returns are derivative-only and produced on
    every read.

Source labels (per Jeff CF3 contract):
  - raw_equity        = "broker/report_equity_log truth"
  - initial_capital   = "kr/data/accounting/capital_state.json"
  - cashflow ledger   = "kr/data/accounting/cashflow_ledger.jsonl"
  - modified_dietz    = "Modified Dietz / kr.accounting.returns (derivative)"
  - raw_simple_return = "broker derivative (NOT cashflow-aware)"
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .config import CapitalConfig
from .ledger import CashflowEvent
from .returns import ModifiedDietzResult, compute_modified_dietz_returns
from .summary import AccountingSummary, compute_summary as _compute_summary_from_events


# ── Source label constants ────────────────────────────────────────────────
SOURCE_RAW_EQUITY = "broker/report_equity_log truth"
SOURCE_INITIAL_CAPITAL = "kr/data/accounting/capital_state.json"
SOURCE_CASHFLOW_LEDGER = "kr/data/accounting/cashflow_ledger.jsonl"
SOURCE_MODIFIED_DIETZ = "Modified Dietz / kr.accounting.returns (derivative)"
SOURCE_RAW_SIMPLE = "broker derivative (NOT cashflow-aware)"


@dataclass(frozen=True)
class RawEquityView:
    value: int
    as_of_date: Optional[str]
    source: str = SOURCE_RAW_EQUITY


@dataclass(frozen=True)
class InitialCapitalView:
    value: int
    currency: str
    strategy_start_date: Optional[str]
    source: str = SOURCE_INITIAL_CAPITAL


@dataclass(frozen=True)
class CashflowView:
    """Aggregated cashflow ledger view for dashboard display."""
    net_external_flow: int
    total_deposits: int
    total_withdrawals: int
    total_dividends: int
    total_interest: int
    total_tax_refund: int
    total_manual_adjustment: int
    event_count: int
    last_event_date: Optional[str]
    source: str = SOURCE_CASHFLOW_LEDGER


@dataclass(frozen=True)
class InvestedCapitalView:
    value: int  # initial_capital + net_external_flow
    formula: str = "initial_capital + net_external_flow"
    source: str = "accounting derivative"


@dataclass(frozen=True)
class ModifiedDietzView:
    """Cashflow-aware return + DD numbers from CF2 engine."""
    cumulative_return: float
    cumulative_trading_pnl: int
    max_drawdown: float
    max_drawdown_date: Optional[str]
    peak_return: float
    peak_date: Optional[str]
    period_start: Optional[str]
    period_end: Optional[str]
    input_equity_points: int
    input_cashflow_events: int
    source: str = SOURCE_MODIFIED_DIETZ


@dataclass(frozen=True)
class RawSimpleReturnView:
    """The naive (raw_equity / initial_capital - 1) calc for transparency.

    Explicitly labelled as NOT cashflow-aware so dashboard viewers know
    the deposit/withdrawal contamination is present in this number. CF3
    keeps it visible so operators can compare against the Modified Dietz
    figure and see the size of the cashflow effect.
    """
    value: float  # ratio, e.g. 0.14 for +14%
    formula: str = "(raw_equity - initial_capital) / initial_capital"
    source: str = SOURCE_RAW_SIMPLE


@dataclass(frozen=True)
class DashboardSnapshot:
    """Read-only composite snapshot. Built entirely from inputs; nothing persisted."""
    raw_equity: RawEquityView
    initial_capital: InitialCapitalView
    cashflow: CashflowView
    invested_capital: InvestedCapitalView
    modified_dietz: ModifiedDietzView
    raw_simple_return: RawSimpleReturnView


def _events_to_summary(
    events: list[CashflowEvent],
    initial_capital: int,
) -> AccountingSummary:
    """Build summary from in-memory events list (test-friendly path).

    The CF1 `compute_summary(ledger, ...)` pulls from the ledger object;
    CF3 needs the same numbers from a list (since the equity series is
    also list-based). We re-implement the aggregation here rather than
    constructing a temp ledger file to avoid any I/O during snapshot build.
    Numbers MUST match `kr.accounting.summary.compute_summary` for any
    given event list — covered by tests.
    """
    from .ledger import EventType  # local import to avoid cycles

    totals = {
        EventType.DEPOSIT: 0,
        EventType.WITHDRAWAL: 0,
        EventType.DIVIDEND: 0,
        EventType.INTEREST: 0,
        EventType.TAX_REFUND: 0,
        EventType.MANUAL_ADJUSTMENT: 0,
    }
    net_signed = 0
    last_date: Optional[str] = None
    for ev in events:
        if ev.type == EventType.MANUAL_ADJUSTMENT:
            totals[EventType.MANUAL_ADJUSTMENT] += ev.amount
        else:
            totals[ev.type] += abs(ev.amount)
        net_signed += ev.signed_amount()
        if last_date is None or ev.event_date > last_date:
            last_date = ev.event_date

    return AccountingSummary(
        initial_capital=initial_capital,
        total_deposits=totals[EventType.DEPOSIT],
        total_withdrawals=totals[EventType.WITHDRAWAL],
        total_dividends=totals[EventType.DIVIDEND],
        total_interest=totals[EventType.INTEREST],
        total_tax_refund=totals[EventType.TAX_REFUND],
        total_manual_adjustment=totals[EventType.MANUAL_ADJUSTMENT],
        net_external_flow=net_signed,
        invested_capital=initial_capital + net_signed,
        event_count=len(events),
        last_event_date=last_date,
    )


def compute_dashboard_snapshot(
    equity_series: list[tuple[str, int]],
    cashflow_events: list[CashflowEvent],
    capital_config: CapitalConfig,
) -> DashboardSnapshot:
    """Pure function. Composes CF1 + CF2 into a CF3 dashboard snapshot.

    Parameters
    ----------
    equity_series : list of (date_str, equity_int) ascending
        Raw broker EOD equity series. NOT modified.
        May be empty (degraded display: raw=0, dietz zeroed).
    cashflow_events : list of CashflowEvent
        From the cashflow ledger. May be empty.
    capital_config : CapitalConfig
        From kr/data/accounting/capital_state.json.

    Returns
    -------
    DashboardSnapshot — all views populated with explicit source labels.

    Raises
    ------
    ValueError — if `compute_modified_dietz_returns` rejects the input
                 (e.g. duplicate dates, descending series, negative equity).
                 CF3 does NOT swallow these — a malformed equity series is
                 a real bug the dashboard must surface.
    """
    initial_capital = capital_config.initial_capital
    summary = _events_to_summary(cashflow_events, initial_capital)

    # Raw equity view
    if equity_series:
        last_date, last_eq = equity_series[-1]
        raw_view = RawEquityView(value=int(last_eq), as_of_date=last_date)
    else:
        raw_view = RawEquityView(value=0, as_of_date=None)

    # Modified Dietz: requires ≥1 point (engine handles 1-point case as
    # zero-return baseline). On empty series, return zeroed view rather
    # than calling the engine (engine validates non-empty).
    if equity_series:
        dietz = compute_modified_dietz_returns(
            equity_series=equity_series,
            cashflow_events=cashflow_events,
            initial_capital=initial_capital,
        )
        dietz_view = ModifiedDietzView(
            cumulative_return=dietz.cumulative_return,
            cumulative_trading_pnl=dietz.cumulative_trading_pnl,
            max_drawdown=dietz.max_drawdown,
            max_drawdown_date=dietz.max_drawdown_date,
            peak_return=dietz.peak_return,
            peak_date=dietz.peak_date,
            period_start=dietz.period_start,
            period_end=dietz.period_end,
            input_equity_points=len(equity_series),
            input_cashflow_events=len(cashflow_events),
        )
    else:
        dietz_view = ModifiedDietzView(
            cumulative_return=0.0,
            cumulative_trading_pnl=0,
            max_drawdown=0.0,
            max_drawdown_date=None,
            peak_return=0.0,
            peak_date=None,
            period_start=None,
            period_end=None,
            input_equity_points=0,
            input_cashflow_events=len(cashflow_events),
        )

    # Raw simple return — preserved for transparency, explicitly labelled
    # as NOT cashflow-aware so operators can see the contamination delta.
    if initial_capital > 0:
        raw_simple = (raw_view.value - initial_capital) / initial_capital
    else:
        raw_simple = 0.0

    return DashboardSnapshot(
        raw_equity=raw_view,
        initial_capital=InitialCapitalView(
            value=initial_capital,
            currency=capital_config.currency,
            strategy_start_date=capital_config.strategy_start_date,
        ),
        cashflow=CashflowView(
            net_external_flow=summary.net_external_flow,
            total_deposits=summary.total_deposits,
            total_withdrawals=summary.total_withdrawals,
            total_dividends=summary.total_dividends,
            total_interest=summary.total_interest,
            total_tax_refund=summary.total_tax_refund,
            total_manual_adjustment=summary.total_manual_adjustment,
            event_count=summary.event_count,
            last_event_date=summary.last_event_date,
        ),
        invested_capital=InvestedCapitalView(
            value=summary.invested_capital,
        ),
        modified_dietz=dietz_view,
        raw_simple_return=RawSimpleReturnView(value=raw_simple),
    )


def snapshot_to_dict(snapshot: DashboardSnapshot) -> dict:
    """JSON-serializable dict (used by the FastAPI endpoint).

    Stable shape — the Daily Report HTML and tests rely on it. Any future
    field additions are append-only; do NOT rename or remove existing keys
    without coordinating with consumers.
    """
    return {
        "raw_equity": {
            "value": snapshot.raw_equity.value,
            "as_of_date": snapshot.raw_equity.as_of_date,
            "source": snapshot.raw_equity.source,
        },
        "initial_capital": {
            "value": snapshot.initial_capital.value,
            "currency": snapshot.initial_capital.currency,
            "strategy_start_date": snapshot.initial_capital.strategy_start_date,
            "source": snapshot.initial_capital.source,
        },
        "cashflow": {
            "net_external_flow": snapshot.cashflow.net_external_flow,
            "total_deposits": snapshot.cashflow.total_deposits,
            "total_withdrawals": snapshot.cashflow.total_withdrawals,
            "total_dividends": snapshot.cashflow.total_dividends,
            "total_interest": snapshot.cashflow.total_interest,
            "total_tax_refund": snapshot.cashflow.total_tax_refund,
            "total_manual_adjustment": snapshot.cashflow.total_manual_adjustment,
            "event_count": snapshot.cashflow.event_count,
            "last_event_date": snapshot.cashflow.last_event_date,
            "source": snapshot.cashflow.source,
        },
        "invested_capital": {
            "value": snapshot.invested_capital.value,
            "formula": snapshot.invested_capital.formula,
            "source": snapshot.invested_capital.source,
        },
        "modified_dietz": {
            "cumulative_return": snapshot.modified_dietz.cumulative_return,
            "cumulative_trading_pnl": snapshot.modified_dietz.cumulative_trading_pnl,
            "max_drawdown": snapshot.modified_dietz.max_drawdown,
            "max_drawdown_date": snapshot.modified_dietz.max_drawdown_date,
            "peak_return": snapshot.modified_dietz.peak_return,
            "peak_date": snapshot.modified_dietz.peak_date,
            "period_start": snapshot.modified_dietz.period_start,
            "period_end": snapshot.modified_dietz.period_end,
            "input_equity_points": snapshot.modified_dietz.input_equity_points,
            "input_cashflow_events": snapshot.modified_dietz.input_cashflow_events,
            "source": snapshot.modified_dietz.source,
        },
        "raw_simple_return": {
            "value": snapshot.raw_simple_return.value,
            "formula": snapshot.raw_simple_return.formula,
            "source": snapshot.raw_simple_return.source,
        },
    }
