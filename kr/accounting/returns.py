"""Modified Dietz cashflow-aware return + DD engine (PR-CF2).

Greenfield additive — does NOT mutate raw equity, does NOT replace
the existing return / DD pipeline. CF2 introduces a SEPARATE engine
that downstream consumers (CF3 dashboard, CF4 verifier) can call
when they need cashflow-aware metrics. The existing risk path
(`exposure_guard.get_monthly_dd_pct()` etc.) remains untouched.

Approach
--------
For each daily period [t, t+1]:

  V_start  = equity at start of day t
  V_end    = equity at start of day t+1  (= equity at end of day t)
  C_t      = sum of signed cashflow events dated day t

  trading_pnl_t   = V_end - V_start - C_t
  daily_return_t  = trading_pnl_t / (V_start + adjusted_basis)
                  = trading_pnl_t / V_start_after_open_cashflow

  V_start_after_open_cashflow = V_start + max(C_t, 0) for deposits
  (deposit increases the trading base before any trading happens;
   withdrawal reduces it. We treat all cashflow as occurring at
   start-of-day for daily Modified Dietz — the standard convention.)

Cumulative return = product over days of (1 + daily_return_t) - 1.
Cumulative trading PnL = sum over days of trading_pnl_t.

Drawdown
--------
DD is computed on the cumulative return curve, NOT on raw equity.
This is the cashflow-aware piece: a deposit that doubles equity
appears as 0% trading return on that day, so the cumulative-return
peak does not jump. Withdrawals likewise leave the peak intact.

  cumret_curve[t] = ∏_{s ≤ t} (1 + daily_return_s) - 1
  peak_curve[t]   = max_{s ≤ t} cumret_curve[s]
  dd[t]           = (1 + cumret_curve[t]) / (1 + peak_curve[t]) - 1
                    (relative drawdown; ≤ 0)
  max_drawdown    = min_t dd[t]

Hard restrictions (CF2 scope)
-----------------------------
- raw equity series passed in is NOT modified
- this module does NOT write to any equity_history / report_equity_log
- this module does NOT replace exposure_guard or any existing DD path
- a future PR (CF3+) decides if/how to surface these results
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .ledger import CashflowEvent, SIGN_CONVENTION


@dataclass(frozen=True)
class DailyMetric:
    date: str
    raw_start: int          # equity at day open
    raw_end: int            # equity at next day open (= today close)
    cashflow: int           # signed sum of cashflows dated this day
    trading_pnl: int        # raw_end - raw_start - cashflow
    base_for_return: int    # raw_start + max(cashflow, 0)  (deposit grows base before trading)
    daily_return: float     # trading_pnl / base_for_return  (or 0 if base 0)
    cumulative_return: float
    peak_return: float
    drawdown: float


@dataclass(frozen=True)
class ModifiedDietzResult:
    daily: list[DailyMetric] = field(default_factory=list)
    cumulative_return: float = 0.0          # compound daily returns - 1
    cumulative_trading_pnl: int = 0          # sum of daily trading_pnl
    max_drawdown: float = 0.0                # most negative; ≤ 0
    max_drawdown_date: Optional[str] = None
    peak_return: float = 0.0
    peak_date: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    initial_capital: int = 0
    final_raw_equity: int = 0
    final_invested_capital: int = 0          # initial + net_external_flow
    net_external_flow: int = 0


def _validate_equity_series(equity_series):
    if not equity_series:
        raise ValueError("equity_series must contain at least one (date, equity) point")
    seen_dates = set()
    prev_date = None
    for i, item in enumerate(equity_series):
        if not (isinstance(item, (list, tuple)) and len(item) == 2):
            raise ValueError(f"equity_series[{i}] must be (date_str, equity_int) tuple")
        d, eq = item
        if not isinstance(d, str):
            raise ValueError(f"equity_series[{i}] date must be str, got {type(d)}")
        if not isinstance(eq, int) or isinstance(eq, bool):
            raise ValueError(f"equity_series[{i}] equity must be int, got {type(eq)}")
        if eq < 0:
            raise ValueError(f"equity_series[{i}] negative equity: {eq}")
        if d in seen_dates:
            raise ValueError(f"equity_series duplicate date: {d}")
        if prev_date is not None and d <= prev_date:
            raise ValueError(
                f"equity_series must be strictly ascending; "
                f"got {prev_date!r} then {d!r}"
            )
        seen_dates.add(d)
        prev_date = d


def compute_modified_dietz_returns(
    equity_series: list[tuple[str, int]],
    cashflow_events: list[CashflowEvent],
    initial_capital: int,
) -> ModifiedDietzResult:
    """Cashflow-aware Modified Dietz return + DD engine.

    Parameters
    ----------
    equity_series : list of (date_str, equity_int)
        Raw broker equity at end-of-day, ascending by date. NOT modified.
    cashflow_events : list of CashflowEvent (CF1)
        External cashflows. Sign computed via signed_amount().
    initial_capital : int
        Strategy inception baseline (CF1 capital_state). Used for
        sanity sums and for invested_capital reporting.

    Returns
    -------
    ModifiedDietzResult — daily metrics + cumulative + DD.

    The first day in `equity_series` is treated as the baseline (no
    return computed — we need a previous day to compute today's return).
    Day t's return is (equity[t] - equity[t-1] - cashflow[t]) / base[t].
    """
    if not isinstance(initial_capital, int) or initial_capital <= 0:
        raise ValueError(f"initial_capital must be positive int, got {initial_capital!r}")
    _validate_equity_series(equity_series)

    # Aggregate signed cashflow per date
    cashflow_by_date: dict[str, int] = {}
    net_external_flow = 0
    for ev in cashflow_events:
        signed = ev.signed_amount()
        cashflow_by_date[ev.event_date] = cashflow_by_date.get(ev.event_date, 0) + signed
        net_external_flow += signed

    daily: list[DailyMetric] = []
    cumret = 0.0
    peak = 0.0
    peak_date: Optional[str] = None
    max_dd = 0.0
    max_dd_date: Optional[str] = None
    cumulative_pnl = 0

    # Need at least 2 points to compute returns
    if len(equity_series) >= 2:
        for i in range(1, len(equity_series)):
            d_prev, v_prev = equity_series[i - 1]
            d_curr, v_curr = equity_series[i]
            cf = cashflow_by_date.get(d_curr, 0)

            trading_pnl = v_curr - v_prev - cf

            # Base for return: previous equity + deposit at open
            # (cashflow at start of day adjusts the trading base; only
            # positive cashflow grows the base, withdrawal reduces it).
            base = v_prev + cf  # combine: deposit grows, withdrawal reduces
            if base <= 0:
                # degenerate: full withdrawal or worse — return undefined
                ret = 0.0
            else:
                ret = trading_pnl / base

            cumret = (1.0 + cumret) * (1.0 + ret) - 1.0
            if cumret > peak:
                peak = cumret
                peak_date = d_curr
            # Relative drawdown vs peak
            dd = (1.0 + cumret) / (1.0 + peak) - 1.0 if (1.0 + peak) != 0 else 0.0
            if dd < max_dd:
                max_dd = dd
                max_dd_date = d_curr

            cumulative_pnl += trading_pnl
            daily.append(DailyMetric(
                date=d_curr,
                raw_start=v_prev,
                raw_end=v_curr,
                cashflow=cf,
                trading_pnl=trading_pnl,
                base_for_return=base,
                daily_return=ret,
                cumulative_return=cumret,
                peak_return=peak,
                drawdown=dd,
            ))

    period_start = equity_series[0][0] if equity_series else None
    period_end = equity_series[-1][0] if equity_series else None
    final_eq = equity_series[-1][1] if equity_series else 0

    return ModifiedDietzResult(
        daily=daily,
        cumulative_return=cumret,
        cumulative_trading_pnl=cumulative_pnl,
        max_drawdown=max_dd,
        max_drawdown_date=max_dd_date,
        peak_return=peak,
        peak_date=peak_date,
        period_start=period_start,
        period_end=period_end,
        initial_capital=initial_capital,
        final_raw_equity=final_eq,
        final_invested_capital=initial_capital + net_external_flow,
        net_external_flow=net_external_flow,
    )
