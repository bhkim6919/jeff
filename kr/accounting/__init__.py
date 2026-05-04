"""kr.accounting — Accounting Correction Sprint (PR-CF1, PR-CF2).

Greenfield accounting module for KR live. Provides:
  - Initial capital state (manual config, never auto-overwritten by broker)
  - Append-only immutable cashflow ledger (deposit / withdrawal / dividend / etc.)
  - Read-only summary (initial_capital, net_external_flow, invested_capital)
  - Modified Dietz cashflow-aware return + DD engine (CF2)

Hard restrictions (Jeff doctrine 2026-05-04):
  - raw equity = immutable broker truth (untouched here)
  - NO adjusted equity time series
  - NO `equity_adj = raw_equity - cumulative_cashflow` pattern
  - NO reuse of kr.finance._deprecated_capital_events
  - CF1 introduces ledger + summary (no behavior change to return / DD /
    dashboard / rebalancing)
  - CF2 introduces a SEPARATE return / DD engine (does NOT replace
    existing exposure_guard or any production return path; downstream
    consumers can call this engine when they need cashflow-aware metrics)

Storage: JSONL primary (file under kr/data/accounting/cashflow_ledger.jsonl).
PG-backed alternative deferred to a future PR if/when query volume
warrants it. Single source of truth = JSONL.
"""
from .config import CapitalConfig, get_initial_capital, load_capital_state
from .ledger import CashflowEvent, CashflowLedger, EventType
from .returns import (
    DailyMetric,
    ModifiedDietzResult,
    compute_modified_dietz_returns,
)
from .summary import AccountingSummary, compute_summary

__all__ = [
    "CapitalConfig",
    "get_initial_capital",
    "load_capital_state",
    "CashflowEvent",
    "CashflowLedger",
    "EventType",
    "AccountingSummary",
    "compute_summary",
    "DailyMetric",
    "ModifiedDietzResult",
    "compute_modified_dietz_returns",
]
