"""kr.accounting — Accounting Correction Sprint foundation (PR-CF1).

Greenfield accounting module for KR live. Provides:
  - Initial capital state (manual config, never auto-overwritten by broker)
  - Append-only immutable cashflow ledger (deposit / withdrawal / dividend / etc.)
  - Read-only summary computation (initial_capital, net_external_flow,
    invested_capital)

Hard restrictions (Jeff doctrine 2026-05-04):
  - raw equity = immutable broker truth (untouched here)
  - NO adjusted equity time series
  - NO `equity_adj = raw_equity - cumulative_cashflow` pattern
  - NO DD / return / dashboard / rebalancing change in CF1
  - NO reuse of kr.finance._deprecated_capital_events
  - Accounting Correction Sprint subsequent PRs (CF2+) consume this
    module to compute Modified Dietz returns; CF1 ITSELF does not
    change any return / DD / dashboard behavior.

Storage: JSONL primary (file under kr/data/accounting/cashflow_ledger.jsonl).
PG-backed alternative deferred to a future PR if/when query volume
warrants it. Single source of truth in CF1 = JSONL.
"""
from .config import CapitalConfig, get_initial_capital, load_capital_state
from .ledger import CashflowEvent, CashflowLedger, EventType
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
]
