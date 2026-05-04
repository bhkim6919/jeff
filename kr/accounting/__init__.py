"""kr.accounting — Accounting Correction Sprint (PR-CF1, PR-CF2, PR-CF3, PR-CF4).

Greenfield accounting module for KR live. Provides:
  - Initial capital state (manual config, never auto-overwritten by broker)
  - Append-only immutable cashflow ledger (deposit / withdrawal / dividend / etc.)
  - Read-only summary (initial_capital, net_external_flow, invested_capital)
  - Modified Dietz cashflow-aware return + DD engine (CF2)
  - Read-only dashboard snapshot composer (CF3): builds a single dataclass
    that the Daily Report HTML and the /api/accounting/summary endpoint
    serialize for dual display (raw + adjusted, source-labelled)
  - Self-audit verifier (CF4 minimal): 12 canonical synthetic scenarios +
    report-API parity check. Pure functions on fixtures; no production PG,
    no `verifier_runs/` write surface, no scheduler hooks.

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
  - CF3 surfaces those CF1+CF2 numbers on the Daily Report and a new
    read-only endpoint. It does NOT modify exposure_guard, the
    report_equity_log write path, or the existing portfolio PnL display.
  - CF4 verifier is read-only on synthetic fixtures only. Production replay,
    real PG access, and verifier_runs/ persistence are intentionally
    deferred to a separate follow-up PR.

Storage: JSONL primary (file under kr/data/accounting/cashflow_ledger.jsonl).
PG-backed alternative deferred to a future PR if/when query volume
warrants it. Single source of truth = JSONL.
"""
from .config import CapitalConfig, get_initial_capital, load_capital_state
from .dashboard import (
    CashflowView,
    DashboardSnapshot,
    InitialCapitalView,
    InvestedCapitalView,
    ModifiedDietzView,
    RawEquityView,
    RawSimpleReturnView,
    compute_dashboard_snapshot,
    snapshot_to_dict,
)
from .ledger import CashflowEvent, CashflowLedger, EventType
from .returns import (
    DailyMetric,
    ModifiedDietzResult,
    compute_modified_dietz_returns,
)
from .summary import AccountingSummary, compute_summary
from .verifier import (
    CANONICAL_SCENARIOS,
    CheckResult,
    ParityMismatch,
    ParityReport,
    Scenario,
    ScenarioReport,
    run_canonical_scenarios,
    verify_report_api_parity,
    verify_scenario,
)

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
    "CashflowView",
    "DashboardSnapshot",
    "InitialCapitalView",
    "InvestedCapitalView",
    "ModifiedDietzView",
    "RawEquityView",
    "RawSimpleReturnView",
    "compute_dashboard_snapshot",
    "snapshot_to_dict",
    "CANONICAL_SCENARIOS",
    "CheckResult",
    "ParityMismatch",
    "ParityReport",
    "Scenario",
    "ScenarioReport",
    "run_canonical_scenarios",
    "verify_report_api_parity",
    "verify_scenario",
]
