"""Accounting contract verifier (PR-CF4 minimal).

Self-audit module that pins the CF1+CF2+CF3 contract against contamination
or drift. Reads-only; no production PG, no `verifier_runs/` write surface,
no scheduler hooks. Runs entirely on synthetic fixtures.

Five invariants (Jeff doctrine 2026-05-04):
  1. raw equity immutability       — snapshot.raw_equity tracks input tail byte-identical
  2. Modified Dietz replay         — snapshot_to_dict twice → byte-identical JSON
  3. cashflow effect 분리           — cashflow-only days yield trading_pnl == 0 / DD == 0
  4. report HTML ↔ API parity      — numbers rendered in Daily Report HTML match payload
  5. source label coverage         — every section has source key, raw_simple labelled "NOT cashflow-aware"

Twelve canonical scenarios S1..S12 cover empty / single-point / monotone /
deposit / withdrawal / dividend / manual / Jeff mandatory / drawdown-with-deposit
/ replay / high-frequency.

Out of scope (CF4 minimal):
  - production PG access
  - verifier_runs/ persistence
  - scheduler / batch integration
  - dashboard / chart changes
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from .config import CapitalConfig
from .dashboard import (
    SOURCE_RAW_SIMPLE,
    compute_dashboard_snapshot,
    snapshot_to_dict,
)
from .ledger import CashflowEvent, EventType


# ── Severity constants ────────────────────────────────────────────────────
SEV_CRITICAL = "CRITICAL"
SEV_ERROR = "ERROR"
SEV_WARN = "WARN"

# ── Status constants ─────────────────────────────────────────────────────
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"


@dataclass(frozen=True)
class Scenario:
    """One synthetic fixture: input series + cashflow events + canonical answers."""
    id: str
    name: str
    description: str
    equity_series: list[tuple[str, int]]
    cashflow_events: list[CashflowEvent]
    initial_capital: int
    # `expected` carries the canonical answers each scenario commits to.
    # Subset is checked depending on what the scenario exercises (e.g.
    # zero-cashflow scenarios assert dietz==raw_simple; high-frequency
    # scenarios assert event_count and net_flow).
    expected: dict


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: str          # CRITICAL | ERROR | WARN
    status: str            # PASS | FAIL
    expected: object
    actual: object
    detail: str = ""


@dataclass(frozen=True)
class ScenarioReport:
    scenario_id: str
    scenario_name: str
    status: str            # PASS = all checks PASS; FAIL = any CRITICAL/ERROR FAIL
    checks: list[CheckResult]
    summary: str


@dataclass(frozen=True)
class ParityMismatch:
    field: str
    payload_value: object
    html_value: object


@dataclass(frozen=True)
class ParityReport:
    status: str            # PASS | FAIL
    values_compared: int
    mismatches: list[ParityMismatch]


# ──────────────────────────────────────────────────────────────────────────
#                            Canonical scenarios
# ──────────────────────────────────────────────────────────────────────────


def _capital_config(initial: int = 5_000_000) -> CapitalConfig:
    return CapitalConfig(
        initial_capital=initial,
        currency="KRW",
        strategy_start_date="2026-04-15",
    )


def _build_scenarios() -> list[Scenario]:
    cfg_initial = 5_000_000

    # S1: empty all
    s1 = Scenario(
        id="S1",
        name="empty all",
        description="No equity points, no cashflow. Graceful zeroed snapshot.",
        equity_series=[],
        cashflow_events=[],
        initial_capital=cfg_initial,
        expected={
            "raw_equity_value": 0,
            "raw_equity_as_of": None,
            "dietz_cumret": 0.0,
            "dietz_dd": 0.0,
            "input_equity_points": 0,
            "input_cashflow_events": 0,
            "net_external_flow": 0,
            "invested_capital": 5_000_000,
            "event_count": 0,
        },
    )

    # S2: single point
    s2 = Scenario(
        id="S2",
        name="single point",
        description="One equity point, no cashflow. dietz.daily must be empty.",
        equity_series=[("2026-04-15", 5_000_000)],
        cashflow_events=[],
        initial_capital=cfg_initial,
        expected={
            "raw_equity_value": 5_000_000,
            "raw_equity_as_of": "2026-04-15",
            "dietz_cumret": 0.0,
            "dietz_dd": 0.0,
            "input_equity_points": 1,
            "raw_simple_return": 0.0,
            "net_external_flow": 0,
        },
    )

    # S3: monotone uptrend, no cashflow → dietz == raw_simple
    s3_eq = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 5_100_000),
        ("2026-04-17", 5_200_000),
        ("2026-04-18", 5_300_000),
        ("2026-04-19", 5_400_000),
        ("2026-04-20", 5_500_000),
    ]
    s3 = Scenario(
        id="S3",
        name="monotone uptrend, no cashflow",
        description=(
            "5M → 5.5M over 5 trading days, no cashflow. Modified Dietz "
            "cumulative_return must equal raw_simple_return; max_drawdown == 0."
        ),
        equity_series=s3_eq,
        cashflow_events=[],
        initial_capital=cfg_initial,
        expected={
            "raw_equity_value": 5_500_000,
            "raw_simple_return": 0.10,
            "dietz_dd": 0.0,
            "zero_cashflow_dietz_eq_raw_simple": True,
            "net_external_flow": 0,
        },
    )

    # S4: monotone downtrend, no cashflow
    s4_eq = [
        ("2026-04-15", 5_000_000),
        ("2026-04-16", 4_900_000),
        ("2026-04-17", 4_800_000),
        ("2026-04-18", 4_700_000),
        ("2026-04-19", 4_600_000),
        ("2026-04-20", 4_500_000),
    ]
    s4 = Scenario(
        id="S4",
        name="monotone downtrend, no cashflow",
        description=(
            "5M → 4.5M over 5 trading days. dietz cumulative_return == "
            "raw_simple_return (-10%); max_drawdown reflects trough."
        ),
        equity_series=s4_eq,
        cashflow_events=[],
        initial_capital=cfg_initial,
        expected={
            "raw_equity_value": 4_500_000,
            "raw_simple_return": -0.10,
            "dietz_dd_negative": True,
            "dietz_dd_date": "2026-04-20",
            "zero_cashflow_dietz_eq_raw_simple": True,
        },
    )

    # S5: deposit-only — trading_pnl == 0, raw_simple contaminated
    s5 = Scenario(
        id="S5",
        name="deposit only",
        description=(
            "Single +1M deposit, no trading. trading_pnl must be 0 and "
            "Modified Dietz cumret must be 0; raw_simple is +20% (contaminated)."
        ),
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 6_000_000),
        ],
        cashflow_events=[
            CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
        ],
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": 0,
            "dietz_cumret": 0.0,
            "dietz_dd": 0.0,
            "raw_simple_return": 0.20,
            "net_external_flow": 1_000_000,
            "total_deposits": 1_000_000,
            "invested_capital": 6_000_000,
        },
    )

    # S6: withdrawal-only
    s6 = Scenario(
        id="S6",
        name="withdrawal only",
        description=(
            "Single -500K withdrawal, no trading. trading_pnl == 0 and DD == 0; "
            "raw_simple is -10% (contaminated)."
        ),
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 4_500_000),
        ],
        cashflow_events=[
            CashflowEvent(event_date="2026-04-16", type=EventType.WITHDRAWAL, amount=500_000),
        ],
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": 0,
            "dietz_cumret": 0.0,
            "dietz_dd": 0.0,
            "raw_simple_return": -0.10,
            "net_external_flow": -500_000,
            "total_withdrawals": 500_000,
            "invested_capital": 4_500_000,
        },
    )

    # S7: dividend-only
    s7 = Scenario(
        id="S7",
        name="dividend only",
        description="Single +10K dividend, no trading. Aggregates as inflow.",
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 5_010_000),
        ],
        cashflow_events=[
            CashflowEvent(event_date="2026-04-16", type=EventType.DIVIDEND, amount=10_000),
        ],
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": 0,
            "dietz_cumret": 0.0,
            "net_external_flow": 10_000,
            "total_dividends": 10_000,
            "invested_capital": 5_010_000,
        },
    )

    # S8: manual_adjustment +/-
    # Day 0: 5M baseline
    # Day 1: +50K manual_adj → equity 5.05M (no trading)
    # Day 2: -30K manual_adj → equity 5.02M (no trading)
    s8 = Scenario(
        id="S8",
        name="manual_adjustment +/-",
        description=(
            "Two manual adjustments of opposite signs (+50K, -30K). "
            "total_manual_adjustment must preserve sign and net to +20K."
        ),
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 5_050_000),
            ("2026-04-17", 5_020_000),
        ],
        cashflow_events=[
            CashflowEvent(
                event_date="2026-04-16",
                type=EventType.MANUAL_ADJUSTMENT,
                amount=50_000,
            ),
            CashflowEvent(
                event_date="2026-04-17",
                type=EventType.MANUAL_ADJUSTMENT,
                amount=-30_000,
            ),
        ],
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": 0,
            "dietz_cumret": 0.0,
            "net_external_flow": 20_000,
            "total_manual_adjustment": 20_000,
            "invested_capital": 5_020_000,
        },
    )

    # S9: Jeff mandatory (CF2 stop-condition case, surfaced through CF3)
    s9 = Scenario(
        id="S9",
        name="Jeff mandatory case",
        description=(
            "5M + 1M deposit - 500K withdrawal + 200K trading. "
            "broker_eq=5.7M, trading_pnl=200K, DD=0, net_flow=+500K, "
            "invested_capital=5.5M."
        ),
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 6_000_000),  # +1M deposit, 0 trading
            ("2026-04-17", 6_100_000),  # +100K trading
            ("2026-04-18", 5_600_000),  # -500K withdrawal, 0 trading
            ("2026-04-19", 5_700_000),  # +100K trading
        ],
        cashflow_events=[
            CashflowEvent(
                event_date="2026-04-16", type=EventType.DEPOSIT,
                amount=1_000_000, source="kakao_bank",
            ),
            CashflowEvent(
                event_date="2026-04-18", type=EventType.WITHDRAWAL,
                amount=500_000, source="kakao_bank",
            ),
        ],
        initial_capital=cfg_initial,
        expected={
            "raw_equity_value": 5_700_000,
            "raw_equity_as_of": "2026-04-19",
            "trading_pnl": 200_000,
            "dietz_dd": 0.0,
            "net_external_flow": 500_000,
            "invested_capital": 5_500_000,
            "raw_simple_return": 0.14,  # contaminated
        },
    )

    # S10: deposit + drawdown
    # Day 0: 5M, Day 1: 6M (after +1M deposit), Day 2: 5.5M (-500K trading loss)
    # trading_pnl day 2 = 5.5M - 6M - 0 = -500K
    # daily_return day 2 = -500K / 6M = -8.33%
    # cumulative_return = -8.33%, DD reflects this trading loss only
    s10 = Scenario(
        id="S10",
        name="deposit + drawdown",
        description=(
            "Deposit +1M then trading loss -500K. DD must reflect the "
            "trading loss against the post-deposit base (-8.33%), not the "
            "raw equity peak."
        ),
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 6_000_000),  # +1M deposit, 0 trading
            ("2026-04-17", 5_500_000),  # -500K trading loss
        ],
        cashflow_events=[
            CashflowEvent(event_date="2026-04-16", type=EventType.DEPOSIT, amount=1_000_000),
        ],
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": -500_000,
            "dietz_cumret_approx": -500_000 / 6_000_000,  # -8.33%
            "dietz_dd_negative": True,
            "dietz_dd_date": "2026-04-17",
            "raw_simple_return": 0.10,  # (5.5M - 5M) / 5M = +10% (contaminated upward by deposit)
        },
    )

    # S11: replay determinism (uses S9 input — verified separately, see verify_scenario)
    s11 = Scenario(
        id="S11",
        name="replay determinism",
        description=(
            "Same input as S9, called twice. snapshot_to_dict output must "
            "be byte-identical between runs."
        ),
        equity_series=s9.equity_series,
        cashflow_events=s9.cashflow_events,
        initial_capital=cfg_initial,
        expected={
            "replay_byte_identical": True,
        },
    )

    # S12: high-frequency cashflow (30 days alternating ±10K)
    # Dates generated with date + timedelta to avoid manual month-rollover logic.
    s12_start = date(2026, 4, 15)
    s12_eq: list[tuple[str, int]] = [(s12_start.isoformat(), 5_000_000)]
    s12_events: list[CashflowEvent] = []
    eq = 5_000_000
    for i in range(1, 31):
        d_iso = (s12_start + timedelta(days=i)).isoformat()
        # Alternate +10K (odd i) / -10K (even i)
        if i % 2 == 1:
            eq += 10_000
            s12_events.append(CashflowEvent(
                event_date=d_iso, type=EventType.DEPOSIT, amount=10_000,
            ))
        else:
            eq -= 10_000
            s12_events.append(CashflowEvent(
                event_date=d_iso, type=EventType.WITHDRAWAL, amount=10_000,
            ))
        s12_eq.append((d_iso, eq))
    s12 = Scenario(
        id="S12",
        name="high-frequency cashflow",
        description=(
            "30 daily cashflows alternating +10K / -10K with no trading. "
            "Tests aggregation accuracy at scale: net_flow == 0, "
            "total_deposits == 150K, total_withdrawals == 150K, event_count == 30."
        ),
        equity_series=s12_eq,
        cashflow_events=s12_events,
        initial_capital=cfg_initial,
        expected={
            "trading_pnl": 0,
            "dietz_cumret": 0.0,
            "net_external_flow": 0,
            "total_deposits": 150_000,
            "total_withdrawals": 150_000,
            "event_count": 30,
        },
    )

    return [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12]


CANONICAL_SCENARIOS: list[Scenario] = _build_scenarios()


# ──────────────────────────────────────────────────────────────────────────
#                            Verification engine
# ──────────────────────────────────────────────────────────────────────────


# Forbidden keys per CF0/CF3 anti-pattern: separate adjusted-equity time series.
FORBIDDEN_KEYS = frozenset({
    "adjusted_equity",
    "equity_adj",
    "adjusted_series",
    "equity_adjusted",
})


def _check(name: str, severity: str, expected, actual, *, detail: str = "") -> CheckResult:
    status = STATUS_PASS if expected == actual else STATUS_FAIL
    return CheckResult(
        name=name, severity=severity, status=status,
        expected=expected, actual=actual, detail=detail,
    )


def _check_approx(
    name: str,
    severity: str,
    expected: float,
    actual: float,
    *,
    abs_tol: float = 1e-9,
    detail: str = "",
) -> CheckResult:
    status = STATUS_PASS if abs(expected - actual) <= abs_tol else STATUS_FAIL
    return CheckResult(
        name=name, severity=severity, status=status,
        expected=expected, actual=actual,
        detail=detail or f"abs_tol={abs_tol}",
    )


def scan_forbidden_keys(payload, *, path: str = "root") -> list[str]:
    """Recursively scan payload for any forbidden adjusted-equity-series keys.

    Public surface — tests inject these keys into a payload to confirm the
    scanner catches them (CF0/CF3 anti-pattern regression).
    """
    offenders: list[str] = []
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k in FORBIDDEN_KEYS:
                offenders.append(f"{path}.{k}")
            offenders.extend(scan_forbidden_keys(v, path=f"{path}.{k}"))
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            offenders.extend(scan_forbidden_keys(item, path=f"{path}[{i}]"))
    return offenders


# Backward-compatible private alias — older callers may use the
# leading-underscore spelling. Public callers must use scan_forbidden_keys.
_scan_forbidden_keys = scan_forbidden_keys


def verify_scenario(scenario: Scenario) -> ScenarioReport:
    """Run every applicable check against a scenario.

    Always runs (universal invariants):
      - raw equity tail match (CRITICAL)
      - raw equity as-of-date match (CRITICAL)
      - net_external_flow correctness (CRITICAL)
      - invested_capital formula (CRITICAL)
      - replay determinism (CRITICAL)
      - all sections have source label (ERROR)
      - raw_simple_return labelled NOT cashflow-aware (ERROR)
      - no forbidden keys in payload (CRITICAL)

    Conditionally runs based on scenario expected dict (per-fixture pinned answers):
      - dietz_cumret / dietz_dd / trading_pnl / raw_simple_return / event counts /
        zero-cashflow consistency / dd date / approx targets

    Returns ScenarioReport(status, checks). status=PASS iff every CRITICAL/ERROR
    check passes.
    """
    cfg = _capital_config(scenario.initial_capital)
    snapshot = compute_dashboard_snapshot(
        equity_series=scenario.equity_series,
        cashflow_events=scenario.cashflow_events,
        capital_config=cfg,
    )
    payload = snapshot_to_dict(snapshot)

    # Replay: run again, ensure JSON byte-identical
    snapshot_replay = compute_dashboard_snapshot(
        equity_series=scenario.equity_series,
        cashflow_events=scenario.cashflow_events,
        capital_config=cfg,
    )
    payload_replay = snapshot_to_dict(snapshot_replay)
    json_first = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    json_replay = json.dumps(payload_replay, sort_keys=True, ensure_ascii=False)

    checks: list[CheckResult] = []

    # ── Universal invariants ─────────────────────────────────────────────
    expected_raw = scenario.equity_series[-1][1] if scenario.equity_series else 0
    expected_date = scenario.equity_series[-1][0] if scenario.equity_series else None
    checks.append(_check(
        "raw_equity_value_tail_match", SEV_CRITICAL,
        expected_raw, snapshot.raw_equity.value,
        detail="raw_equity.value must equal equity_series[-1][1] byte-identical",
    ))
    checks.append(_check(
        "raw_equity_as_of_match", SEV_CRITICAL,
        expected_date, snapshot.raw_equity.as_of_date,
    ))

    expected_net = sum(ev.signed_amount() for ev in scenario.cashflow_events)
    checks.append(_check(
        "net_external_flow_correctness", SEV_CRITICAL,
        expected_net, snapshot.cashflow.net_external_flow,
        detail="net_external_flow == sum(signed_amount(e) for e in events)",
    ))

    checks.append(_check(
        "invested_capital_formula", SEV_CRITICAL,
        scenario.initial_capital + expected_net,
        snapshot.invested_capital.value,
        detail="invested_capital == initial_capital + net_external_flow",
    ))

    checks.append(_check(
        "replay_determinism", SEV_CRITICAL,
        json_first, json_replay,
        detail="snapshot_to_dict twice must produce byte-identical JSON",
    ))

    # Source label coverage (every top-level section)
    section_keys = (
        "raw_equity", "initial_capital", "cashflow", "invested_capital",
        "modified_dietz", "raw_simple_return",
    )
    missing_source = [s for s in section_keys if "source" not in payload.get(s, {})]
    checks.append(_check(
        "source_label_coverage", SEV_ERROR,
        [], missing_source,
        detail="every top-level section must carry a source key",
    ))

    # raw_simple_return label warning
    rs_source = payload.get("raw_simple_return", {}).get("source", "")
    label_ok = "NOT cashflow-aware" in rs_source
    checks.append(CheckResult(
        name="raw_simple_not_cashflow_aware_label",
        severity=SEV_ERROR,
        status=STATUS_PASS if label_ok else STATUS_FAIL,
        expected="NOT cashflow-aware (substring)",
        actual=rs_source,
        detail="raw_simple_return.source must explicitly mark contamination",
    ))
    # Also pin the exact constant we ship from kr.accounting.dashboard
    checks.append(_check(
        "raw_simple_source_pinned",
        SEV_ERROR,
        SOURCE_RAW_SIMPLE,
        rs_source,
    ))

    forbidden_hits = scan_forbidden_keys(payload)
    checks.append(_check(
        "no_forbidden_adjusted_equity_keys", SEV_CRITICAL,
        [], forbidden_hits,
        detail=(
            f"forbidden keys {sorted(FORBIDDEN_KEYS)} must not appear in payload "
            "(CF0/CF3 anti-pattern guard)"
        ),
    ))

    # ── Zero-cashflow exact invariants ──────────────────────────────────
    # When the scenario has no cashflow events, two strong invariants hold:
    #   (a) trading_pnl == raw_end - raw_start   (exact integer math)
    #   (b) dietz cumulative_return == raw_simple_return  (chain product
    #       of (V_curr / V_prev) telescopes to V_end / V_start, so the
    #       geometric chain equals the simple ratio at fp precision)
    # These checks pin the engine's behavior at TIGHT tolerance (1e-12),
    # not the prior 0.02 lax bound — Jeff doctrine 2026-05-04.
    if not scenario.cashflow_events and len(scenario.equity_series) >= 2:
        raw_start = scenario.equity_series[0][1]
        raw_end = scenario.equity_series[-1][1]
        checks.append(_check(
            "zero_cashflow_trading_pnl_eq_raw_delta",
            SEV_CRITICAL,
            raw_end - raw_start,
            snapshot.modified_dietz.cumulative_trading_pnl,
            detail="zero-cashflow: trading_pnl must exactly equal raw_end - raw_start",
        ))
        checks.append(_check_approx(
            "zero_cashflow_dietz_eq_raw_simple",
            SEV_CRITICAL,
            snapshot.raw_simple_return.value,
            snapshot.modified_dietz.cumulative_return,
            abs_tol=1e-12,
            detail=(
                "zero-cashflow: dietz cumret must equal raw_simple_return "
                "at fp precision (chain telescopes to V_end/V_start)"
            ),
        ))

    # ── Conditional / pinned per-scenario expectations ───────────────────
    exp = scenario.expected

    if "raw_equity_value" in exp:
        checks.append(_check(
            "scenario_raw_equity_value", SEV_CRITICAL,
            exp["raw_equity_value"], snapshot.raw_equity.value,
        ))
    if "raw_equity_as_of" in exp:
        checks.append(_check(
            "scenario_raw_equity_as_of", SEV_CRITICAL,
            exp["raw_equity_as_of"], snapshot.raw_equity.as_of_date,
        ))
    if "trading_pnl" in exp:
        checks.append(_check(
            "scenario_trading_pnl", SEV_CRITICAL,
            exp["trading_pnl"],
            snapshot.modified_dietz.cumulative_trading_pnl,
            detail="cashflow-only days must yield 0 trading_pnl (cashflow effect 분리)",
        ))
    if "dietz_cumret" in exp:
        checks.append(_check_approx(
            "scenario_dietz_cumret", SEV_CRITICAL,
            exp["dietz_cumret"], snapshot.modified_dietz.cumulative_return,
        ))
    if "dietz_cumret_approx" in exp:
        checks.append(_check_approx(
            "scenario_dietz_cumret_approx", SEV_CRITICAL,
            exp["dietz_cumret_approx"],
            snapshot.modified_dietz.cumulative_return,
            abs_tol=1e-6,
        ))
    if "dietz_dd" in exp:
        checks.append(_check_approx(
            "scenario_dietz_dd", SEV_CRITICAL,
            exp["dietz_dd"], snapshot.modified_dietz.max_drawdown,
            abs_tol=1e-12,
        ))
    if exp.get("dietz_dd_negative"):
        dd_ok = snapshot.modified_dietz.max_drawdown < 0.0
        checks.append(CheckResult(
            name="scenario_dietz_dd_negative",
            severity=SEV_CRITICAL,
            status=STATUS_PASS if dd_ok else STATUS_FAIL,
            expected="max_drawdown < 0",
            actual=snapshot.modified_dietz.max_drawdown,
        ))
    if "dietz_dd_date" in exp:
        checks.append(_check(
            "scenario_dietz_dd_date", SEV_CRITICAL,
            exp["dietz_dd_date"],
            snapshot.modified_dietz.max_drawdown_date,
        ))
    if "raw_simple_return" in exp:
        checks.append(_check_approx(
            "scenario_raw_simple_return", SEV_CRITICAL,
            exp["raw_simple_return"],
            snapshot.raw_simple_return.value,
        ))
    # Note: zero-cashflow consistency is now pinned by the universal
    # invariants `zero_cashflow_trading_pnl_eq_raw_delta` (exact integer)
    # and `zero_cashflow_dietz_eq_raw_simple` (1e-12 tolerance) above —
    # always applied when scenario.cashflow_events is empty. The legacy
    # per-scenario flag with 0.02 tolerance was removed (Jeff fix-up
    # 2026-05-04: chain product telescopes to V_end/V_start exactly, so
    # no large tolerance is justified).
    if exp.get("zero_cashflow_dietz_eq_raw_simple"):
        # Compatibility: scenarios still in CANONICAL_SCENARIOS may carry
        # this flag (S3/S4); the universal check above already covers them
        # at 1e-12. This branch is now a no-op kept only so the expected
        # dict can carry the flag for documentation without triggering
        # KeyError; the actual assertion is the universal one above.
        pass
    if "net_external_flow" in exp:
        checks.append(_check(
            "scenario_net_external_flow", SEV_CRITICAL,
            exp["net_external_flow"],
            snapshot.cashflow.net_external_flow,
        ))
    if "total_deposits" in exp:
        checks.append(_check(
            "scenario_total_deposits", SEV_CRITICAL,
            exp["total_deposits"],
            snapshot.cashflow.total_deposits,
        ))
    if "total_withdrawals" in exp:
        checks.append(_check(
            "scenario_total_withdrawals", SEV_CRITICAL,
            exp["total_withdrawals"],
            snapshot.cashflow.total_withdrawals,
        ))
    if "total_dividends" in exp:
        checks.append(_check(
            "scenario_total_dividends", SEV_CRITICAL,
            exp["total_dividends"],
            snapshot.cashflow.total_dividends,
        ))
    if "total_manual_adjustment" in exp:
        checks.append(_check(
            "scenario_total_manual_adjustment", SEV_CRITICAL,
            exp["total_manual_adjustment"],
            snapshot.cashflow.total_manual_adjustment,
        ))
    if "event_count" in exp:
        checks.append(_check(
            "scenario_event_count", SEV_CRITICAL,
            exp["event_count"],
            snapshot.cashflow.event_count,
        ))
    if "invested_capital" in exp:
        checks.append(_check(
            "scenario_invested_capital", SEV_CRITICAL,
            exp["invested_capital"],
            snapshot.invested_capital.value,
        ))
    if "input_equity_points" in exp:
        checks.append(_check(
            "scenario_input_equity_points", SEV_CRITICAL,
            exp["input_equity_points"],
            snapshot.modified_dietz.input_equity_points,
        ))
    if "input_cashflow_events" in exp:
        checks.append(_check(
            "scenario_input_cashflow_events", SEV_CRITICAL,
            exp["input_cashflow_events"],
            snapshot.modified_dietz.input_cashflow_events,
        ))
    if exp.get("replay_byte_identical"):
        checks.append(_check(
            "scenario_replay_byte_identical", SEV_CRITICAL,
            json_first, json_replay,
            detail="explicit replay assertion (S11)",
        ))

    # Aggregate: PASS iff every CRITICAL/ERROR check passes (WARN does not fail)
    fail_count = sum(
        1 for c in checks
        if c.status == STATUS_FAIL and c.severity in (SEV_CRITICAL, SEV_ERROR)
    )
    summary = (
        f"{len(checks) - fail_count}/{len(checks)} checks PASS"
        if fail_count == 0
        else f"{fail_count} CRITICAL/ERROR fail(s) of {len(checks)} checks"
    )
    return ScenarioReport(
        scenario_id=scenario.id,
        scenario_name=scenario.name,
        status=STATUS_PASS if fail_count == 0 else STATUS_FAIL,
        checks=checks,
        summary=summary,
    )


def run_canonical_scenarios() -> list[ScenarioReport]:
    """Run verify_scenario over all 12 canonical fixtures."""
    return [verify_scenario(s) for s in CANONICAL_SCENARIOS]


# ──────────────────────────────────────────────────────────────────────────
#                      Report HTML ↔ API parity verifier
# ──────────────────────────────────────────────────────────────────────────


# Numeric extractors. Each pattern is anchored on a label string that the
# Daily Report emits so the regex is unambiguous. The patterns match the
# format `kr/report/rest_daily_report.py` produces (see "회계 (CF3)" section).
_KRW_NUMBER_RE = re.compile(r"([+-]?[\d,]+)\s*원")
_KRW_NUMBER_DATED_RE = re.compile(r"([+-]?[\d,]+)\s*원\s*@\s*(\d{4}-\d{2}-\d{2})")
_PERCENT_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)\s*%")


# The seven labels the Daily Report's "회계 (CF3)" section emits. Every
# label here MUST be found in the rendered HTML for parity to PASS; any
# missing or unparseable cell is a FAIL. Tests import this constant.
REQUIRED_PARITY_LABELS: tuple[str, ...] = (
    "Raw equity (broker truth)",
    "Initial capital",
    "Net external flow",
    "Invested capital",
    "Raw simple return",
    "Modified Dietz cumulative return",
    "Modified Dietz max DD",
)

# Tolerance for percentage comparisons. The Daily Report formats with
# %.2f, so any drift > 0.005 of a percentage point is a real mismatch.
_PERCENT_ABS_TOL = 0.01


def _strip_thousands(s: str) -> int:
    return int(s.replace(",", ""))


def _extract_html_value_after_label(html: str, label: str) -> Optional[str]:
    """Extract the value-cell text from a Daily Report row whose label-cell
    contains `label`. Returns the inner text of the next `<td>` after the
    label, or None.
    """
    # The Daily Report emits rows of shape:
    #   <tr><td>{label}</td><td>{value}</td><td class="src">{source}</td></tr>
    # Loosely match across possible whitespace.
    pattern = re.compile(
        r"<td[^>]*>\s*"
        + re.escape(label)
        + r"\s*</td>\s*<td[^>]*>(.*?)</td>",
        re.DOTALL,
    )
    m = pattern.search(html)
    return m.group(1).strip() if m else None


def _check_label_raw_equity(cell: str, payload: dict) -> list[ParityMismatch]:
    """Raw equity cell carries both value and date — emit per-field
    mismatches with the label as field-name prefix so the test of
    `m.field.startswith("raw_equity")` (legacy) AND the new
    `m.field.startswith("Raw equity")` both work without ambiguity.
    """
    out: list[ParityMismatch] = []
    m = _KRW_NUMBER_DATED_RE.search(cell)
    if not m:
        out.append(ParityMismatch(
            "Raw equity (broker truth) (parse failed)",
            payload["raw_equity"], cell,
        ))
        return out
    html_value = _strip_thousands(m.group(1))
    html_date = m.group(2)
    if html_value != payload["raw_equity"]["value"]:
        out.append(ParityMismatch(
            "raw_equity.value (Raw equity (broker truth))",
            payload["raw_equity"]["value"], html_value,
        ))
    if html_date != payload["raw_equity"]["as_of_date"]:
        out.append(ParityMismatch(
            "raw_equity.as_of_date (Raw equity (broker truth))",
            payload["raw_equity"]["as_of_date"], html_date,
        ))
    return out


def _check_label_krw_value(
    cell: str, payload_value: int, label: str,
) -> list[ParityMismatch]:
    m = _KRW_NUMBER_RE.search(cell)
    if not m:
        return [ParityMismatch(
            f"{label} (parse failed)", payload_value, cell,
        )]
    html_value = _strip_thousands(m.group(1))
    if html_value != payload_value:
        return [ParityMismatch(label, payload_value, html_value)]
    return []


def _check_label_percent(
    cell: str, payload_ratio: float, label: str,
) -> list[ParityMismatch]:
    m = _PERCENT_RE.search(cell)
    if not m:
        return [ParityMismatch(
            f"{label} (parse failed)", payload_ratio, cell,
        )]
    html_pct = float(m.group(1))
    payload_pct = round(payload_ratio * 100, 2)
    if abs(html_pct - payload_pct) > _PERCENT_ABS_TOL:
        return [ParityMismatch(label, payload_pct, html_pct)]
    return []


def _label_handlers(payload: dict):
    """Return the per-label parser/comparator dispatch table.

    Each entry returns a list of ParityMismatch (empty if value matches).
    Keys are the exact label strings emitted by `kr/report/rest_daily_report.py`.
    Mismatch field names use the LABEL (not the internal payload key) so
    operators see the same label they'd find in the report.
    """
    return {
        "Raw equity (broker truth)":
            lambda cell: _check_label_raw_equity(cell, payload),
        "Initial capital":
            lambda cell: _check_label_krw_value(
                cell, payload["initial_capital"]["value"], "Initial capital",
            ),
        "Net external flow":
            lambda cell: _check_label_krw_value(
                cell, payload["cashflow"]["net_external_flow"], "Net external flow",
            ),
        "Invested capital":
            lambda cell: _check_label_krw_value(
                cell, payload["invested_capital"]["value"], "Invested capital",
            ),
        "Raw simple return":
            lambda cell: _check_label_percent(
                cell, payload["raw_simple_return"]["value"], "Raw simple return",
            ),
        "Modified Dietz cumulative return":
            lambda cell: _check_label_percent(
                cell, payload["modified_dietz"]["cumulative_return"],
                "Modified Dietz cumulative return",
            ),
        "Modified Dietz max DD":
            lambda cell: _check_label_percent(
                cell, payload["modified_dietz"]["max_drawdown"],
                "Modified Dietz max DD",
            ),
    }


def verify_report_api_parity(payload: dict, html: str) -> ParityReport:
    """Compare numbers rendered in the Daily Report HTML against the payload dict.

    Contract (Jeff fix-up 2026-05-04):
      - Every label in REQUIRED_PARITY_LABELS MUST be present in the HTML.
        Missing label → ParityMismatch(field=f"{label} missing",
        payload_value="required", html_value=None).
      - Every located cell MUST parse against its expected pattern.
        Parse failure → ParityMismatch with "(parse failed)" suffix.
      - Every parsed value MUST match the payload (exact for KRW integers,
        within `_PERCENT_ABS_TOL` for percent-formatted fields).
      - `values_compared` counts cells that parsed successfully (whether
        matching or mismatching). If `values_compared <
        len(REQUIRED_PARITY_LABELS)`, status is FAIL.
      - status == PASS iff: no mismatches AND
        values_compared == len(REQUIRED_PARITY_LABELS).
    """
    mismatches: list[ParityMismatch] = []
    values_compared = 0
    handlers = _label_handlers(payload)

    for label in REQUIRED_PARITY_LABELS:
        cell = _extract_html_value_after_label(html, label)
        if cell is None:
            mismatches.append(ParityMismatch(
                field=f"{label} missing",
                payload_value="required",
                html_value=None,
            ))
            continue
        handler = handlers[label]
        cell_mismatches = handler(cell)
        if any("parse failed" in m.field for m in cell_mismatches):
            # Parse failed — count as not-compared and surface mismatch.
            mismatches.extend(cell_mismatches)
            continue
        # Successful parse → counted as compared (mismatches surface
        # value drift but the cell was understood).
        values_compared += 1
        mismatches.extend(cell_mismatches)

    pass_status = (
        not mismatches
        and values_compared == len(REQUIRED_PARITY_LABELS)
    )
    return ParityReport(
        status=STATUS_PASS if pass_status else STATUS_FAIL,
        values_compared=values_compared,
        mismatches=mismatches,
    )
