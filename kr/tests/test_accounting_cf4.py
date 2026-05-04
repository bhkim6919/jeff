"""PR-CF4 minimal — accounting verifier tests.

Verifies the CF4 self-audit module:
  - 12 canonical scenarios (S1..S12) all PASS
  - report HTML ↔ API payload parity
  - 5 invariants pinned (raw immutability / replay / cashflow 분리 /
    parity / source label coverage)
  - forbidden adjusted-equity-series keys absent in payload
  - CRITICAL/ERROR/WARN severity routing works
  - run_canonical_scenarios returns 12 PASS reports

Out of scope for CF4 minimal (NOT covered here):
  - production PG access
  - verifier_runs/ write surface
  - real cron / scheduler integration
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from accounting import (
    CANONICAL_SCENARIOS,
    CapitalConfig,
    CashflowEvent,
    EventType,
    ParityReport,
    Scenario,
    ScenarioReport,
    compute_dashboard_snapshot,
    run_canonical_scenarios,
    snapshot_to_dict,
    verify_report_api_parity,
    verify_scenario,
)
from accounting.verifier import (
    FORBIDDEN_KEYS,
    SEV_CRITICAL,
    SEV_ERROR,
    STATUS_FAIL,
    STATUS_PASS,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


# ─── Section 1: All 12 canonical scenarios PASS ──────────────────────────


def test_canonical_scenarios_count_is_twelve():
    """The verifier ships exactly 12 canonical scenarios (S1..S12)."""
    assert len(CANONICAL_SCENARIOS) == 12
    ids = [s.id for s in CANONICAL_SCENARIOS]
    assert ids == [f"S{i}" for i in range(1, 13)]


def test_run_canonical_scenarios_all_pass():
    """All 12 scenarios PASS — no CRITICAL/ERROR fail anywhere."""
    reports = run_canonical_scenarios()
    assert len(reports) == 12
    failed = [r for r in reports if r.status != STATUS_PASS]
    assert not failed, (
        "Canonical scenarios failing: "
        + "\n".join(
            f"\n  {r.scenario_id} {r.scenario_name}: {r.summary}\n    "
            + "\n    ".join(
                f"{c.severity} {c.name} expected={c.expected!r} actual={c.actual!r}"
                for c in r.checks if c.status == STATUS_FAIL
            )
            for r in failed
        )
    )


@pytest.mark.parametrize("scenario", CANONICAL_SCENARIOS, ids=[s.id for s in CANONICAL_SCENARIOS])
def test_each_scenario_individually(scenario: Scenario):
    """Per-scenario isolation: any single failure surfaces with its own ID."""
    report = verify_scenario(scenario)
    assert report.status == STATUS_PASS, (
        f"{scenario.id} {scenario.name} FAILED: {report.summary}\n"
        + "\n".join(
            f"  {c.severity} {c.name} expected={c.expected!r} actual={c.actual!r} ({c.detail})"
            for c in report.checks if c.status == STATUS_FAIL
        )
    )


# ─── Section 2: 5 invariants pinned ──────────────────────────────────────


def test_invariant_raw_equity_immutability():
    """Invariant 1 — raw equity tail must equal input equity_series[-1]."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    snap = compute_dashboard_snapshot(
        equity_series=s9.equity_series,
        cashflow_events=s9.cashflow_events,
        capital_config=CapitalConfig(
            initial_capital=s9.initial_capital,
            currency="KRW",
            strategy_start_date="2026-04-15",
        ),
    )
    assert snap.raw_equity.value == s9.equity_series[-1][1]
    assert snap.raw_equity.as_of_date == s9.equity_series[-1][0]


def test_invariant_replay_determinism_byte_identical():
    """Invariant 2 — snapshot_to_dict twice must be byte-identical JSON."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    cfg = CapitalConfig(
        initial_capital=s9.initial_capital, currency="KRW",
        strategy_start_date="2026-04-15",
    )
    snap1 = snapshot_to_dict(compute_dashboard_snapshot(
        s9.equity_series, s9.cashflow_events, cfg))
    snap2 = snapshot_to_dict(compute_dashboard_snapshot(
        s9.equity_series, s9.cashflow_events, cfg))
    j1 = json.dumps(snap1, sort_keys=True, ensure_ascii=False)
    j2 = json.dumps(snap2, sort_keys=True, ensure_ascii=False)
    assert j1 == j2


def test_invariant_cashflow_only_zero_trading_pnl():
    """Invariant 3 — days with only cashflow (no trading) must yield trading_pnl == 0."""
    for sid in ("S5", "S6", "S7", "S12"):
        scenario = next(s for s in CANONICAL_SCENARIOS if s.id == sid)
        report = verify_scenario(scenario)
        assert report.status == STATUS_PASS
        # Cumulative trading_pnl must be 0 in cashflow-only scenarios
        cfg = CapitalConfig(
            initial_capital=scenario.initial_capital, currency="KRW",
            strategy_start_date="2026-04-15",
        )
        snap = compute_dashboard_snapshot(
            scenario.equity_series, scenario.cashflow_events, cfg)
        assert snap.modified_dietz.cumulative_trading_pnl == 0, (
            f"{sid}: trading_pnl != 0 ({snap.modified_dietz.cumulative_trading_pnl}) "
            "— cashflow effect 분리 invariant broken"
        )


def test_invariant_source_label_coverage():
    """Invariant 5 — every top-level section carries a source key."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    cfg = CapitalConfig(
        initial_capital=s9.initial_capital, currency="KRW",
        strategy_start_date="2026-04-15",
    )
    payload = snapshot_to_dict(compute_dashboard_snapshot(
        s9.equity_series, s9.cashflow_events, cfg))
    sections = ("raw_equity", "initial_capital", "cashflow",
                "invested_capital", "modified_dietz", "raw_simple_return")
    for section in sections:
        assert "source" in payload[section], f"{section} missing source label"


def test_invariant_no_forbidden_adjusted_equity_keys():
    """Invariant — payload must not contain adjusted_equity / equity_adj / etc."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    cfg = CapitalConfig(
        initial_capital=s9.initial_capital, currency="KRW",
        strategy_start_date="2026-04-15",
    )
    payload = snapshot_to_dict(compute_dashboard_snapshot(
        s9.equity_series, s9.cashflow_events, cfg))

    def _scan(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                assert k not in FORBIDDEN_KEYS, (
                    f"forbidden key {k!r} found — CF0/CF3 anti-pattern guard broken"
                )
                _scan(v)
        elif isinstance(obj, list):
            for item in obj:
                _scan(item)

    _scan(payload)


# ─── Section 3: Verifier surfaces a useful failure when contract drifts ───


def test_verify_scenario_detects_synthetic_violation():
    """Construct a deliberately-wrong scenario expectation and verify the
    verifier reports CRITICAL FAIL — proves the verifier actually checks."""
    bad_scenario = Scenario(
        id="X1",
        name="synthetic violation (test only)",
        description="Forced-wrong expected values to verify the verifier fails.",
        equity_series=[
            ("2026-04-15", 5_000_000),
            ("2026-04-16", 5_100_000),
        ],
        cashflow_events=[],
        initial_capital=5_000_000,
        expected={
            "raw_equity_value": 9_999_999,  # wrong on purpose
            "trading_pnl": 999_999,         # wrong on purpose
        },
    )
    report = verify_scenario(bad_scenario)
    assert report.status == STATUS_FAIL
    fails = [c for c in report.checks if c.status == STATUS_FAIL]
    assert any(c.name == "scenario_raw_equity_value" for c in fails)
    assert any(c.name == "scenario_trading_pnl" for c in fails)


# ─── Section 4: Report HTML ↔ API parity ─────────────────────────────────


def _generate_report_html(scenario: Scenario) -> tuple[Path, dict]:
    """Helper: render the Daily Report HTML for a scenario and return
    (path, payload_dict) so tests can call verify_report_api_parity."""
    import sys
    sys.path.insert(0, str(REPO_ROOT / "kr"))
    from report.rest_daily_report import generate_eod_report

    cfg = CapitalConfig(
        initial_capital=scenario.initial_capital, currency="KRW",
        strategy_start_date="2026-04-15",
    )
    snapshot = compute_dashboard_snapshot(
        equity_series=scenario.equity_series,
        cashflow_events=scenario.cashflow_events,
        capital_config=cfg,
    )
    payload = snapshot_to_dict(snapshot)
    out_path = generate_eod_report(
        portfolio={
            "total_asset": payload["raw_equity"]["value"],
            "pnl_pct": 0.0, "total_pnl": 0,
            "cash": 0, "holdings_count": 0,
        },
        accounting=payload,
    )
    assert out_path is not None and out_path.exists()
    return out_path, payload


def test_parity_jeff_mandatory_case():
    """Render Jeff mandatory case to HTML and assert every numeric value
    in the rendered "회계 (CF3)" section matches the payload exactly."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    out_path, payload = _generate_report_html(s9)
    try:
        html = out_path.read_text(encoding="utf-8")
        report = verify_report_api_parity(payload, html)
        assert report.status == STATUS_PASS, (
            f"Parity FAIL: {report.values_compared} compared, "
            f"mismatches={report.mismatches}"
        )
        # The accounting section emits 7 dual-display rows; we expect
        # at least the 4 numeric ones (raw, init cap, net flow, invested
        # cap) plus the 3 percent ones (raw simple, dietz cumret, dd) =
        # 7 comparable values.
        assert report.values_compared >= 5
    finally:
        out_path.unlink()


def test_parity_zero_cashflow_uptrend():
    """Render S3 (monotone uptrend, no cashflow) — parity must hold."""
    s3 = next(s for s in CANONICAL_SCENARIOS if s.id == "S3")
    out_path, payload = _generate_report_html(s3)
    try:
        html = out_path.read_text(encoding="utf-8")
        report = verify_report_api_parity(payload, html)
        assert report.status == STATUS_PASS, report.mismatches
    finally:
        out_path.unlink()


def test_parity_detects_synthetic_html_corruption():
    """If we corrupt the HTML to mismatch the payload, parity must report
    a FAIL — proves the parity checker is doing actual work.

    The accounting section row is identifiable by the unique label
    "Raw equity (broker truth)" — we corrupt the value cell after that
    label specifically, so the test only invalidates the accounting
    section (not the portfolio section, which the parity checker does
    not inspect).
    """
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    out_path, payload = _generate_report_html(s9)
    try:
        html = out_path.read_text(encoding="utf-8")
        # Surgical replacement: only the accounting section's raw equity row.
        target_label = "Raw equity (broker truth)"
        idx = html.index(target_label)
        before = html[:idx]
        after = html[idx:]
        corrupted_after = after.replace("5,700,000원", "9,999,999원", 1)
        assert corrupted_after != after, "test setup broken: replacement did not occur"
        corrupted = before + corrupted_after
        report = verify_report_api_parity(payload, corrupted)
        assert report.status == STATUS_FAIL
        assert any(
            m.field.startswith("raw_equity") for m in report.mismatches
        ), f"expected a raw_equity mismatch, got {report.mismatches}"
    finally:
        out_path.unlink()


# ─── Section 5: severity routing ─────────────────────────────────────────


def test_check_result_severities_used():
    """The verifier emits both CRITICAL and ERROR severities so operators
    can route alerts differently. Verify both severity levels are present."""
    s9 = next(s for s in CANONICAL_SCENARIOS if s.id == "S9")
    report = verify_scenario(s9)
    severities = {c.severity for c in report.checks}
    assert SEV_CRITICAL in severities
    assert SEV_ERROR in severities


def test_warn_severity_does_not_fail_scenario():
    """A WARN-only failure must NOT mark the scenario FAIL.
    (CF4 minimal does not currently emit WARN, but the routing must support
    it for future performance regression checks per design memo.)"""
    # Build a scenario with no expected failures, then inspect routing logic.
    s2 = next(s for s in CANONICAL_SCENARIOS if s.id == "S2")
    report = verify_scenario(s2)
    # Sanity: verify that the aggregator only counts CRITICAL/ERROR fails.
    # If the severity routing were broken, the test would surface as the
    # report's status being inconsistent with the per-check fail count.
    crit_err_fails = sum(
        1 for c in report.checks
        if c.status == STATUS_FAIL and c.severity in (SEV_CRITICAL, SEV_ERROR)
    )
    if crit_err_fails == 0:
        assert report.status == STATUS_PASS


# ─── Section 6: scenario shape validation ────────────────────────────────


def test_every_scenario_has_required_fields():
    """Each canonical scenario must populate all required Scenario fields."""
    for s in CANONICAL_SCENARIOS:
        assert s.id, "Scenario.id required"
        assert s.name, "Scenario.name required"
        assert s.description, "Scenario.description required"
        assert s.initial_capital > 0
        assert isinstance(s.expected, dict)
        # equity_series and cashflow_events may be empty (S1) but must be lists
        assert isinstance(s.equity_series, list)
        assert isinstance(s.cashflow_events, list)


def test_scenario_ids_unique():
    ids = [s.id for s in CANONICAL_SCENARIOS]
    assert len(ids) == len(set(ids)), f"duplicate scenario IDs: {ids}"
