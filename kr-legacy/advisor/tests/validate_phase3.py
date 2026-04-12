"""Phase 3 End-to-End Validation -Recommend → Approve → Apply → PostEval → Drift.

Tests 6 scenarios with synthetic data designed to trigger each recommender path,
then walks the full lifecycle: recommendation → approval → override → post-eval
verdict → drift guard detection.

Run:
    cd kr-legacy
    python -m advisor.tests.validate_phase3

Exit code 0 = all pass, 1 = failures.
"""
from __future__ import annotations

import copy
import json
import shutil
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from advisor.ingestion.schema import DailySnapshot, DataMeta, SnapshotWindow
from advisor.ingestion.data_filter import build_window
from advisor.recommenders.param_recommender import recommend_params
from advisor.pipeline.approval import (
    create_approval_template, apply_approved_override,
    load_overrides, save_overrides, OverrideConflictError, OverrideLimitError,
)
from advisor.pipeline.post_evaluator import PostApplyEvaluator
from advisor.pipeline.drift_guard import AdvisorDriftGuard
from advisor.pipeline.metrics import (
    load_metrics, save_metrics, record_recommendation,
    record_approval, record_verdict,
)
from advisor.config import ADVISOR_DIR

# -- Helpers --------------------------------------------------

PASS = 0
FAIL = 0
APPROVED_DIR = ADVISOR_DIR / "approved"
BACKUP_OVERRIDE = APPROVED_DIR / "config_override.json.bak"
BACKUP_METRICS = ADVISOR_DIR / "metrics" / "advisor_metrics.json.bak"


def _meta(mode="paper_test"):
    return DataMeta(
        source="engine", mode=mode, strategy_version="4.0",
        is_operational=False, timestamp=datetime.now().isoformat(),
    )


def _trading_days(start: str, n: int) -> list[str]:
    dt = datetime.strptime(start, "%Y%m%d")
    days = []
    while len(days) < n:
        if dt.weekday() < 5:
            days.append(dt.strftime("%Y%m%d"))
        dt += timedelta(days=1)
    return days


def _make_snapshot(day: str, equity_overrides: dict = None,
                   closes: list[dict] = None,
                   operational_flags: list[str] = None) -> DailySnapshot:
    eq = {
        "date": f"{day[:4]}-{day[4:6]}-{day[6:]}",
        "equity": "500000000",
        "cash": "50000000",
        "n_positions": "20",
        "daily_pnl_pct": "0.001",
        "monthly_dd_pct": "-0.02",
        "risk_mode": "NORMAL",
        "rebalance_executed": "N",
        "reconcile_corrections": "0",
    }
    if equity_overrides:
        eq.update(equity_overrides)

    return DailySnapshot(
        trading_day=day,
        data_cutoff_time=f"{day[:4]}-{day[4:6]}-{day[6:]}T16:30:00",
        reference_point="EOD",
        meta=_meta(),
        equity=eq,
        closes=closes or [],
        operational_flags=operational_flags or [],
    )


def _make_close(code="005930", reason="TRAIL_STOP", pnl=-0.05,
                hold=3, hwm=0.02):
    return {
        "code": code,
        "exit_reason": reason,
        "pnl_pct": str(pnl),
        "hold_days": str(hold),
        "max_hwm_pct": str(hwm),
    }


def _build_test_window(snapshots, end_date):
    for s in snapshots:
        s.operational_flags = []
    return build_window(snapshots, end_date, window=min(20, len(snapshots)))


def _check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" -{detail}"
        print(msg)


def _backup_state():
    """Backup existing override/metrics files."""
    override_path = APPROVED_DIR / "config_override.json"
    metrics_path = ADVISOR_DIR / "metrics" / "advisor_metrics.json"
    if override_path.exists():
        shutil.copy2(override_path, BACKUP_OVERRIDE)
    if metrics_path.exists():
        shutil.copy2(metrics_path, BACKUP_METRICS)


def _restore_state():
    """Restore original override/metrics files."""
    override_path = APPROVED_DIR / "config_override.json"
    metrics_path = ADVISOR_DIR / "metrics" / "advisor_metrics.json"
    if BACKUP_OVERRIDE.exists():
        shutil.copy2(BACKUP_OVERRIDE, override_path)
        BACKUP_OVERRIDE.unlink()
    elif override_path.exists():
        override_path.unlink()
    if BACKUP_METRICS.exists():
        shutil.copy2(BACKUP_METRICS, metrics_path)
        BACKUP_METRICS.unlink()
    elif metrics_path.exists():
        metrics_path.unlink()


# ==============================================================
# TEST 1: Recommender -Trail Stop premature exit trigger
# ==============================================================
def test_recommender_trail_premature():
    print("\n-- TEST 1: Recommender - Trail Stop premature exit --")
    days = _trading_days("20260301", 20)

    # 60% premature trail exits (hold<=5, pnl<0) → should trigger TRAIL_PCT rec
    closes_per_day = []
    for i, d in enumerate(days):
        if i < 12:  # 12 premature exits
            closes_per_day.append(
                [_make_close(reason="TRAIL_STOP", pnl=-0.04, hold=2, hwm=0.01)])
        elif i < 16:  # 4 normal exits
            closes_per_day.append(
                [_make_close(reason="TRAIL_STOP", pnl=0.08, hold=30, hwm=0.12)])
        else:
            closes_per_day.append([])

    snapshots = [_make_snapshot(d, closes=c) for d, c in zip(days, closes_per_day)]
    window = _build_test_window(snapshots, days[-1])

    recs = recommend_params(window)

    trail_recs = [r for r in recs if r["parameter"] == "TRAIL_PCT"]
    _check("TRAIL_PCT recommendation generated",
           len(trail_recs) >= 1,
           f"got {len(trail_recs)} recs")

    if trail_recs:
        rec = trail_recs[0]
        _check("suggests wider stop (0.15)",
               rec["suggested_value"] == 0.15,
               f"got {rec.get('suggested_value')}")
        _check("confidence is LOW",
               rec["confidence"] == "LOW")
        _check("has rationale with premature rate",
               "premature" in rec["rationale"].lower(),
               rec["rationale"][:80])
        return rec
    return None


# ==============================================================
# TEST 2: Recommender -Tighter stop suggestion
# ==============================================================
def test_recommender_trail_tighter():
    print("\n-- TEST 2: Recommender -Trail Stop tighter suggestion --")
    days = _trading_days("20260301", 20)

    # 0% premature, high HWM → should suggest tighter stop
    closes_per_day = []
    for i, d in enumerate(days):
        if i < 15:
            closes_per_day.append(
                [_make_close(reason="TRAIL_STOP", pnl=0.05, hold=25, hwm=0.12)])
        else:
            closes_per_day.append([])

    snapshots = [_make_snapshot(d, closes=c) for d, c in zip(days, closes_per_day)]
    window = _build_test_window(snapshots, days[-1])

    recs = recommend_params(window)
    trail_recs = [r for r in recs if r["parameter"] == "TRAIL_PCT"]

    _check("tighter stop recommendation generated",
           len(trail_recs) >= 1,
           f"got {len(trail_recs)} recs")

    if trail_recs:
        _check("suggests 0.10",
               trail_recs[0]["suggested_value"] == 0.10,
               f"got {trail_recs[0].get('suggested_value')}")


# ==============================================================
# TEST 3: Recommender -Short hold + Win rate decline + DD guard
# ==============================================================
def test_recommender_multi_signal():
    print("\n-- TEST 3: Recommender -Multiple signals --")
    days = _trading_days("20260301", 20)

    closes_per_day = []
    for i, d in enumerate(days):
        if i < 10:
            # first half: 80% win rate, short holds
            c = _make_close(reason="REBALANCE",
                            pnl=0.03 if i < 8 else -0.02,
                            hold=2, hwm=0.04)
            closes_per_day.append([c])
        else:
            # second half: 20% win rate
            c = _make_close(reason="REBALANCE",
                            pnl=-0.04 if i < 18 else 0.01,
                            hold=2, hwm=0.01)
            closes_per_day.append([c])

    # DD guard active > 50% of days
    eq_overrides = [
        {"risk_mode": "DD_CAUTION"} if i % 2 == 0 else {"risk_mode": "MONTHLY_BLOCKED"}
        for i in range(20)
    ]

    snapshots = [_make_snapshot(d, equity_overrides=eq, closes=c)
                 for d, eq, c in zip(days, eq_overrides, closes_per_day)]
    window = _build_test_window(snapshots, days[-1])

    recs = recommend_params(window)
    params = [r["parameter"] for r in recs]

    _check("REBAL_DAYS recommendation (short holds)",
           "REBAL_DAYS" in params,
           f"got params: {params}")
    _check("STRATEGY_REVIEW recommendation (win rate decline)",
           "STRATEGY_REVIEW" in params,
           f"got params: {params}")
    _check("DD_LEVELS recommendation (guard too active)",
           "DD_LEVELS" in params,
           f"got params: {params}")


# ==============================================================
# TEST 4: Approval → Override → Conflict/Limit
# ==============================================================
def test_approval_pipeline(rec: dict):
    print("\n-- TEST 4: Approval Pipeline --")

    # Reset overrides
    APPROVED_DIR.mkdir(parents=True, exist_ok=True)
    save_overrides({"version": 1, "overrides": []})

    # 4a. Create approval template
    template = create_approval_template(rec)
    _check("template has recommendation_id",
           template["recommendation_id"] == rec["id"])
    _check("template has validation_result placeholder",
           template["validation_result"]["oos_cagr"] is None)
    _check("template approval=False by default",
           template["approval"]["approved"] is False)

    # 4b. Approve and apply
    template["approval"]["approved"] = True
    template["approval"]["reviewer"] = "test_validate_phase3"
    template["approval"]["reviewed_at"] = datetime.now().isoformat()
    template["approval"]["applied_from"] = "20260401"

    result = apply_approved_override(template)
    _check("override applied successfully",
           len(result["overrides"]) == 1)
    _check("override has correct parameter",
           result["overrides"][0]["parameter"] == rec["parameter"])

    # 4c. Conflict detection (same param again)
    conflict = False
    try:
        apply_approved_override(template)
    except OverrideConflictError:
        conflict = True
    _check("duplicate param raises OverrideConflictError", conflict)

    # 4d. Limit detection
    save_overrides({"version": 1, "overrides": [
        {"parameter": f"P{i}", "recommendation_id": f"R{i}",
         "value": i, "approved_at": "", "applied_from": "", "rollback_by": ""}
        for i in range(3)
    ]})
    template2 = copy.deepcopy(template)
    template2["parameter"] = "P_NEW"
    limit_hit = False
    try:
        apply_approved_override(template2)
    except OverrideLimitError:
        limit_hit = True
    _check("MAX_ACTIVE_OVERRIDES limit raises OverrideLimitError", limit_hit)

    # 4e. Unapproved rejection
    unapproved = copy.deepcopy(template)
    unapproved["approval"]["approved"] = False
    rejected = False
    try:
        save_overrides({"version": 1, "overrides": []})
        apply_approved_override(unapproved)
    except ValueError:
        rejected = True
    _check("unapproved override rejected", rejected)


# ==============================================================
# TEST 5: PostApplyEvaluator -KEEP / REVIEW / ROLLBACK
# ==============================================================
def test_post_evaluator():
    print("\n-- TEST 5: PostApplyEvaluator --")
    evaluator = PostApplyEvaluator()

    override = {"recommendation_id": "ADV_TEST_01", "parameter": "TRAIL_PCT"}

    # 5a. KEEP scenario: post is better
    pre_days = _trading_days("20260201", 10)
    post_days = _trading_days("20260301", 10)

    pre_snaps = [_make_snapshot(d, equity_overrides={"daily_pnl_pct": "-0.005"})
                 for d in pre_days]
    post_snaps = [_make_snapshot(d, equity_overrides={"daily_pnl_pct": "0.005"})
                  for d in post_days]

    pre_window = _build_test_window(pre_snaps, pre_days[-1])
    post_window = _build_test_window(post_snaps, post_days[-1])

    result = evaluator.evaluate(override, pre_window, post_window)
    _check("KEEP verdict when post > pre",
           result["verdict"] == "KEEP",
           f"got {result['verdict']}")
    _check("delta has avg_daily_pnl_delta > 0",
           result["delta"]["avg_daily_pnl_delta"] > 0,
           f"delta={result['delta']['avg_daily_pnl_delta']:.4f}")

    # 5b. REVIEW scenario: post slightly worse PnL
    post_snaps2 = [_make_snapshot(d, equity_overrides={"daily_pnl_pct": "-0.007"})
                   for d in post_days]
    post_window2 = _build_test_window(post_snaps2, post_days[-1])
    result2 = evaluator.evaluate(override, pre_window, post_window2)
    _check("REVIEW verdict when post slightly worse",
           result2["verdict"] == "REVIEW",
           f"got {result2['verdict']}")

    # 5c. ROLLBACK scenario: post has big MDD increase
    # Pre: small positive returns (low MDD)
    pre_snaps3 = [_make_snapshot(d, equity_overrides={"daily_pnl_pct": "0.002"})
                  for d in pre_days]
    # Post: large swings causing MDD
    post_pnls = ["-0.04", "0.01", "-0.05", "-0.03", "0.02",
                 "-0.04", "-0.02", "0.01", "-0.03", "-0.01"]
    post_snaps3 = [_make_snapshot(d, equity_overrides={"daily_pnl_pct": p})
                   for d, p in zip(post_days, post_pnls)]

    pre_window3 = _build_test_window(pre_snaps3, pre_days[-1])
    post_window3 = _build_test_window(post_snaps3, post_days[-1])
    result3 = evaluator.evaluate(override, pre_window3, post_window3)
    _check("ROLLBACK verdict when MDD increases significantly",
           result3["verdict"] == "ROLLBACK",
           f"got {result3['verdict']}, mdd_delta={result3['delta']['mdd_delta']:.4f}")


# ==============================================================
# TEST 6: Drift Guard -frequency, flip, rollback, suspension
# ==============================================================
def test_drift_guard():
    print("\n-- TEST 6: Drift Guard --")
    guard = AdvisorDriftGuard()

    # 6a. No drift (1 rec in 20 days)
    history = [{"category": "PARAM", "parameter": "TRAIL_PCT",
                "suggested_value": 0.15}]
    warnings = guard.check(history, [])
    _check("no drift with 1 rec", len(warnings) == 0, f"warnings={warnings}")

    # 6b. Frequency drift (>3 param recs in 20 days)
    history_4 = [{"category": "PARAM", "parameter": f"P{i}",
                  "suggested_value": i * 0.01}
                 for i in range(5)]
    warnings = guard.check(history_4, [])
    freq_drift = any("param recommendations" in w.lower() for w in warnings)
    _check("frequency drift detected (5 recs)", freq_drift, f"warnings={warnings}")

    # 6c. Same param repeated
    history_dup = [
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.15},
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.13},
    ]
    warnings = guard.check(history_dup, [])
    dup_drift = any("TRAIL_PCT" in w and "2x" in w for w in warnings)
    _check("duplicate param drift detected", dup_drift, f"warnings={warnings}")

    # 6d. Direction flip (0.12 → 0.10 → 0.15 → 0.08)
    history_flip = [
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.12},
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.10},
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.15},
        {"category": "PARAM", "parameter": "TRAIL_PCT", "suggested_value": 0.08},
    ]
    warnings = guard.check(history_flip, [])
    flip_blocked = any("BLOCK:" in w for w in warnings)
    _check("direction flip triggers BLOCK", flip_blocked, f"warnings={warnings}")
    _check("should_suspend_params = True on BLOCK",
           guard.should_suspend_params(warnings))

    # 6e. Rollback frequency
    approval_hist = [{"rolled_back": True}, {"rolled_back": True},
                     {"approved": True}, {"approved": True}]
    warnings = guard.check([], approval_hist)
    rollback_warn = any("rollback" in w.lower() for w in warnings)
    _check("rollback frequency triggers SUSPENDED", rollback_warn,
           f"warnings={warnings}")
    _check("should_suspend_params = True on SUSPENDED",
           guard.should_suspend_params(warnings))


# ==============================================================
# TEST 7: Metrics -record + rate calculation
# ==============================================================
def test_metrics():
    print("\n-- TEST 7: Metrics Tracking --")
    metrics = {
        "updated_at": "",
        "total_recommendations": 0,
        "approved": 0,
        "kept": 0,
        "reviewed": 0,
        "rolled_back": 0,
        "approval_rate": 0.0,
        "success_rate": 0.0,
        "recommendation_history": [],
    }

    # Record 3 recommendations
    record_recommendation(metrics, "R1", "TRAIL_PCT")
    record_recommendation(metrics, "R2", "REBAL_DAYS")
    record_recommendation(metrics, "R3", "DD_LEVELS")
    _check("3 recommendations recorded",
           metrics["total_recommendations"] == 3)
    _check("history has 3 entries",
           len(metrics["recommendation_history"]) == 3)

    # Approve 2
    record_approval(metrics, "R1", True)
    record_approval(metrics, "R2", True)
    record_approval(metrics, "R3", False)
    _check("approval_rate = 2/3",
           abs(metrics["approval_rate"] - 2/3) < 0.01,
           f"got {metrics['approval_rate']:.2f}")

    # Verdicts
    record_verdict(metrics, "R1", "KEEP")
    record_verdict(metrics, "R2", "ROLLBACK")
    _check("success_rate = 1/2 (1 KEEP / 2 approved)",
           abs(metrics["success_rate"] - 0.5) < 0.01,
           f"got {metrics['success_rate']:.2f}")
    _check("rolled_back count = 1", metrics["rolled_back"] == 1)

    # Check R1 status in history
    r1 = next(h for h in metrics["recommendation_history"] if h["id"] == "R1")
    _check("R1 status = KEEP", r1["status"] == "KEEP")

    r3 = next(h for h in metrics["recommendation_history"] if h["id"] == "R3")
    _check("R3 status = REJECTED", r3["status"] == "REJECTED")


# ==============================================================
# TEST 8: Recommender -insufficient data returns empty
# ==============================================================
def test_recommender_insufficient_data():
    print("\n-- TEST 8: Recommender -insufficient data guard --")
    days = _trading_days("20260301", 5)

    # Only 2 closes (< 5 minimum)
    closes_per_day = [
        [_make_close()] if i < 2 else []
        for i in range(5)
    ]
    snapshots = [_make_snapshot(d, closes=c) for d, c in zip(days, closes_per_day)]
    window = _build_test_window(snapshots, days[-1])

    recs = recommend_params(window)
    _check("returns empty with < 5 closes",
           len(recs) == 0, f"got {len(recs)} recs")


# ==============================================================
# MAIN
# ==============================================================
def main():
    print("=" * 60)
    print("  Phase 3 E2E Validation")
    print("=" * 60)

    _backup_state()

    try:
        # Recommender tests
        rec = test_recommender_trail_premature()
        test_recommender_trail_tighter()
        test_recommender_multi_signal()
        test_recommender_insufficient_data()

        # Approval pipeline (uses rec from test 1)
        if rec:
            test_approval_pipeline(rec)
        else:
            print("\n-- TEST 4: SKIPPED (no recommendation from test 1) --")

        # Post evaluator
        test_post_evaluator()

        # Drift guard
        test_drift_guard()

        # Metrics
        test_metrics()

    finally:
        _restore_state()

    # Summary
    total = PASS + FAIL
    print("\n" + "=" * 60)
    print(f"  Phase 3 Validation: {PASS}/{total} passed, {FAIL} failed")
    if FAIL == 0:
        print("  ALL PASS")
    else:
        print(f"  {FAIL} FAILURES -fix before proceeding to Phase 4")
    print("=" * 60)

    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
