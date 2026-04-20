"""
test_promotion_evidence.py — Promotion evidence gap coverage

Spec §검증 체크리스트:
  1) ops source 없음 -> UNKNOWN/evidence_missing, safe 처리 금지
  2) structured source의 명시적 0 -> PASS 처리
  3) unresolved duplicate=1 -> BLOCKED
  4) regime history 없음 -> coverage unknown, hardcoded 1 미사용
  5) bull/bear/sideways history -> coverage >= 2
  6) 동일 일자 EOD 두 번 호출 -> regime history 중복 기록 방지
  7) UI failures_by_category 에 sample 과 ops 가 별도 분류되는지

Run:
  .venv/Scripts/python.exe -m pytest kr/tests/test_promotion_evidence.py -v
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# make kr/ importable
_KR = Path(__file__).resolve().parent.parent
if str(_KR) not in sys.path:
    sys.path.insert(0, str(_KR))

import pytest


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_metrics(strategy="breakout_trend", group="event",
                  sample_days=120, total_trades=60, market="KR"):
    from lab.promotion.metrics import StrategyMetrics, METRICS_VERSION, FILL_MODEL, SLIPPAGE_MODEL
    return StrategyMetrics(
        strategy=strategy, group=group, market=market,
        sample_days=sample_days, total_trades=total_trades,
        rebal_cycles=5,
        sharpe=1.2, calmar=1.5, cagr_pct=15.0, mdd_pct=-15.0,
        consistency_score=0.6, positive_month_ratio=0.7,
        cost_drag_pct_year=2.0, effective_sharpe_after_slip=1.0,
        turnover_pct_month=50.0,
        metrics_version=METRICS_VERSION,
        cost_model_version="COST_MODEL_KR_V1",
        fill_model_version=FILL_MODEL,
        slippage_model_version=SLIPPAGE_MODEL,
    )


def _make_dq(regime_coverage=3, regime_flip=1):
    from lab.promotion.hard_gates import DataQualityMetrics
    return DataQualityMetrics(
        status="OK",
        missing_data_ratio=0.0,
        ohlcv_sync_status="OK",
        kospi_stale_days=0,
        regime_coverage=regime_coverage,
        regime_flip_observed=regime_flip,
    )


# ─── Test 1: ops source 없음 -> UNKNOWN, safe 처리 금지 ────────────────

def test_no_ops_source_results_in_unknown(tmp_path):
    """Check #1: source 없으면 CRITICAL 필드는 None 유지, state='UNKNOWN'."""
    from lab.promotion.evidence import collect_ops_evidence

    missing_state = tmp_path / "does_not_exist.json"
    missing_snap = tmp_path / "no_snap.json"
    ops = collect_ops_evidence(
        runtime_state_path=missing_state,
        log_summary_path=None,
        ops_snapshot_path=missing_snap,
    )

    # All CRITICAL fields must be None (UNKNOWN), not 0
    assert ops.recon_ok_streak is None
    assert ops.unresolved_broker_mismatch is None
    assert ops.duplicate_execution_count is None
    assert ops.stale_decision_input_count is None
    assert ops.dirty_exit_recovery_fail_count is None
    assert ops.pending_external_stale_cleanup_fail_count is None
    assert ops.state_uncertain_days_recent is None

    # has_any_unknown_critical == True -> hard_gate -> BLOCKED
    assert ops.has_any_unknown_critical() is True

    # hard_gate run -> critical_fail
    from lab.promotion.hard_gates import evaluate_promotion_hard_gates
    result = evaluate_promotion_hard_gates(_make_metrics(), ops, _make_dq())
    assert result["critical_fail"] is True
    assert len(result["evidence_missing"]) >= 7  # 7 CRITICAL ops fields
    assert "recon_ok_streak" in result["evidence_missing"]


# ─── Test 2: 명시적 0 -> PASS ────────────────────────────────────────

def test_structured_zero_is_pass(tmp_path):
    """Check #2: ops snapshot에 value=0 이면 UNKNOWN 아니라 PASS."""
    from runtime.ops_metrics import FieldEvidence, save_ops_snapshot
    from lab.promotion.evidence import collect_ops_evidence

    # All CRITICAL fields explicitly 0 (observed as clean)
    fields = {
        "recon_ok_streak_days":                      FieldEvidence(value=30, source="reconcile", window="total"),
        "broker_mismatch_unresolved_count":          FieldEvidence(value=0,  source="reconcile", window="open"),
        "duplicate_execution_incident_count":        FieldEvidence(value=0,  source="order_tracker", window="session"),
        "stale_decision_input_incident_count":       FieldEvidence(value=0,  source="decision",    window="session"),
        "dirty_exit_recovery_fail_count":            FieldEvidence(value=0,  source="startup",     window="session"),
        "pending_external_stale_cleanup_fail_count": FieldEvidence(value=0,  source="startup",     window="session"),
        "state_uncertain_days_recent":               FieldEvidence(value=0,  source="startup",     window="7d"),
    }
    snap_path = tmp_path / "ops_metrics.json"
    assert save_ops_snapshot(fields, path=snap_path, origin="test") is True

    ops = collect_ops_evidence(
        runtime_state_path=tmp_path / "no_rt.json",
        ops_snapshot_path=snap_path,
    )
    # values should be honored as 0 (not None)
    assert ops.recon_ok_streak == 30
    assert ops.unresolved_broker_mismatch == 0
    assert ops.duplicate_execution_count == 0
    assert ops.state_uncertain_days_recent == 0
    assert ops.has_any_unknown_critical() is False


# ─── Test 3: unresolved duplicate=1 -> BLOCKED ─────────────────────────

def test_duplicate_execution_blocks_promotion(tmp_path):
    from runtime.ops_metrics import FieldEvidence, save_ops_snapshot
    from lab.promotion.evidence import collect_ops_evidence
    from lab.promotion.hard_gates import evaluate_promotion_hard_gates

    fields = {
        "recon_ok_streak_days":                      FieldEvidence(value=30, source="reconcile"),
        "broker_mismatch_unresolved_count":          FieldEvidence(value=0,  source="reconcile"),
        "duplicate_execution_incident_count":        FieldEvidence(value=1,  source="order_tracker"),  # 🚨
        "stale_decision_input_incident_count":       FieldEvidence(value=0,  source="decision"),
        "dirty_exit_recovery_fail_count":            FieldEvidence(value=0,  source="startup"),
        "pending_external_stale_cleanup_fail_count": FieldEvidence(value=0,  source="startup"),
        "state_uncertain_days_recent":               FieldEvidence(value=0,  source="startup"),
    }
    snap_path = tmp_path / "ops_metrics.json"
    save_ops_snapshot(fields, path=snap_path, origin="test")

    ops = collect_ops_evidence(
        runtime_state_path=tmp_path / "no_rt.json",
        ops_snapshot_path=snap_path,
    )
    assert ops.duplicate_execution_count == 1

    result = evaluate_promotion_hard_gates(_make_metrics(), ops, _make_dq())
    assert result["critical_fail"] is True
    assert "ops" in result["failures_by_category"]
    assert "duplicate_execution_count" in result["failures_by_category"]["ops"]


# ─── Test 4: regime history 없음 -> coverage UNKNOWN ─────────────────

def test_regime_coverage_unknown_without_history(tmp_path):
    """Check #4: history 없으면 coverage=None (hardcoded=1 금지)."""
    from lab.promotion.regime_history import coverage_from_history

    empty_hist = tmp_path / "empty_history.jsonl"
    cov = coverage_from_history("breakout_trend", history_path=empty_hist)
    assert cov["regime_coverage"] is None
    assert cov["regime_flip_observed"] is None
    assert cov["total_days"] == 0

    # hard_gate 에 주입하면 UNKNOWN → BLOCKED
    from lab.promotion.hard_gates import DataQualityMetrics, evaluate_promotion_hard_gates
    dq = DataQualityMetrics(
        status="OK", missing_data_ratio=0.0, ohlcv_sync_status="OK",
        kospi_stale_days=0,
        regime_coverage=None, regime_flip_observed=None,
    )
    # Provide *valid* ops so regime gate is the only blocker
    from lab.promotion.hard_gates import OpsMetrics
    ops = OpsMetrics(
        recon_ok_streak=30, unresolved_broker_mismatch=0,
        duplicate_execution_count=0, stale_decision_input_count=0,
        dirty_exit_recovery_fail_count=0,
        pending_external_stale_cleanup_fail_count=0,
        state_uncertain_days_recent=0,
    )
    result = evaluate_promotion_hard_gates(_make_metrics(), ops, dq)

    # regime_coverage must appear in evidence_missing, not just failures
    assert "regime_coverage" in result["evidence_missing"]
    # overall must not pass
    assert result["all_pass"] is False


# ─── Test 5: bull/bear/sideways mixed -> coverage>=2 ─────────────────

def test_regime_coverage_from_mixed_history(tmp_path):
    from lab.promotion.regime_history import record_regime, coverage_from_history

    hp = tmp_path / "regime_history.jsonl"
    # Mixed regime observations
    scenarios = [
        ("2026-01-02", "BULL"),
        ("2026-01-03", "BULL"),
        ("2026-01-04", "BEAR"),   # flip 1
        ("2026-01-05", "BEAR"),
        ("2026-01-06", "SIDEWAYS"), # flip 2
    ]
    for i, (d, lbl) in enumerate(scenarios):
        ok = record_regime(
            trade_date=d, strategy_name="breakout_trend",
            regime_label=lbl, confidence=0.9,
            snapshot_version=f"{d}:DB:x:y:h{i}",
            history_path=hp,
        )
        assert ok is True

    cov = coverage_from_history("breakout_trend", history_path=hp)
    assert cov["regime_coverage"] == 3   # BULL, BEAR, SIDEWAYS
    assert cov["regime_flip_observed"] == 2
    assert cov["total_days"] == 5


# ─── Test 6: EOD duplicate call -> no duplicate record ───────────────

def test_regime_record_idempotent_same_snapshot(tmp_path):
    from lab.promotion.regime_history import record_regime, load_history

    hp = tmp_path / "rh.jsonl"
    sv = "2026-01-05:DB:2026-01-05:900:hash1"

    assert record_regime(
        trade_date="2026-01-05", strategy_name="breakout_trend",
        regime_label="BULL", confidence=0.8,
        snapshot_version=sv, history_path=hp,
    ) is True

    # Same snapshot_version -> skip (no duplicate)
    assert record_regime(
        trade_date="2026-01-05", strategy_name="breakout_trend",
        regime_label="BULL", confidence=0.8,
        snapshot_version=sv, history_path=hp,
    ) is False

    rows = load_history("breakout_trend", history_path=hp)
    assert len(rows) == 1

    # Different snapshot_version same day -> append (rerun)
    sv2 = "2026-01-05:DB:2026-01-05:900:hash2"
    assert record_regime(
        trade_date="2026-01-05", strategy_name="breakout_trend",
        regime_label="BULL", confidence=0.8,
        snapshot_version=sv2, history_path=hp,
    ) is True

    rows = load_history("breakout_trend", history_path=hp)
    assert len(rows) == 2


# ─── Test 7: evidence_missing vs failures_by_category 분리 ───────────

def test_failures_split_by_category(tmp_path):
    """Check #7: sample 부족과 ops 실패가 별도 카테고리로 분류된다."""
    from runtime.ops_metrics import FieldEvidence, save_ops_snapshot
    from lab.promotion.evidence import collect_ops_evidence
    from lab.promotion.hard_gates import evaluate_promotion_hard_gates

    fields = {
        "recon_ok_streak_days":                      FieldEvidence(value=5,  source="reconcile"),  # < 20 -> FAIL
        "broker_mismatch_unresolved_count":          FieldEvidence(value=0,  source="reconcile"),
        "duplicate_execution_incident_count":        FieldEvidence(value=0,  source="order_tracker"),
        "stale_decision_input_incident_count":       FieldEvidence(value=0,  source="decision"),
        "dirty_exit_recovery_fail_count":            FieldEvidence(value=0,  source="startup"),
        "pending_external_stale_cleanup_fail_count": FieldEvidence(value=0,  source="startup"),
        "state_uncertain_days_recent":               FieldEvidence(value=0,  source="startup"),
    }
    snap_path = tmp_path / "ops_metrics.json"
    save_ops_snapshot(fields, path=snap_path, origin="test")

    ops = collect_ops_evidence(
        runtime_state_path=tmp_path / "no_rt.json",
        ops_snapshot_path=snap_path,
    )

    # sample_days 부족 + ops 실패 동시
    metrics = _make_metrics(sample_days=10, total_trades=5)  # < 60 & < 30
    result = evaluate_promotion_hard_gates(metrics, ops, _make_dq())

    cats = result["failures_by_category"]
    assert "sample_days" in cats["sample"]
    assert "total_trades" in cats["sample"]
    assert "recon_ok_streak" in cats["ops"]
    assert result["critical_fail"] is True


# ─── Test 8: OrderTracker ops_snapshot exposes structured counts ──────

def test_order_tracker_ops_snapshot():
    from runtime.order_tracker import OrderTracker

    t = OrderTracker(trading_mode="mock")
    # Record a fill
    t.record_fill("ORD_0001", "BUY", "005930", 10, 100.0, cumulative_qty=10)
    # Duplicate fill (same fill_id)
    t.record_fill("ORD_0001", "BUY", "005930", 10, 100.0, cumulative_qty=10)
    # Another new fill
    t.record_fill("ORD_0002", "SELL", "000660", 5, 200.0, cumulative_qty=5)

    snap = t.ops_snapshot()
    assert snap["duplicate_execution_incident_count_total"] == 1
    assert snap["pending_external_unresolved_count"] == 0
    assert "session_id" in snap

    # Register + timeout
    rec = t.register("005930", "BUY", 10, 100.0)
    t.mark_timeout(rec.order_id)
    assert t.ops_snapshot()["order_timeout_events_total"] >= 1
    assert t.ops_snapshot()["timeout_uncertain_unresolved_count"] == 1


# ─── Test 9: Regime gate respects macro/regime group ─────────────────

def test_regime_flip_gate_macro_group(tmp_path):
    """macro/regime 그룹은 regime_flip_observed 도 hard gate."""
    from lab.promotion.hard_gates import DataQualityMetrics, OpsMetrics, evaluate_promotion_hard_gates

    ops = OpsMetrics(
        recon_ok_streak=30, unresolved_broker_mismatch=0,
        duplicate_execution_count=0, stale_decision_input_count=0,
        dirty_exit_recovery_fail_count=0,
        pending_external_stale_cleanup_fail_count=0,
        state_uncertain_days_recent=0,
    )
    dq = DataQualityMetrics(
        status="OK", missing_data_ratio=0.0, ohlcv_sync_status="OK",
        kospi_stale_days=0,
        regime_coverage=None, regime_flip_observed=None,
    )
    # macro 그룹은 regime_flip_observed 도 UNKNOWN -> evidence_missing
    metrics = _make_metrics(group="macro", sample_days=120, total_trades=15)
    result = evaluate_promotion_hard_gates(metrics, ops, dq)
    assert "regime_coverage" in result["evidence_missing"]
    assert "regime_flip_observed" in result["evidence_missing"]


# ─── Test 10: transition_log 중복 방지 ───────────────────────────────

def test_transition_log_dedup(tmp_path):
    from lab.promotion.transition_log import record_transition, load_transitions

    lp = tmp_path / "tlog.jsonl"
    # First record
    assert record_transition(
        strategy="breakout_trend", new_status="CANDIDATE",
        score=55, blockers=[], reason="test", log_path=lp,
    ) is True
    # Same status -> skip
    assert record_transition(
        strategy="breakout_trend", new_status="CANDIDATE",
        score=55, blockers=[], reason="test", log_path=lp,
    ) is False
    # Different status -> append
    assert record_transition(
        strategy="breakout_trend", new_status="PAPER_READY",
        score=70, blockers=[], reason="improved", log_path=lp,
    ) is True

    rows = load_transitions("breakout_trend", log_path=lp)
    assert len(rows) == 2
    assert rows[-1]["old_status"] == "CANDIDATE"
    assert rows[-1]["new_status"] == "PAPER_READY"


if __name__ == "__main__":
    # allow direct run without pytest
    import traceback
    failures = 0
    tests = [
        ("regime_history mixed",      test_regime_coverage_from_mixed_history),
        ("regime_history dedup",      test_regime_record_idempotent_same_snapshot),
        ("order_tracker ops",         test_order_tracker_ops_snapshot),
        ("transition_log dedup",      test_transition_log_dedup),
    ]
    for name, fn in tests:
        td = Path(__file__).parent / "_tmp_" / name.replace(" ", "_")
        td.mkdir(parents=True, exist_ok=True)
        try:
            fn(td) if fn.__code__.co_argcount else fn()
            print(f"PASS  {name}")
        except Exception:
            failures += 1
            print(f"FAIL  {name}")
            traceback.print_exc()
    sys.exit(1 if failures else 0)
