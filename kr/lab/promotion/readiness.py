"""
readiness.py — Readiness Score (0~100, soft)
==============================================
Per LIVE_PROMOTION_CRITERIA.md §3 + spec §2.

Hard gates 모두 통과한 전략에 한해 4개 subscore 가중평균.

**UNKNOWN 처리 원칙** (spec §2):
- 항목이 UNKNOWN이면 **가점 금지** (0점 간주 아님, 해당 항목 제외)
- 제외된 항목 만큼 가중치 재분배 — 단, 가중치 재분배 후 알려진 항목 점수는
  **부풀리지 않음**. 최종 점수는 (known_weight / total_weight) 스케일 유지.
- evidence_coverage 필드로 "몇 % 증거 기반인지" 노출

운영 실패 severity는 감점으로 반영 (HIGH −10, MEDIUM −3, LOW −1).
"""
from __future__ import annotations

from typing import Dict, List, Tuple

from .metrics import StrategyMetrics, norm
from .hard_gates import OpsMetrics, DataQualityMetrics


# Severity-based deductions (§5)
HIGH_DEDUCT = 10
MEDIUM_DEDUCT = 3
LOW_DEDUCT = 1


def _performance_subscore(m: StrategyMetrics) -> Tuple[float, List[str]]:
    """§3.1 — 0~1. 성과는 equity_history에서 유래 → 항상 known (metric level).

    Returns (score, unknown_fields) — performance는 unknown 없음.
    """
    score = (
        0.4 * norm(m.sharpe, 1.0, 2.0)
        + 0.3 * norm(m.calmar, 0.8, 2.0)
        + 0.2 * norm(m.cagr_pct, 5, 25)
        + 0.1 * norm(m.consistency_score, 0.3, 0.8)
    )
    return score, []


def _stability_subscore(m: StrategyMetrics, ops: OpsMetrics) -> Tuple[float, List[str]]:
    """§3.2 — unknown 있으면 해당 항목 제외 + 가중치 재분배.

    가중치: mdd 0.3, positive_month 0.3, recon_streak 0.2, stale_input 0.2
    """
    parts = []   # list of (weight, value)
    unknown = []

    # mdd (metrics-derived, 항상 known)
    parts.append((0.3, max(0.0, 1 - abs(m.mdd_pct) / 40)))
    # positive_month_ratio (metrics-derived)
    parts.append((0.3, m.positive_month_ratio))

    # recon_ok_streak (ops, UNKNOWN 가능)
    if ops.recon_ok_streak is None:
        unknown.append("recon_ok_streak")
    else:
        parts.append((0.2, norm(ops.recon_ok_streak, 20, 60)))

    # stale_decision_input_count (ops, UNKNOWN 가능)
    if ops.stale_decision_input_count is None:
        unknown.append("stale_decision_input_count")
    else:
        if m.sample_days > 0:
            stale_ratio = min(1.0, ops.stale_decision_input_count / m.sample_days)
        else:
            stale_ratio = 0.0
        parts.append((0.2, 1.0 - stale_ratio))

    # 가중치 재분배: known weight 합 기준 정규화
    total_w = sum(w for w, _ in parts)
    if total_w <= 0:
        return 0.0, unknown
    weighted = sum(w * v for w, v in parts) / total_w

    # 최종 스코어는 (known_weight_ratio) 곱해서 over-claim 방지
    coverage = total_w / 1.0  # original total = 1.0
    score_scaled = weighted * coverage
    return score_scaled, unknown


def _operational_subscore(m: StrategyMetrics, ops: OpsMetrics,
                          dq: DataQualityMetrics) -> Tuple[float, List[str]]:
    """§3.3 — unknown 있으면 제외 + 스케일 축소.

    가중치: critical_ops_pass 0.4, ohlcv_complete 0.3, failed_fill 0.2, regime 0.1
    """
    parts: List[Tuple[float, float]] = []
    unknown: List[str] = []

    # critical_ops_pass — 모든 CRITICAL known & 전부 0인지
    crit_unknown = ops.critical_unknown_fields()
    if crit_unknown:
        # 일부 CRITICAL UNKNOWN → 이 항목 제외
        unknown.extend(crit_unknown)
    else:
        critical_ops_pass = (
            ops.unresolved_broker_mismatch == 0
            and ops.duplicate_execution_count == 0
            and ops.stale_decision_input_count == 0
            and (ops.dirty_exit_recovery_fail_count or 0) == 0
            and (ops.pending_external_stale_cleanup_fail_count or 0) == 0
            and (ops.state_uncertain_days_recent or 0) == 0
        )
        parts.append((0.4, 1.0 if critical_ops_pass else 0.0))

    # ohlcv_complete_ratio (DQ)
    ohlcv_complete = 1.0 - dq.missing_data_ratio
    parts.append((0.3, norm(ohlcv_complete, 0.9, 1.0)))

    # failed_fill_ratio — order_timeout + ghost_fill
    if ops.order_timeout_events_24h is None and ops.ghost_fill_events_24h is None:
        unknown.append("fill_events")
    else:
        est_orders = max(1, m.total_trades)
        timeout = ops.order_timeout_events_24h or 0
        ghost = ops.ghost_fill_events_24h or 0
        failed_fill_ratio = min(1.0, (timeout + ghost) / est_orders)
        parts.append((0.2, 1.0 - failed_fill_ratio))

    # regime_adequacy (DQ) — None = UNKNOWN (해당 항목 제외, 가점 금지)
    if dq.regime_coverage is None:
        unknown.append("regime_coverage")
    else:
        regime_adequacy = 0.0
        if dq.regime_coverage >= 2:
            flip = dq.regime_flip_observed or 0
            regime_adequacy = 0.5 + (0.1 * min(5, flip))
        regime_adequacy = min(1.0, regime_adequacy)
        parts.append((0.1, regime_adequacy))

    total_w = sum(w for w, _ in parts)
    if total_w <= 0:
        return 0.0, unknown
    weighted = sum(w * v for w, v in parts) / total_w
    coverage = total_w / 1.0
    return weighted * coverage, unknown


def _cost_realism_subscore(m: StrategyMetrics) -> Tuple[float, List[str]]:
    """§3.4 — metric-derived 값만 사용, UNKNOWN 없음."""
    cost_component = max(0.0, 1.0 - m.cost_drag_pct_year / 5.0)
    slip_component = norm(m.effective_sharpe_after_slip, 0.7, 1.5)
    turnover_penalty = 0.0
    if m.turnover_pct_month > 100:
        turnover_penalty = min(1.0, (m.turnover_pct_month - 100) / 100)
    score = (
        0.5 * cost_component
        + 0.3 * slip_component
        + 0.2 * (1.0 - turnover_penalty)
    )
    return score, []


def compute_readiness_score(
    strategy_metrics: StrategyMetrics,
    ops_metrics: OpsMetrics,
    data_quality: DataQualityMetrics,
) -> Dict:
    """Total score + subscore breakdown + evidence coverage.

    Returns:
      {
        "total_score": int (0-100),
        "subscores": {
          "performance": float 0-100,
          "stability":   float 0-100,
          "operational": float 0-100,
          "cost_realism":float 0-100,
        },
        "deductions": int (severity-based),
        "deduction_reasons": [str, ...],
        "evidence_coverage": 0.0~1.0,   # (known subscore weight / total)
        "unknown_fields": [str, ...],    # 전체 unknown field 리스트
      }
    """
    perf, unk_perf = _performance_subscore(strategy_metrics)
    stab, unk_stab = _stability_subscore(strategy_metrics, ops_metrics)
    ops_s, unk_ops = _operational_subscore(strategy_metrics, ops_metrics, data_quality)
    cost_s, unk_cost = _cost_realism_subscore(strategy_metrics)

    all_unknown = unk_perf + unk_stab + unk_ops + unk_cost

    # 각 subscore는 이미 자기 가중치 내에서 coverage 스케일 적용됨 (§3 원칙)
    # Total: 동일 가중치 (30/30/20/20) 유지 — unknown 으로 비어있는 부분은 자연 감소
    total = (
        perf * 30
        + stab * 30
        + ops_s * 20
        + cost_s * 20
    )

    # Severity deductions — None 항목은 감점 대상 아님 (unknown은 감점 대상 아님)
    deductions = 0
    reasons = []
    if (ops_metrics.recon_unreliable_events_24h or 0) > 0:
        d = HIGH_DEDUCT * min(3, ops_metrics.recon_unreliable_events_24h)
        deductions += d
        reasons.append(f"recon_unreliable:-{d}")
    if (ops_metrics.order_timeout_events_24h or 0) >= 3:
        deductions += HIGH_DEDUCT
        reasons.append(f"order_timeout:-{HIGH_DEDUCT}")
    if (ops_metrics.ghost_fill_events_24h or 0) > 0:
        deductions += HIGH_DEDUCT
        reasons.append(f"ghost_fill:-{HIGH_DEDUCT}")
    if (ops_metrics.telegram_failures_24h or 0) > 0:
        deductions += MEDIUM_DEDUCT
        reasons.append(f"telegram_fail:-{MEDIUM_DEDUCT}")
    if (ops_metrics.log_rotation_failures or 0) > 0:
        deductions += LOW_DEDUCT
        reasons.append(f"log_rotation:-{LOW_DEDUCT}")

    total_after_deduct = max(0, int(round(total - deductions)))

    # Evidence coverage — 전체 subscore 중 known 비율
    # perf/cost는 항상 100% known. stab/ops는 unknown 영향.
    # 간이 계산: (4 - unknown_subscore_count) / 4
    unknown_subscore_count = (
        (1 if unk_stab else 0)
        + (1 if unk_ops else 0)
    )
    evidence_coverage = 1.0 - (unknown_subscore_count * 0.25)

    return {
        "total_score": total_after_deduct,
        "subscores": {
            "performance": round(perf * 100, 1),
            "stability":   round(stab * 100, 1),
            "operational": round(ops_s * 100, 1),
            "cost_realism":round(cost_s * 100, 1),
        },
        "deductions": deductions,
        "deduction_reasons": reasons,
        "raw_total_before_deduct": round(total, 1),
        "evidence_coverage": round(evidence_coverage, 2),
        "unknown_fields": sorted(set(all_unknown)),
    }
