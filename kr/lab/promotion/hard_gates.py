"""
hard_gates.py — Hard Gate Evaluation
=====================================
Per LIVE_PROMOTION_CRITERIA.md §2.

**원칙**: 하나라도 실패하면 BLOCKED. 감점 없음.
**운영 CRITICAL 실패는 즉시 BLOCKED** — 성과 점수 고려 없이.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

from .metrics import StrategyMetrics


# 그룹별 sample 요구치 (§4 그룹별 차등)
GROUP_MIN_SAMPLE_DAYS = {
    "event": 60,
    "rebal": 90,
    "macro": 120,
    "regime": 120,
    "unknown": 60,
}
GROUP_MIN_TRADES = {
    "event": 30,
    "rebal": 0,        # rebal은 cycles로 판정
    "macro": 10,
    "regime": 10,
    "unknown": 20,
}
GROUP_MIN_REBAL_CYCLES = {
    "rebal": 3,
}


@dataclass
class GateResult:
    gate_name: str
    passed: bool
    value: object
    required: object
    severity: str = "HARD"    # HARD / CRITICAL
    reason: str = ""
    # 3-state: PASS / FAIL / UNKNOWN
    # UNKNOWN = evidence missing (source 미연결). safe로 간주 금지.
    # default ""(빈 문자열) → __post_init__에서 passed 기반으로 유도.
    # 명시 state="UNKNOWN" 인 경우만 evidence_missing 으로 분류된다.
    state: str = ""
    evidence_source: str = ""   # "structured_state" / "log_summary" / "unknown"

    def __post_init__(self):
        if not self.state:
            # passed 기반 자동 유도: True → PASS, False → FAIL
            self.state = "PASS" if self.passed else "FAIL"

    def to_dict(self) -> Dict:
        return {
            "gate_name": self.gate_name,
            "pass": self.passed,
            "state": self.state,
            "value": self.value if self.state != "UNKNOWN" else None,
            "required": self.required,
            "severity": self.severity,
            "reason": self.reason,
            "evidence_source": self.evidence_source,
        }


@dataclass
class OpsMetrics:
    """운영 지표 (§2.3 + §5). Engine/RECON/State 추적기에서 수집.

    Optional[int] = UNKNOWN (evidence missing). 0과 UNKNOWN을 반드시 구분.
    - None: 감시자 미이식 / 소스 없음 → UNKNOWN
    - 0:    실제로 0회 관측 → PASS
    - > 0:  incident 발생 → FAIL

    가장 중요한 rule: **None을 0으로 대체 금지**.
    """
    # CRITICAL (None 허용, UNKNOWN 시 BLOCKED)
    recon_ok_streak: Optional[int] = None
    unresolved_broker_mismatch: Optional[int] = None
    duplicate_execution_count: Optional[int] = None
    stale_decision_input_count: Optional[int] = None

    # 추가 CRITICAL 범주 (spec §1)
    dirty_exit_recovery_fail_count: Optional[int] = None
    pending_external_stale_cleanup_fail_count: Optional[int] = None
    state_uncertain_days_recent: Optional[int] = None

    # HIGH (None 허용, UNKNOWN 시 soft 감점)
    recon_unreliable_events_24h: Optional[int] = None
    order_timeout_events_24h: Optional[int] = None
    ghost_fill_events_24h: Optional[int] = None

    # MEDIUM
    telegram_failures_24h: Optional[int] = None
    log_rotation_failures: Optional[int] = None

    # 각 필드의 source (디버깅/감사)
    evidence_sources: Dict[str, str] = field(default_factory=dict)

    def has_any_unknown_critical(self) -> bool:
        """CRITICAL 필드 중 하나라도 None이면 True."""
        critical_fields = [
            self.recon_ok_streak,
            self.unresolved_broker_mismatch,
            self.duplicate_execution_count,
            self.stale_decision_input_count,
            self.dirty_exit_recovery_fail_count,
            self.pending_external_stale_cleanup_fail_count,
            self.state_uncertain_days_recent,
        ]
        return any(v is None for v in critical_fields)

    def critical_unknown_fields(self) -> List[str]:
        """UNKNOWN인 CRITICAL 필드명 리스트."""
        out = []
        for name in [
            "recon_ok_streak",
            "unresolved_broker_mismatch",
            "duplicate_execution_count",
            "stale_decision_input_count",
            "dirty_exit_recovery_fail_count",
            "pending_external_stale_cleanup_fail_count",
            "state_uncertain_days_recent",
        ]:
            if getattr(self, name) is None:
                out.append(name)
        return out


@dataclass
class DataQualityMetrics:
    """데이터 품질 (§2.6).

    regime_coverage / regime_flip_observed는 **Optional[int]** —
    None = UNKNOWN (regime_history 없음). 0 은 "기록은 있으나 관측된 regime 없음"
    을 의미하므로 명확히 구분한다. hardcoded=1 fallback 금지.
    """
    status: str = "OK"                    # OK / PARTIAL / BAD / UNKNOWN
    missing_data_ratio: float = 0.0
    ohlcv_sync_status: str = "OK"
    kospi_stale_days: int = 0
    regime_coverage: Optional[int] = None       # None = UNKNOWN
    regime_flip_observed: Optional[int] = None  # None = UNKNOWN


def evaluate_promotion_hard_gates(
    strategy_metrics: StrategyMetrics,
    ops_metrics: OpsMetrics,
    data_quality: DataQualityMetrics,
) -> Dict:
    """Hard gates 전체 평가.

    Returns:
      {
        "all_pass": bool,
        "critical_fail": bool,
        "gates": [GateResult.to_dict(), ...],
        "blockers": [str, ...],
      }
    """
    gates: List[GateResult] = []

    # 2.1 Sample size
    min_days = GROUP_MIN_SAMPLE_DAYS.get(strategy_metrics.group, 60)
    gates.append(GateResult(
        "sample_days",
        strategy_metrics.sample_days >= min_days,
        strategy_metrics.sample_days, min_days,
        reason=f"{strategy_metrics.sample_days} < {min_days}" if strategy_metrics.sample_days < min_days else "",
    ))

    min_trades = GROUP_MIN_TRADES.get(strategy_metrics.group, 20)
    if strategy_metrics.group == "event":
        gates.append(GateResult(
            "total_trades", strategy_metrics.total_trades >= min_trades,
            strategy_metrics.total_trades, min_trades,
            reason=f"{strategy_metrics.total_trades} < {min_trades}" if strategy_metrics.total_trades < min_trades else "",
        ))

    if strategy_metrics.group == "rebal":
        min_cycles = GROUP_MIN_REBAL_CYCLES.get("rebal", 3)
        gates.append(GateResult(
            "rebal_cycles", strategy_metrics.rebal_cycles >= min_cycles,
            strategy_metrics.rebal_cycles, min_cycles,
            reason=f"{strategy_metrics.rebal_cycles} < {min_cycles}" if strategy_metrics.rebal_cycles < min_cycles else "",
        ))

    # 2.2 Regime coverage — Optional 처리, None = UNKNOWN → BLOCKED
    _rc = data_quality.regime_coverage
    if _rc is None:
        gates.append(GateResult(
            "regime_coverage", False, None, 2,
            severity="HARD", state="UNKNOWN",
            reason="EVIDENCE_MISSING (regime_history)",
            evidence_source="regime_history_empty",
        ))
    else:
        _passed = _rc >= 2
        gates.append(GateResult(
            "regime_coverage", _passed, _rc, 2,
            severity="HARD",
            state="PASS" if _passed else "FAIL",
            reason="" if _passed else f"only {_rc} regimes observed",
        ))

    if strategy_metrics.group in ("macro", "regime"):
        _rf = data_quality.regime_flip_observed
        if _rf is None:
            gates.append(GateResult(
                "regime_flip_observed", False, None, 1,
                severity="HARD", state="UNKNOWN",
                reason="EVIDENCE_MISSING (regime_history)",
                evidence_source="regime_history_empty",
            ))
        else:
            _flip_pass = _rf >= 1
            gates.append(GateResult(
                "regime_flip_observed", _flip_pass, _rf, 1,
                severity="HARD",
                state="PASS" if _flip_pass else "FAIL",
                reason="" if _flip_pass else "no regime flip observed",
            ))

    # 2.3 Operational (CRITICAL) — 3-state 처리
    # UNKNOWN = evidence missing → BLOCKED (safe 간주 금지)
    def _critical_gate(name: str, value: Optional[int], req_op: str, req: int,
                       reason_fmt_fail: str) -> GateResult:
        src = ops_metrics.evidence_sources.get(name, "unknown")
        if value is None:
            return GateResult(
                name, False, None, req,
                severity="CRITICAL", state="UNKNOWN",
                reason=f"EVIDENCE_MISSING ({name})",
                evidence_source=src,
            )
        # req_op: ">=" or "=="
        if req_op == ">=":
            passed = value >= req
        else:
            passed = value == req
        return GateResult(
            name, passed, value, req,
            severity="CRITICAL", state="PASS" if passed else "FAIL",
            reason="" if passed else reason_fmt_fail.format(value=value),
            evidence_source=src,
        )

    gates.append(_critical_gate(
        "recon_ok_streak", ops_metrics.recon_ok_streak, ">=", 20,
        "recon streak only {value}",
    ))
    gates.append(_critical_gate(
        "unresolved_broker_mismatch", ops_metrics.unresolved_broker_mismatch, "==", 0,
        "{value} mismatches",
    ))
    gates.append(_critical_gate(
        "duplicate_execution_count", ops_metrics.duplicate_execution_count, "==", 0,
        "{value} duplicates",
    ))
    gates.append(_critical_gate(
        "stale_decision_input_count", ops_metrics.stale_decision_input_count, "==", 0,
        "{value} stale input events",
    ))
    # 추가 CRITICAL 항목 (spec §1)
    gates.append(_critical_gate(
        "dirty_exit_recovery_fail_count", ops_metrics.dirty_exit_recovery_fail_count,
        "==", 0, "{value} dirty exit recovery failures",
    ))
    gates.append(_critical_gate(
        "pending_external_stale_cleanup_fail_count",
        ops_metrics.pending_external_stale_cleanup_fail_count,
        "==", 0, "{value} pending cleanup failures",
    ))
    gates.append(_critical_gate(
        "state_uncertain_days_recent", ops_metrics.state_uncertain_days_recent,
        "==", 0, "{value} state uncertain days",
    ))

    # 2.4 Performance floor
    gates.append(GateResult(
        "mdd_pct_floor",
        strategy_metrics.mdd_pct >= -25.0,
        round(strategy_metrics.mdd_pct, 2), -25.0,
        reason=f"MDD {strategy_metrics.mdd_pct:.1f}% < −25%" if strategy_metrics.mdd_pct < -25.0 else "",
    ))
    gates.append(GateResult(
        "sharpe_floor",
        strategy_metrics.sharpe >= 0.5,
        round(strategy_metrics.sharpe, 2), 0.5,
        reason=f"Sharpe {strategy_metrics.sharpe:.2f} < 0.5" if strategy_metrics.sharpe < 0.5 else "",
    ))
    gates.append(GateResult(
        "cost_drag_ceiling",
        strategy_metrics.cost_drag_pct_year <= 5.0,
        round(strategy_metrics.cost_drag_pct_year, 2), 5.0,
        reason=f"cost drag {strategy_metrics.cost_drag_pct_year:.1f}%/year > 5%" if strategy_metrics.cost_drag_pct_year > 5.0 else "",
    ))

    # 2.5 Cost/fill/slip 적용 여부 (버전 필드 존재 check)
    gates.append(GateResult(
        "cost_model_applied",
        bool(strategy_metrics.cost_model_version),
        strategy_metrics.cost_model_version or "MISSING", "set",
        reason="cost_model_version missing" if not strategy_metrics.cost_model_version else "",
    ))
    gates.append(GateResult(
        "fill_model_applied",
        bool(strategy_metrics.fill_model_version),
        strategy_metrics.fill_model_version or "MISSING", "set",
        reason="fill_model_version missing" if not strategy_metrics.fill_model_version else "",
    ))
    gates.append(GateResult(
        "slippage_buffer_applied",
        bool(strategy_metrics.slippage_model_version),
        strategy_metrics.slippage_model_version or "MISSING", "set",
        reason="slippage_model_version missing" if not strategy_metrics.slippage_model_version else "",
    ))

    # 2.6 Data quality
    gates.append(GateResult(
        "data_quality_status",
        data_quality.status != "BAD",
        data_quality.status, "not BAD",
        reason=f"DQ is {data_quality.status}" if data_quality.status == "BAD" else "",
    ))
    gates.append(GateResult(
        "missing_data_ratio",
        data_quality.missing_data_ratio <= 0.05,
        round(data_quality.missing_data_ratio, 3), 0.05,
        reason=f"missing {data_quality.missing_data_ratio:.1%} > 5%" if data_quality.missing_data_ratio > 0.05 else "",
    ))
    gates.append(GateResult(
        "ohlcv_sync_status",
        data_quality.ohlcv_sync_status in ("OK", "PARTIAL"),
        data_quality.ohlcv_sync_status, "OK or PARTIAL",
        reason=f"sync status {data_quality.ohlcv_sync_status}" if data_quality.ohlcv_sync_status not in ("OK", "PARTIAL") else "",
    ))
    if strategy_metrics.market == "KR":
        gates.append(GateResult(
            "kospi_stale_days",
            data_quality.kospi_stale_days <= 2,
            data_quality.kospi_stale_days, 2,
            reason=f"KOSPI stale {data_quality.kospi_stale_days} days" if data_quality.kospi_stale_days > 2 else "",
        ))

    # 집계 — UNKNOWN은 별도 분류 (evidence_missing)
    blockers: List[str] = []
    critical_fail = False
    evidence_missing: List[str] = []
    sample_failures: List[str] = []
    ops_failures: List[str] = []
    perf_failures: List[str] = []
    quality_failures: List[str] = []

    # 분류를 위한 gate group mapping
    sample_gates = {"sample_days", "total_trades", "rebal_cycles",
                    "regime_coverage", "regime_flip_observed"}
    ops_gates = {"recon_ok_streak", "unresolved_broker_mismatch",
                 "duplicate_execution_count", "stale_decision_input_count",
                 "dirty_exit_recovery_fail_count",
                 "pending_external_stale_cleanup_fail_count",
                 "state_uncertain_days_recent"}
    perf_gates = {"mdd_pct_floor", "sharpe_floor", "cost_drag_ceiling"}
    quality_gates = {"data_quality_status", "missing_data_ratio",
                     "ohlcv_sync_status", "kospi_stale_days",
                     "cost_model_applied", "fill_model_applied",
                     "slippage_buffer_applied"}

    for g in gates:
        if g.state == "PASS":
            continue

        prefix = "[CRITICAL] " if g.severity == "CRITICAL" else ""
        msg = f"{prefix}{g.gate_name}"
        if g.reason:
            msg += f":{g.reason}"

        blockers.append(msg)

        if g.state == "UNKNOWN":
            evidence_missing.append(g.gate_name)
            # CRITICAL UNKNOWN = critical_fail (BLOCKED)
            if g.severity == "CRITICAL":
                critical_fail = True
        elif g.state == "FAIL":
            if g.severity == "CRITICAL":
                critical_fail = True
            if g.gate_name in sample_gates:
                sample_failures.append(g.gate_name)
            elif g.gate_name in ops_gates:
                ops_failures.append(g.gate_name)
            elif g.gate_name in perf_gates:
                perf_failures.append(g.gate_name)
            elif g.gate_name in quality_gates:
                quality_failures.append(g.gate_name)

    all_pass = len(blockers) == 0

    return {
        "all_pass": all_pass,
        "critical_fail": critical_fail,
        "gates": [g.to_dict() for g in gates],
        "blockers": blockers,
        # 분류된 실패 이유 — UI에서 "sample 부족" vs "evidence missing" 구분용
        "evidence_missing": evidence_missing,
        "failures_by_category": {
            "sample": sample_failures,
            "ops": ops_failures,
            "performance": perf_failures,
            "quality": quality_failures,
        },
    }
