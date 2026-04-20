"""
adapters.py — Lab Engine → Promotion 입력 변환
==============================================
Lab lane(LabLiveLane) → StrategyMetrics
Runtime state / engine health → OpsMetrics
Meta / sync state → DataQualityMetrics

Promotion 판정을 부르기 전 이 모듈을 통해 입력을 표준화.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .metrics import compute_metrics_from_lane, StrategyMetrics
from .hard_gates import OpsMetrics, DataQualityMetrics


# 전략명 → (그룹, factor_tag) 매핑 (KR 18 + US 20)
_STRATEGY_GROUP_MAP = {
    # KR
    "breakout_trend":     ("event",  "momentum"),
    "breakout_trend_ha":  ("event",  "momentum"),
    "hybrid_qscore":      ("rebal",  "quality"),
    "hybrid_qscore_ha":   ("rebal",  "quality"),
    "liquidity_signal":   ("event",  "liquidity"),
    "liquidity_signal_ha":("event",  "liquidity"),
    "lowvol_momentum":    ("rebal",  "vol_momentum"),
    "lowvol_momentum_ha": ("rebal",  "vol_momentum"),
    "mean_reversion":     ("event",  "reversion"),
    "mean_reversion_ha":  ("event",  "reversion"),
    "momentum_base":      ("rebal",  "momentum"),
    "momentum_base_ha":   ("rebal",  "momentum"),
    "quality_factor":     ("rebal",  "quality"),
    "quality_factor_ha":  ("rebal",  "quality"),
    "sector_rotation":    ("macro",  "sector"),
    "sector_rotation_ha": ("macro",  "sector"),
    "vol_regime":         ("regime", "vol"),
    "vol_regime_ha":      ("regime", "vol"),
    # US
    "russell3000_lowvol":    ("rebal",  "vol"),
    "russell3000_lowvol_ha": ("rebal",  "vol"),
}


def resolve_group(strategy_name: str) -> str:
    return _STRATEGY_GROUP_MAP.get(strategy_name, ("unknown", "unknown"))[0]


def resolve_factor(strategy_name: str) -> str:
    return _STRATEGY_GROUP_MAP.get(strategy_name, ("unknown", "unknown"))[1]


def lane_to_metrics(
    lane,
    *,
    strategy: str,
    initial_cash: float,
    market: str = "KR",
) -> StrategyMetrics:
    """LabLiveLane-like 객체 → StrategyMetrics."""
    group = resolve_group(strategy)
    return compute_metrics_from_lane(
        strategy=strategy,
        group=group,
        market=market,
        equity_history=getattr(lane, "equity_history", []) or [],
        trades=getattr(lane, "trades", []) or [],
        initial_cash=initial_cash,
        current_positions=getattr(lane, "positions", {}) or {},
        current_cash=getattr(lane, "cash", 0) or 0,
        rebal_cycles=getattr(lane, "rebal_count", None),
    )


def runtime_to_ops(
    runtime_state_path=None,
    log_summary_path=None,
    ops_snapshot_path=None,
) -> OpsMetrics:
    """Evidence collector 기반 — default 0 금지, UNKNOWN은 None.

    Source priority (evidence.py):
      1) ops_snapshot_path  (kr/data/ops/ops_metrics.json — 누적 evidence 전용)
      2) runtime_state_path (kr/state/runtime_state_live.json — 현재 상태)
      3) log_summary_path   (kr/data/logs/summary/runtime_summary.json — fallback)

    Returns: OpsMetrics (일부 None 가능 = UNKNOWN)
    """
    from pathlib import Path as _Path
    from .evidence import collect_ops_evidence

    _kr_root = _Path(__file__).resolve().parent.parent.parent
    if runtime_state_path is None:
        runtime_state_path = _kr_root / "state" / "runtime_state_live.json"
    if log_summary_path is None:
        log_summary_path = _kr_root / "data" / "logs" / "summary" / "runtime_summary.json"
    if ops_snapshot_path is None:
        ops_snapshot_path = _kr_root / "data" / "ops" / "ops_metrics.json"

    return collect_ops_evidence(
        runtime_state_path=runtime_state_path,
        log_summary_path=log_summary_path,
        ops_snapshot_path=ops_snapshot_path,
    )


def build_data_quality(
    run_meta: Dict,
    ohlcv_sync: Dict,
    meta_strategy_fit: Dict = None,
    *,
    strategy_name: Optional[str] = None,
) -> DataQualityMetrics:
    """Lab engine run_meta + ohlcv_sync.json + meta.strategy_fit → DataQualityMetrics.

    Regime coverage는 ``regime_history.coverage_from_history(strategy_name)`` 만
    단일 source 로 사용한다. history 가 없으면 UNKNOWN (None) — hardcoded fallback
    을 두지 않는다.
    """
    run_meta = run_meta or {}
    ohlcv_sync = ohlcv_sync or {}
    meta_strategy_fit = meta_strategy_fit or {}

    mc = run_meta.get("market_context", {}) or {}
    stock_last = mc.get("stock_last_date", "")
    kospi_last = mc.get("kospi_last_date", "")
    # 간이 KOSPI stale 계산 (일 단위)
    kospi_stale_days = 0
    try:
        from datetime import date
        if stock_last and kospi_last:
            sd = date.fromisoformat(stock_last)
            kd = date.fromisoformat(kospi_last)
            kospi_stale_days = max(0, (sd - kd).days)
    except Exception:
        pass

    # Regime coverage — history 단일 source (hardcoded fallback 금지)
    regime_coverage: Optional[int] = None
    regime_flip: Optional[int] = None
    if strategy_name:
        try:
            from .regime_history import coverage_from_history
            cov = coverage_from_history(strategy_name)
            regime_coverage = cov["regime_coverage"]  # None 가능
            regime_flip = cov["regime_flip_observed"]  # None 가능
        except Exception:
            regime_coverage = None
            regime_flip = None

    # DQ status
    dq_statuses = []
    for _s, f in meta_strategy_fit.items():
        status = (f.get("data_quality", {}) or {}).get("status", "UNKNOWN")
        dq_statuses.append(status)
    if "BAD" in dq_statuses:
        dq_status = "BAD"
    elif "UNKNOWN" in dq_statuses:
        dq_status = "PARTIAL"
    else:
        dq_status = "OK"

    # missing_data_ratio — engine이 trace하는 값 사용 (없으면 0)
    missing = float(run_meta.get("missing_data_ratio", 0) or 0) / 100  # engine은 % 단위

    return DataQualityMetrics(
        status=dq_status,
        missing_data_ratio=missing,
        ohlcv_sync_status=str(ohlcv_sync.get("sync_status", "?")),
        kospi_stale_days=kospi_stale_days,
        regime_coverage=regime_coverage,
        regime_flip_observed=regime_flip,
    )
