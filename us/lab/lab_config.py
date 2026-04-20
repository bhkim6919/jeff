# -*- coding: utf-8 -*-
"""
lab_config.py — US Strategy Lab Configuration
===============================================
10 strategies, 4 groups, US cost model.
"""
from __future__ import annotations

# ── Cost Model (US) ─────────────────────────────────────
BUY_COST = 0.0005       # 0.05% slippage (Alpaca commission-free)
SELL_COST = 0.0005       # 0.05% slippage (no tax in US)
INITIAL_CASH = 100_000   # $100K
CASH_BUFFER = 0.95       # 95% allocation cap

# ── Universe ────────────────────────────────────────────
DEFAULT_UNIVERSE = "RESEARCH_R1000"

# ── Strategy Groups ─────────────────────────────────────
# Cross-group comparison 금지 — 그룹 내에서만 비교
STRATEGY_GROUPS = {
    "rebal":  ["momentum_base", "lowvol_momentum", "quality_factor", "hybrid_qscore"],
    "event":  ["breakout_trend", "mean_reversion", "liquidity_signal"],
    "macro":  ["sector_rotation"],
    "regime": ["vol_regime"],
    "experimental": ["russell3000_lowvol"],
    # B군 (HA 필터 적용 독립 전략군)
    "rebal_ha":  ["momentum_base_ha", "lowvol_momentum_ha", "quality_factor_ha", "hybrid_qscore_ha"],
    "event_ha":  ["breakout_trend_ha", "mean_reversion_ha", "liquidity_signal_ha"],
    "macro_ha":  ["sector_rotation_ha"],
    "regime_ha": ["vol_regime_ha"],
    "experimental_ha": ["russell3000_lowvol_ha"],
}

# ── Per-Strategy Config ─────────────────────────────────
STRATEGY_CONFIGS = {
    # REBAL group (21-day rebalance, Trail -12%)
    "momentum_base": {
        "group": "rebal", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "Pure 12-1 month momentum, no filters",
    },
    "lowvol_momentum": {
        "group": "rebal", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "vol_percentile": 0.20, "universe": "RESEARCH_R1000",
        "description": "Gen4 core: LowVol 20%ile + Mom12-1",
    },
    "quality_factor": {
        "group": "rebal", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "ROE 40% + Value(1/PBR) 30% + Dividend 30%",
    },
    "hybrid_qscore": {
        "group": "rebal", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "RS 25% + Sector 20% + Quality 20% + Trend 15% + LowVol 20%",
    },
    # EVENT group (no scheduled rebalance, dynamic entry/exit)
    "breakout_trend": {
        "group": "event", "version": "v1",
        "max_positions": 15, "rebal_days": None, "trail_pct": 0.08,
        "breakout_window": 60, "universe": "RESEARCH_R1000",
        "description": "60-day high breakout, tight trail -8%",
    },
    "mean_reversion": {
        "group": "event", "version": "v1",
        "max_positions": 5, "rebal_days": None, "trail_pct": 0.05,
        "rsi_entry": 30, "rsi_exit": 50, "max_hold_days": 5,
        "universe": "RESEARCH_R1000",
        "description": "RSI<30 entry, MA200 filter, 5-day max hold",
    },
    "liquidity_signal": {
        "group": "event", "version": "v1",
        "max_positions": 10, "rebal_days": None, "trail_pct": 0.10,
        "vol_surge_ratio": 2.0, "universe": "RESEARCH_R1000",
        "description": "Volume 2x surge + green candle entry",
    },
    # MACRO group
    "sector_rotation": {
        "group": "macro", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "top_n_sectors": 3, "sector_window": 60, "universe": "RESEARCH_R1000",
        "description": "Top 3 sectors by 60d return → individual momentum",
    },
    # REGIME group (isolated)
    "vol_regime": {
        "group": "regime", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "VIX-based adaptive: high→LowVol, low→Momentum",
    },
    # EXPERIMENTAL (R3000)
    "russell3000_lowvol": {
        "group": "experimental", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "vol_percentile": 0.20, "universe": "RESEARCH_R3000",
        "description": "[EXPERIMENTAL] Gen4 core on Russell 3000 universe",
    },
    # ── B군 HA 전략 (독립 전략군, A군 파라미터 동일) ──────────────
    "momentum_base_ha": {
        "group": "rebal_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "B군: MomentumBase + HA entry/exit filter",
    },
    "lowvol_momentum_ha": {
        "group": "rebal_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "vol_percentile": 0.20, "universe": "RESEARCH_R1000",
        "description": "B군: LowVol+Mom + HA entry/exit filter",
    },
    "quality_factor_ha": {
        "group": "rebal_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "B군: QualityFactor + HA entry/exit filter",
    },
    "hybrid_qscore_ha": {
        "group": "rebal_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "B군: HybridQScore + HA entry/exit filter",
    },
    "breakout_trend_ha": {
        "group": "event_ha", "version": "v1",
        "max_positions": 15, "rebal_days": None, "trail_pct": 0.08,
        "breakout_window": 60, "universe": "RESEARCH_R1000",
        "description": "B군: BreakoutTrend + HA entry/exit filter",
    },
    "mean_reversion_ha": {
        "group": "event_ha", "version": "v1",
        "max_positions": 5, "rebal_days": None, "trail_pct": 0.05,
        "rsi_entry": 30, "rsi_exit": 50, "max_hold_days": 5,
        "universe": "RESEARCH_R1000",
        "description": "B군: MeanReversion + HA entry/exit filter",
    },
    "liquidity_signal_ha": {
        "group": "event_ha", "version": "v1",
        "max_positions": 10, "rebal_days": None, "trail_pct": 0.10,
        "vol_surge_ratio": 2.0, "universe": "RESEARCH_R1000",
        "description": "B군: LiquiditySignal + HA entry/exit filter",
    },
    "sector_rotation_ha": {
        "group": "macro_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "top_n_sectors": 3, "sector_window": 60, "universe": "RESEARCH_R1000",
        "description": "B군: SectorRotation + HA entry/exit filter",
    },
    "vol_regime_ha": {
        "group": "regime_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "universe": "RESEARCH_R1000",
        "description": "B군: VolRegime + HA entry/exit filter",
    },
    "russell3000_lowvol_ha": {
        "group": "experimental_ha", "version": "v1",
        "max_positions": 20, "rebal_days": 21, "trail_pct": 0.12,
        "vol_percentile": 0.20, "universe": "RESEARCH_R3000",
        "description": "B군: Russell3000LowVol + HA entry/exit filter",
    },
}

# ── Missing Data Thresholds (전략별) ────────────────────
MISSING_THRESHOLDS = {
    "momentum_base":      {"min_history": 252, "max_missing": 0.10},
    "lowvol_momentum":    {"min_history": 252, "max_missing": 0.10},
    "quality_factor":     {"min_history": 252, "max_missing": 0.15},
    "hybrid_qscore":      {"min_history": 252, "max_missing": 0.15},
    "breakout_trend":     {"min_history": 60,  "max_missing": 0.05},
    "mean_reversion":     {"min_history": 200, "max_missing": 0.10},
    "liquidity_signal":   {"min_history": 20,  "max_missing": 0.05},
    "sector_rotation":    {"min_history": 60,  "max_missing": 0.10},
    "vol_regime":         {"min_history": 252, "max_missing": 0.10},
    "russell3000_lowvol": {"min_history": 252, "max_missing": 0.20},
    # B군 HA (동일 threshold)
    "momentum_base_ha":      {"min_history": 252, "max_missing": 0.10},
    "lowvol_momentum_ha":    {"min_history": 252, "max_missing": 0.10},
    "quality_factor_ha":     {"min_history": 252, "max_missing": 0.15},
    "hybrid_qscore_ha":      {"min_history": 252, "max_missing": 0.15},
    "breakout_trend_ha":     {"min_history": 60,  "max_missing": 0.05},
    "mean_reversion_ha":     {"min_history": 200, "max_missing": 0.10},
    "liquidity_signal_ha":   {"min_history": 20,  "max_missing": 0.05},
    "sector_rotation_ha":    {"min_history": 60,  "max_missing": 0.10},
    "vol_regime_ha":         {"min_history": 252, "max_missing": 0.10},
    "russell3000_lowvol_ha": {"min_history": 252, "max_missing": 0.20},
}

# ── Metrics (필수 12개) ─────────────────────────────────
REQUIRED_METRICS = [
    "cagr", "mdd", "sharpe", "calmar",
    "turnover", "avg_hold_days", "trade_count", "win_rate",
    "exposure", "avg_positions",
    "exit_reason_distribution", "missing_data_ratio",
]
