"""
lab_config.py — Strategy Lab configuration
============================================
LabConfig: 전체 Lab 실행 설정
StrategyConfig: 전략별 개별 제약
STRATEGY_GROUPS: 그룹 분리 (비교는 그룹 내만)
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional


class FillTiming(Enum):
    NEXT_OPEN = "next_open"          # T-1 signal → T open fill (기본)
    SAME_DAY_CLOSE = "same_day"      # signal → 당일 close (experimental only)


# ── Strategy Groups (cross-group 비교 금지) ──────────────────────
STRATEGY_GROUPS: Dict[str, List[str]] = {
    "rebal":  ["momentum_base", "lowvol_momentum", "quality_factor", "hybrid_qscore"],
    "event":  ["breakout_trend", "mean_reversion", "liquidity_signal"],
    "macro":  ["sector_rotation"],
    "regime": ["vol_regime"],  # isolated — 다른 전략과 절대 혼합 금지
    # B군 (HA 필터 적용 독립 전략군) — 2026-04-23 DISABLED:
    # .py source 유실 (git history 없음), .pyc only → import 에러 9개/restart.
    # Option (b) 적용: config 에서 제거해 로그 정화. 재작성 후 주석 복구.
    # "rebal_ha":  ["momentum_base_ha", "lowvol_momentum_ha", "quality_factor_ha", "hybrid_qscore_ha"],
    # "event_ha":  ["breakout_trend_ha", "mean_reversion_ha", "liquidity_signal_ha"],
    # "macro_ha":  ["sector_rotation_ha"],
    # "regime_ha": ["vol_regime_ha"],
}

DISABLE_CROSS_GROUP_COMPARISON = True

# ── Exposure soft bands (warn only, hard assert 아님) ──────────
EXPECTED_EXPOSURE: Dict[str, tuple] = {
    "rebal":  (0.70, 0.95),
    "event":  (0.20, 0.80),
    "regime": (0.10, 0.95),
    "macro":  (0.60, 0.95),
    # HA exposure bands kept — will be re-activated when HA 재구현
    "rebal_ha":  (0.70, 0.95),
    "event_ha":  (0.20, 0.80),
    "regime_ha": (0.10, 0.95),
    "macro_ha":  (0.60, 0.95),
}


@dataclass
class StrategyConfig:
    """전략별 개별 제약. 전략마다 다르게 설정 가능."""
    max_positions: int = 20
    rebal_days: Optional[int] = 21       # None = 리밸런싱 없음 (event)
    fill_timing: FillTiming = FillTiming.NEXT_OPEN
    min_expected_hold: int = 10          # avg_hold_days 하한 (검증용, warn)
    group: str = "rebal"
    version: str = "v1"                  # 전략 로직 변경 시 bump (meta DB 추적용)


# ── Per-strategy config map ──────────────────────────────────────
STRATEGY_CONFIGS: Dict[str, StrategyConfig] = {
    "momentum_base":    StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal"),
    "lowvol_momentum":  StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal"),
    "quality_factor":   StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal"),
    "hybrid_qscore":    StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal"),
    "breakout_trend":   StrategyConfig(max_positions=15, rebal_days=None, min_expected_hold=3, group="event"),
    "mean_reversion":   StrategyConfig(max_positions=5,  rebal_days=None, min_expected_hold=1, group="event"),
    "liquidity_signal": StrategyConfig(max_positions=10, rebal_days=None, min_expected_hold=2, group="event"),
    "sector_rotation":  StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="macro"),
    "vol_regime":       StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="regime"),
    # ── B군 HA 전략 (독립 전략군, A군 파라미터 동일) ──────────────
    # 2026-04-23 DISABLED — .py source 유실, .pyc only → import 실패.
    # 재작성 후 주석 복구하면 Lab Live 가 자동 가동.
    # "momentum_base_ha":    StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal_ha"),
    # "lowvol_momentum_ha":  StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal_ha"),
    # "quality_factor_ha":   StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal_ha"),
    # "hybrid_qscore_ha":    StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="rebal_ha"),
    # "breakout_trend_ha":   StrategyConfig(max_positions=15, rebal_days=None, min_expected_hold=3, group="event_ha"),
    # "mean_reversion_ha":   StrategyConfig(max_positions=5,  rebal_days=None, min_expected_hold=1, group="event_ha"),
    # "liquidity_signal_ha": StrategyConfig(max_positions=10, rebal_days=None, min_expected_hold=2, group="event_ha"),
    # "sector_rotation_ha":  StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="macro_ha"),
    # "vol_regime_ha":       StrategyConfig(max_positions=20, rebal_days=21, min_expected_hold=10, group="regime_ha"),
}


@dataclass
class LabConfig:
    """전체 Lab 실행 설정. BASELINE_SPEC 기준값 + DEFAULT_CONFIG 기본값."""

    # ── Costs (BASELINE_SPEC: 고정) ──────────────────────────────
    BUY_COST: float = 0.00115        # 0.015% fee + 0.10% slippage
    SELL_COST: float = 0.00295       # 0.015% fee + 0.10% slippage + 0.18% tax

    # ── Capital (DEFAULT_CONFIG) ─────────────────────────────────
    INITIAL_CASH: int = 100_000_000  # 1억원
    CASH_BUFFER: float = 0.95        # 매수 시 95%까지만 사용

    # ── Universe filters (DEFAULT_CONFIG) ────────────────────────
    UNIV_MIN_CLOSE: int = 2000
    UNIV_MIN_AMOUNT: float = 2e9     # 20일 평균 거래대금
    UNIV_MIN_HISTORY: int = 260      # 최소 거래일 수

    # ── Scoring (BASELINE_SPEC: 고정) ────────────────────────────
    VOL_LOOKBACK: int = 252
    VOL_PERCENTILE: float = 0.30
    MOM_LOOKBACK: int = 252
    MOM_SKIP: int = 22
    N_STOCKS: int = 20               # 기본 포지션 수 (전략별 override)
    TRAIL_PCT: float = 0.12
    REBAL_DAYS: int = 21

    # ── Date range ───────────────────────────────────────────────
    START_DATE: str = "2026-03-01"
    END_DATE: str = ""               # 빈 문자열 = 최신 데이터까지
    LOOKBACK_DAYS: int = 252         # 지표 warmup용 선행 데이터

    # ── Run options ──────────────────────────────────────────────
    STRATEGIES: List[str] = field(default_factory=lambda: list(STRATEGY_CONFIGS.keys()))
    GROUP: str = ""                  # 빈 문자열 = 전체, "rebal"/"event"/"macro"/"regime"
    LAB_MODE: str = "portfolio"      # "portfolio" | "pure_signal"
    EXPERIMENTAL_SAME_DAY: bool = False  # True = event 전략 same_day_close

    # ── Heartbeat (P0-3) ─────────────────────────────────────────
    HEARTBEAT_WARN_SEC: int = 60
    HEARTBEAT_STALE_SEC: int = 180

    # ── Paths ────────────────────────────────────────────────────
    BASE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)

    @property
    def OHLCV_DIR(self) -> Path:
        return self.BASE_DIR.parent / "backtest" / "data_full" / "ohlcv"

    @property
    def INDEX_FILE(self) -> Path:
        return self.BASE_DIR.parent / "backtest" / "data_full" / "index" / "KOSPI.csv"

    @property
    def SECTOR_MAP_FILE(self) -> Path:
        # kr/data에 없으면 backtest/data_full에서 찾음
        local = self.BASE_DIR / "data" / "sector_map.json"
        if local.exists():
            return local
        return self.BASE_DIR.parent / "backtest" / "data_full" / "sector_map.json"

    @property
    def FUNDAMENTAL_DIR(self) -> Path:
        return self.BASE_DIR.parent / "backtest" / "data_full" / "fundamental"

    @property
    def OUTPUT_DIR(self) -> Path:
        return self.BASE_DIR / "report" / "output" / "lab"

    @property
    def LAB_RUNS_DIR(self) -> Path:
        return self.BASE_DIR / "data" / "lab_runs"

    def get_strategy_config(self, name: str) -> StrategyConfig:
        return STRATEGY_CONFIGS.get(name, StrategyConfig())

    def get_active_strategies(self) -> List[str]:
        """그룹 필터 적용 후 활성 전략 목록 반환."""
        if self.GROUP:
            group_strats = STRATEGY_GROUPS.get(self.GROUP, [])
            return [s for s in self.STRATEGIES if s in group_strats]
        return list(self.STRATEGIES)

    def get_active_groups(self) -> Dict[str, List[str]]:
        """활성 전략을 그룹별로 분류."""
        active = set(self.get_active_strategies())
        result = {}
        for group, strats in STRATEGY_GROUPS.items():
            filtered = [s for s in strats if s in active]
            if filtered:
                result[group] = filtered
        return result
