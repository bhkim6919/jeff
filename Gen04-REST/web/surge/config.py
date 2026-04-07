# -*- coding: utf-8 -*-
"""
config.py -- Surge Trader Simulator Configuration
===================================================
시간대별 슬리피지, 체결 안전계수, 리스크 가드 파라미터 등.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Tuple


# ── 시간대별 슬리피지 테이블 ──────────────────────────────────
# (start_hhmm, end_hhmm, slippage_rate)
DEFAULT_SLIPPAGE_SCHEDULE: List[Tuple[str, str, float]] = [
    ("09:00", "09:10", 0.005),   # 장 초반 과열
    ("09:10", "10:00", 0.003),
    ("10:00", "14:30", 0.0015),  # 정상 구간
    ("14:30", "15:20", 0.0025),  # 장 마감 접근
]

DEFAULT_BLOCKED_PERIODS: List[Tuple[str, str]] = [
    # ("09:00", "09:05"),  # 기본값: 비활성 (사용자 선택)
]


@dataclass
class SurgeConfig:
    """Surge simulator parameters — all values are overridable via API."""

    # ── TP / SL ────────────────────────────────────────────
    tp_pct: float = 1.5           # Take-profit target (%)
    sl_pct: float = 1.0           # Stop-loss limit (%)
    max_hold_sec: int = 600       # 최대 보유 시간 (초)
    cooldown_sec: int = 120       # 동일 종목 재진입 쿨다운 (초)

    # ── 슬리피지 / 비용 ───────────────────────────────────
    slippage_schedule: List[Tuple[str, str, float]] = field(
        default_factory=lambda: list(DEFAULT_SLIPPAGE_SCHEDULE)
    )
    fee_rate: float = 0.00015     # 0.015% per side
    tax_rate: float = 0.0018      # 0.18% sell only

    # ── 체결 현실성 ───────────────────────────────────────
    fill_safety_k: float = 2.0    # ask_size >= qty * K
    deterministic_fill: bool = True  # REPLAY 모드 필수: 확률 없이 결정론적

    # ── 리스크 가드 ───────────────────────────────────────
    max_daily_entries: int = 50
    max_concurrent: int = 5
    max_loss_per_stock: int = 3
    consecutive_loss_halt: int = 5
    blocked_periods: List[Tuple[str, str]] = field(
        default_factory=lambda: list(DEFAULT_BLOCKED_PERIODS)
    )

    # ── Stale Guard ───────────────────────────────────────
    max_tr_lag_sec: float = 15.0     # TR 수신 → 진입 판단 최대 허용 지연
    max_hoga_stale_sec: float = 5.0  # 호가 데이터 최대 허용 나이

    # ── 필터 ──────────────────────────────────────────────
    min_price: int = 5000
    min_volume_krw: int = 1_000_000_000  # 10억
    exclude_etf: bool = True
    min_change_pct: float = 3.0   # 최소 등락률 (%) for ranking filter

    # ── 자본 ──────────────────────────────────────────────
    initial_cash: int = 10_000_000   # 1천만원
    per_trade_pct: float = 20.0      # 1회 매매 자본 비율 (%)

    # ── 스캐너 ────────────────────────────────────────────
    scan_interval_sec: int = 30      # 스캔 주기 (초)
    ranking_source: str = "등락률"   # 실시간순위 | 등락률 | 거래량 | 거래대금
    ranking_top_n: int = 20

    # ── 모드 ──────────────────────────────────────────────
    mode: str = "LIVE_SIM"           # LIVE_SIM | REPLAY_SIM

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # tuple lists → serializable
        d["slippage_schedule"] = [list(t) for t in self.slippage_schedule]
        d["blocked_periods"] = [list(t) for t in self.blocked_periods]
        return d


# ── Default instance ─────────────────────────────────────────
DEFAULT_SURGE_CONFIG = SurgeConfig()


# ── GUI Parameter Ranges ─────────────────────────────────────
SURGE_PARAM_RANGES: Dict[str, Dict[str, Any]] = {
    "tp_pct":              {"type": "range", "min": 0.5, "max": 5.0, "step": 0.1, "unit": "%"},
    "sl_pct":              {"type": "range", "min": 0.3, "max": 3.0, "step": 0.1, "unit": "%"},
    "max_hold_sec":        {"type": "range", "min": 60,  "max": 3600, "step": 60, "unit": "초"},
    "cooldown_sec":        {"type": "range", "min": 30,  "max": 600, "step": 30, "unit": "초"},
    "fill_safety_k":       {"type": "range", "min": 1.0, "max": 5.0, "step": 0.5},
    "max_daily_entries":   {"type": "range", "min": 10,  "max": 200, "step": 10},
    "max_concurrent":      {"type": "range", "min": 1,   "max": 20,  "step": 1},
    "max_loss_per_stock":  {"type": "range", "min": 1,   "max": 10,  "step": 1},
    "consecutive_loss_halt": {"type": "range", "min": 3, "max": 20, "step": 1},
    "max_tr_lag_sec":      {"type": "range", "min": 5.0, "max": 60.0, "step": 5.0, "unit": "초"},
    "min_price":           {"type": "range", "min": 1000, "max": 50000, "step": 1000, "unit": "원"},
    "min_change_pct":      {"type": "range", "min": 1.0, "max": 10.0, "step": 0.5, "unit": "%"},
    "initial_cash":        {"type": "range", "min": 1_000_000, "max": 100_000_000, "step": 1_000_000, "unit": "원"},
    "per_trade_pct":       {"type": "range", "min": 5.0, "max": 50.0, "step": 5.0, "unit": "%"},
    "scan_interval_sec":   {"type": "range", "min": 10,  "max": 120, "step": 10, "unit": "초"},
    "ranking_source":      {"type": "select", "options": ["등락률", "거래량", "거래대금"]},
    "ranking_top_n":       {"type": "range", "min": 5, "max": 50, "step": 5},
}


def config_from_dict(d: Dict[str, Any]) -> SurgeConfig:
    """Merge user overrides with defaults. Unknown keys are ignored."""
    base = asdict(DEFAULT_SURGE_CONFIG)
    for k, v in d.items():
        if k in base:
            # Convert lists of lists back to lists of tuples
            if k == "slippage_schedule" and isinstance(v, list):
                v = [tuple(x) for x in v]
            elif k == "blocked_periods" and isinstance(v, list):
                v = [tuple(x) for x in v]
            base[k] = v
    return SurgeConfig(**base)
