"""
exit_policy.py — ExitPolicy ABC + 구현체들
============================================
전략별 종료 조건을 엔진에서 분리. trail_pct if/else 제거.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice


class ExitPolicy(ABC):
    """전략별 종료 정책 인터페이스."""

    @abstractmethod
    def check_exit(
        self,
        snapshot: DailySnapshot,
        position: Dict[str, Any],
        strategy_state: dict,
    ) -> Optional[str]:
        """종료 조건 확인.

        Args:
            snapshot: 당일 스냅샷 (frozen)
            position: {qty, entry_price, entry_idx, high_wm, buy_cost_total, ticker}
            strategy_state: 전략 내부 상태 (read-only 권장)

        Returns:
            exit_reason 문자열 or None (HOLD)
        """
        ...


class TrailStopExit(ExitPolicy):
    """Trailing stop exit (close-based)."""

    def __init__(self, trail_pct: float = 0.12):
        self.trail_pct = trail_pct

    def check_exit(self, snapshot, position, strategy_state) -> Optional[str]:
        tk = position["ticker"]
        p = float(snapshot.close.get(tk, 0))
        if p <= 0 or pd.isna(p):
            return None

        # Update HWM
        hwm = max(position.get("high_wm", p), p)
        position["high_wm"] = hwm

        # Check drawdown
        if hwm > 0:
            dd = (p - hwm) / hwm
            if dd <= -self.trail_pct:
                return "TRAIL"
        return None


class MeanReversionExit(ExitPolicy):
    """RSI > 50 / hold >= 5일 / loss > -5%."""

    def __init__(self, rsi_exit: float = 50.0, max_hold: int = 5,
                 max_loss: float = -0.05):
        self.rsi_exit = rsi_exit
        self.max_hold = max_hold
        self.max_loss = max_loss

    def check_exit(self, snapshot, position, strategy_state) -> Optional[str]:
        tk = position["ticker"]
        p = float(snapshot.close.get(tk, 0))
        if p <= 0 or pd.isna(p):
            return None

        # Hold days
        hold_days = snapshot.day_idx - position["entry_idx"]
        if hold_days >= self.max_hold:
            return "TIME_EXIT"

        # Loss check
        entry = position["entry_price"]
        if entry > 0:
            pnl = (p - entry) / entry
            if pnl <= self.max_loss:
                return "STOP_LOSS"

        # RSI check
        rsi = strategy_state.get(f"rsi_{tk}")
        if rsi is not None and rsi > self.rsi_exit:
            return "RSI_EXIT"

        return None


class BreakoutExit(ExitPolicy):
    """Trail stop -8% from breakout high."""

    def __init__(self, trail_pct: float = 0.08):
        self.trail_pct = trail_pct

    def check_exit(self, snapshot, position, strategy_state) -> Optional[str]:
        tk = position["ticker"]
        p = float(snapshot.close.get(tk, 0))
        if p <= 0 or pd.isna(p):
            return None

        hwm = max(position.get("high_wm", p), p)
        position["high_wm"] = hwm

        if hwm > 0:
            dd = (p - hwm) / hwm
            if dd <= -self.trail_pct:
                return "TRAIL_BREAKOUT"
        return None


class LiquidityExit(ExitPolicy):
    """거래량 급감 (vol/avg5 < 0.5) + trail -10%."""

    def __init__(self, vol_decay_thresh: float = 0.5, trail_pct: float = 0.10):
        self.vol_decay_thresh = vol_decay_thresh
        self.trail_pct = trail_pct

    def check_exit(self, snapshot, position, strategy_state) -> Optional[str]:
        tk = position["ticker"]
        p = float(snapshot.close.get(tk, 0))
        if p <= 0 or pd.isna(p):
            return None

        # Trail stop
        hwm = max(position.get("high_wm", p), p)
        position["high_wm"] = hwm
        if hwm > 0 and (p - hwm) / hwm <= -self.trail_pct:
            return "TRAIL_LIQUIDITY"

        # Volume decay
        if snapshot.day_idx >= 5:
            vol_today = float(snapshot.volume.get(tk, 0))
            vol_5d = snapshot.volume_matrix[tk].iloc[-5:].mean()
            if vol_5d > 0 and vol_today / vol_5d < self.vol_decay_thresh:
                return "VOLUME_DECAY"

        return None


class SectorRotationExit(ExitPolicy):
    """Trail -12% + 섹터 탈락."""

    def __init__(self, trail_pct: float = 0.12):
        self.trail_pct = trail_pct

    def check_exit(self, snapshot, position, strategy_state) -> Optional[str]:
        tk = position["ticker"]
        p = float(snapshot.close.get(tk, 0))
        if p <= 0 or pd.isna(p):
            return None

        # Trail stop
        hwm = max(position.get("high_wm", p), p)
        position["high_wm"] = hwm
        if hwm > 0 and (p - hwm) / hwm <= -self.trail_pct:
            return "TRAIL"

        # Sector fallout check
        top_sectors = strategy_state.get("top_sectors", set())
        if top_sectors:
            stock_sector = snapshot.sector_map.get(tk, {}).get("sector", "")
            if stock_sector and stock_sector not in top_sectors:
                return "SECTOR_FALLOUT"

        return None
