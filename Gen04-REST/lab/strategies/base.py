"""
base.py — BaseStrategy ABC + Signal dataclass
================================================
모든 Lab 전략의 기본 인터페이스.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any

from lab.snapshot import DailySnapshot
from lab.exit_policy import ExitPolicy
from lab.lab_config import StrategyConfig


@dataclass
class Signal:
    """전략 신호."""
    ticker: str
    direction: str          # "BUY" or "SELL"
    reason: str             # e.g. "MOMENTUM_TOP", "RSI_OVERSOLD"
    priority: float = 0.0   # 높을수록 먼저 체결
    metadata: dict = field(default_factory=dict)


class BaseStrategy(ABC):
    """모든 Lab 전략의 기본 클래스."""

    def __init__(self, name: str, config: StrategyConfig,
                 exit_policy: ExitPolicy):
        self.name = name
        self.config = config
        self.exit_policy = exit_policy
        # 전략 내부 상태 (전략별 독립, 공유 금지)
        self._state: Dict[str, Any] = {}
        self._last_rebal_idx: int = -999

    @abstractmethod
    def generate_signals(
        self,
        snapshot: DailySnapshot,
        positions: Dict[str, dict],
    ) -> List[Signal]:
        """당일 snapshot과 현재 포지션으로 BUY/SELL 신호 생성.

        Args:
            snapshot: 당일 DailySnapshot (frozen, 수정 금지)
            positions: deepcopy된 현재 포지션 {ticker: {qty, entry_price, ...}}

        Returns:
            Signal 리스트. SELL 먼저, BUY 나중에 처리됨.
        """
        ...

    def on_fill(self, ticker: str, fill_price: float, qty: int, day_idx: int):
        """매수 체결 후 콜백. 내부 상태 업데이트용."""
        pass

    def on_exit(self, ticker: str, exit_price: float, reason: str, day_idx: int):
        """포지션 종료 후 콜백. 내부 상태 업데이트용."""
        pass

    def _should_rebalance(self, day_idx: int) -> bool:
        """리밸런싱 시점 판단."""
        if self.config.rebal_days is None:
            return False
        return (day_idx - self._last_rebal_idx) >= self.config.rebal_days

    def _mark_rebalanced(self, day_idx: int):
        self._last_rebal_idx = day_idx
