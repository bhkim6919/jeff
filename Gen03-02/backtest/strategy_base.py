# -*- coding: utf-8 -*-
"""
Strategy ABC + StrategySelector
================================
레짐별 전략 자동 선택 프레임워크.

Strategy 인터페이스:
  generate_signals(date, universe_ohlcv, index_df, regime) → List[signal_dict]
  exit_check(position, current_bar, regime) → Optional[exit_reason]

signal_dict 표준 포맷:
  {code, entry, tp, sl, sector, qscore, stage, strategy_name}
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class Signal:
    code: str
    entry: float
    tp: float
    sl: float
    sector: str
    qscore: float
    stage: str
    strategy_name: str
    extra: Dict[str, Any] = None

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}

    def to_dict(self) -> dict:
        d = {
            "code": self.code, "entry": int(self.entry),
            "tp": int(self.tp), "sl": int(self.sl),
            "sector": self.sector, "qscore": round(self.qscore, 4),
            "stage": self.stage, "strategy_name": self.strategy_name,
        }
        d.update(self.extra)
        return d


class Strategy(ABC):
    """전략 기본 인터페이스."""

    name: str = "base"

    @abstractmethod
    def generate_signals(
        self,
        eval_date: pd.Timestamp,
        universe: Dict[str, pd.DataFrame],
        index_df: pd.DataFrame,
        regime: str,
        sector_map: Dict[str, str],
    ) -> List[Signal]:
        """
        eval_date 기준 진입 시그널 생성.
        universe: {ticker: ohlcv_df (eval_date 이전 데이터만)}
        """
        ...

    def exit_check(
        self,
        position: dict,
        current_bar: dict,
        regime: str,
    ) -> Optional[str]:
        """
        포지션 청산 판단. 기본 SL/TP/MAX_HOLD 로직.
        반환: 청산 사유 문자열 또는 None(유지)
        """
        price = current_bar.get("close", 0)
        low = current_bar.get("low", price)
        high = current_bar.get("high", price)

        sl = position.get("sl", 0)
        tp = position.get("tp", 0)
        hold_days = position.get("hold_days", 0)
        max_hold = position.get("max_hold", 60)

        if sl > 0 and low <= sl:
            return "SL"
        if tp > 0 and high >= tp:
            return "TP"
        if hold_days >= max_hold:
            return "MAX_HOLD"
        return None


class StrategySelector:
    """레짐에 따라 전략 선택."""

    def __init__(self, strategies: Dict[str, Strategy]):
        """strategies: {"BULL": TrendStrategy(), "SIDEWAYS": MR(), "BEAR": Defense()}"""
        self._strategies = strategies

    def select(self, regime: str) -> Strategy:
        if regime in self._strategies:
            return self._strategies[regime]
        return self._strategies.get("BEAR", list(self._strategies.values())[0])

    def all_strategies(self) -> Dict[str, Strategy]:
        return dict(self._strategies)
