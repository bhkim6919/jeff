"""
PositionTracker
===============
개별 포지션 데이터 클래스.
Gen3에서 entry_date 필드를 포함하여 MAX_HOLD_DAYS 청산에 활용한다.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class Position:
    code:          str
    sector:        str
    quantity:      int
    avg_price:     float
    current_price: float
    entry_date:    date = field(default_factory=date.today)

    # TP/SL 계획값 (RiskManager → register_plan() 으로 주입)
    tp:       float = 0.0
    sl:       float = 0.0
    q_score:  float = 0.0
    rr_ratio: float = 0.0

    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price

    @property
    def unrealized_pnl(self) -> float:
        return (self.current_price - self.avg_price) * self.quantity

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.avg_price == 0:
            return 0.0
        return (self.current_price - self.avg_price) / self.avg_price

    @property
    def held_days(self) -> int:
        """오늘까지 보유 일수."""
        return (date.today() - self.entry_date).days
