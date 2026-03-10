"""
OrderTracker
============
주문 상태 추적. 체결 확인 및 미체결 주문 관리.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum


class OrderStatus(Enum):
    PENDING   = "PENDING"    # 주문 접수 전
    SUBMITTED = "SUBMITTED"  # 주문 전송됨
    FILLED    = "FILLED"     # 체결 완료
    REJECTED  = "REJECTED"   # 거부됨
    CANCELLED = "CANCELLED"  # 취소됨


@dataclass
class OrderRecord:
    order_id:    str
    code:        str
    sector:      str
    side:        str           # "BUY" | "SELL"
    quantity:    int
    price:       float
    status:      OrderStatus = OrderStatus.PENDING
    exec_price:  float        = 0.0
    exec_qty:    int          = 0
    submitted_at: Optional[datetime] = None
    filled_at:   Optional[datetime]  = None
    reject_reason: str = ""

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_done(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED)


class OrderTracker:
    """주문 레지스트리 — 당일 주문 전체 이력 관리."""

    def __init__(self):
        self._orders: Dict[str, OrderRecord] = {}
        self._seq: int = 0

    def new_order_id(self) -> str:
        self._seq += 1
        ts = datetime.now().strftime("%H%M%S")
        return f"ORD_{ts}_{self._seq:04d}"

    def register(self, code: str, sector: str, side: str,
                 quantity: int, price: float) -> OrderRecord:
        oid = self.new_order_id()
        rec = OrderRecord(
            order_id=oid, code=code, sector=sector,
            side=side, quantity=quantity, price=price,
            status=OrderStatus.PENDING,
        )
        self._orders[oid] = rec
        return rec

    def mark_submitted(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.status       = OrderStatus.SUBMITTED
            rec.submitted_at = datetime.now()

    def mark_filled(self, order_id: str, exec_price: float, exec_qty: int) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.status     = OrderStatus.FILLED
            rec.exec_price = exec_price
            rec.exec_qty   = exec_qty
            rec.filled_at  = datetime.now()

    def mark_rejected(self, order_id: str, reason: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.status        = OrderStatus.REJECTED
            rec.reject_reason = reason

    def filled_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if r.is_filled]

    def pending_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if not r.is_done]

    def summary(self) -> dict:
        total    = len(self._orders)
        filled   = sum(1 for r in self._orders.values() if r.is_filled)
        rejected = sum(1 for r in self._orders.values() if r.status == OrderStatus.REJECTED)
        return {"total": total, "filled": filled, "rejected": rejected,
                "pending": total - filled - rejected}
