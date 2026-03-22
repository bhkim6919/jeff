"""
OrderTracker
============
주문 상태 추적. 체결 확인 및 미체결 주문 관리.

v7.8: Fill Ledger — 체결 이벤트 idempotency guard.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum


class OrderStatus(Enum):
    NEW               = "NEW"                # 주문 생성 (아직 미전송)
    SUBMITTED         = "SUBMITTED"          # 주문 전송됨
    PARTIAL_FILLED    = "PARTIAL_FILLED"     # 부분 체결
    FILLED            = "FILLED"             # 체결 완료
    TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 타임아웃 (체결 불확실)
    CANCELLED         = "CANCELLED"          # 취소됨
    REJECTED          = "REJECTED"           # 거부됨


@dataclass
class FillEvent:
    """v7.8: 개별 체결 이벤트 기록 — 중복 반영 방지용."""
    fill_id:        str       # f"{order_no}_{side}_{cumulative_qty}"
    order_no:       str
    side:           str       # "BUY" | "SELL"
    code:           str
    exec_qty:       int
    exec_price:     float
    cumulative_qty: int
    timestamp:      datetime
    source:         str       # "CHEJAN" | "GHOST" | "RECONCILE"


@dataclass
class OrderRecord:
    order_id:    str
    code:        str
    sector:      str
    side:        str           # "BUY" | "SELL"
    quantity:    int
    price:       float
    status:      OrderStatus = OrderStatus.NEW
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
        return self.status in (OrderStatus.FILLED, OrderStatus.REJECTED,
                               OrderStatus.CANCELLED, OrderStatus.TIMEOUT_UNCERTAIN)


class OrderTracker:
    """주문 레지스트리 — 당일 주문 전체 이력 관리."""

    def __init__(self):
        self._orders: Dict[str, OrderRecord] = {}
        self._seq: int = 0
        # v7.8: Fill Ledger — 체결 이벤트 중복 반영 방지
        self._fill_ledger: Dict[str, FillEvent] = {}  # fill_id → FillEvent

    def new_order_id(self) -> str:
        self._seq += 1
        ts = datetime.now().strftime("%H%M%S")
        return f"ORD_{ts}_{self._seq:04d}"

    def _transition(self, rec: OrderRecord, new_status: OrderStatus,
                    detail: str = "") -> None:
        """상태 전이 + 표준 로그."""
        old = rec.status.value
        rec.status = new_status
        tag = f"[ORD] {old} → {new_status.value} {rec.code}"
        if detail:
            tag += f" {detail}"
        print(tag)

    def register(self, code: str, sector: str, side: str,
                 quantity: int, price: float) -> OrderRecord:
        oid = self.new_order_id()
        rec = OrderRecord(
            order_id=oid, code=code, sector=sector,
            side=side, quantity=quantity, price=price,
            status=OrderStatus.NEW,
        )
        self._orders[oid] = rec
        print(f"[ORD] NEW {side} {code} qty={quantity} price={price:,.0f}")
        return rec

    def mark_submitted(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.submitted_at = datetime.now()
            self._transition(rec, OrderStatus.SUBMITTED)

    def mark_partial_filled(self, order_id: str, exec_price: float,
                            exec_qty: int) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.exec_price = exec_price
            rec.exec_qty   += exec_qty
            self._transition(rec, OrderStatus.PARTIAL_FILLED,
                             f"qty={exec_qty} price={exec_price:,.0f}")

    def mark_filled(self, order_id: str, exec_price: float, exec_qty: int) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.exec_price = exec_price
            rec.exec_qty   = exec_qty
            rec.filled_at  = datetime.now()
            self._transition(rec, OrderStatus.FILLED,
                             f"qty={exec_qty} price={exec_price:,.0f}")

    def mark_timeout_uncertain(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            self._transition(rec, OrderStatus.TIMEOUT_UNCERTAIN)

    def mark_rejected(self, order_id: str, reason: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.reject_reason = reason
            self._transition(rec, OrderStatus.REJECTED, reason)

    def mark_cancelled(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            self._transition(rec, OrderStatus.CANCELLED)

    def filled_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if r.is_filled]

    def pending_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if not r.is_done]

    def uncertain_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values()
                if r.status == OrderStatus.TIMEOUT_UNCERTAIN]

    # ── v7.8: Fill Ledger ────────────────────────────────────────────────────

    def record_fill(self, order_no: str, side: str, code: str,
                    exec_qty: int, exec_price: float,
                    cumulative_qty: int, source: str = "CHEJAN") -> bool:
        """체결 이벤트 기록. 이미 기록된 fill이면 False 반환 (중복 반영 방지)."""
        fill_id = f"{order_no}_{side}_{cumulative_qty}"
        if fill_id in self._fill_ledger:
            return False
        self._fill_ledger[fill_id] = FillEvent(
            fill_id=fill_id, order_no=order_no, side=side, code=code,
            exec_qty=exec_qty, exec_price=exec_price,
            cumulative_qty=cumulative_qty,
            timestamp=datetime.now(), source=source,
        )
        return True

    def is_fill_recorded(self, order_no: str, side: str, cumulative_qty: int) -> bool:
        """해당 체결 이벤트가 이미 기록됐는지 확인."""
        fill_id = f"{order_no}_{side}_{cumulative_qty}"
        return fill_id in self._fill_ledger

    def fill_ledger_summary(self) -> dict:
        return {"total_fills": len(self._fill_ledger),
                "sources": {s: sum(1 for f in self._fill_ledger.values() if f.source == s)
                            for s in ("CHEJAN", "GHOST", "RECONCILE")}}

    # ── 요약 ─────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        total    = len(self._orders)
        filled   = sum(1 for r in self._orders.values() if r.is_filled)
        rejected = sum(1 for r in self._orders.values() if r.status == OrderStatus.REJECTED)
        uncertain = sum(1 for r in self._orders.values() if r.status == OrderStatus.TIMEOUT_UNCERTAIN)
        return {"total": total, "filled": filled, "rejected": rejected,
                "uncertain": uncertain,
                "pending": total - filled - rejected - uncertain}
