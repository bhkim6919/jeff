"""
order_tracker.py — Order registry + fill idempotency
=====================================================
Adapted from Gen3 core/order_tracker.py (186L).

Key features:
  - OrderRecord: tracks order lifecycle (NEW → SUBMITTED → FILLED/REJECTED)
  - FillEvent + Fill Ledger: prevents double-fill from chejan retransmissions
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger("gen4.tracker")


class OrderStatus(Enum):
    NEW               = "NEW"
    SUBMITTED         = "SUBMITTED"
    PARTIAL_FILLED    = "PARTIAL_FILLED"
    FILLED            = "FILLED"
    TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"
    CANCELLED         = "CANCELLED"
    REJECTED          = "REJECTED"


@dataclass
class FillEvent:
    """Individual fill event — for idempotency guard."""
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
    order_id:      str
    code:          str
    side:          str           # "BUY" | "SELL"
    quantity:      int
    price:         float
    reason:        str = ""      # "REBALANCE_ENTRY" | "REBALANCE_EXIT" | "TRAIL_STOP"
    status:        OrderStatus = OrderStatus.NEW
    exec_price:    float = 0.0
    exec_qty:      int = 0
    submitted_at:  Optional[datetime] = None
    filled_at:     Optional[datetime] = None
    reject_reason: str = ""

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_done(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.REJECTED,
                               OrderStatus.CANCELLED, OrderStatus.TIMEOUT_UNCERTAIN)


class OrderTracker:
    """Daily order registry + fill idempotency."""

    def __init__(self):
        self._orders: Dict[str, OrderRecord] = {}
        self._seq: int = 0
        self._fill_ledger: Dict[str, FillEvent] = {}

    def new_order_id(self) -> str:
        self._seq += 1
        ts = datetime.now().strftime("%H%M%S")
        return f"ORD_{ts}_{self._seq:04d}"

    def _transition(self, rec: OrderRecord, new_status: OrderStatus,
                    detail: str = "") -> None:
        old = rec.status.value
        rec.status = new_status
        msg = f"{old} -> {new_status.value} {rec.code}"
        if detail:
            msg += f" {detail}"
        logger.info(f"[ORDER] {msg}")

    def register(self, code: str, side: str, quantity: int,
                 price: float, reason: str = "") -> OrderRecord:
        oid = self.new_order_id()
        rec = OrderRecord(
            order_id=oid, code=code, side=side,
            quantity=quantity, price=price, reason=reason,
            status=OrderStatus.NEW,
        )
        self._orders[oid] = rec
        logger.info(f"[ORDER] NEW {side} {code} qty={quantity} "
                     f"price={price:,.0f} reason={reason}")
        return rec

    def mark_submitted(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.submitted_at = datetime.now()
            self._transition(rec, OrderStatus.SUBMITTED)

    def mark_filled(self, order_id: str, exec_price: float, exec_qty: int) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.exec_price = exec_price
            rec.exec_qty = exec_qty
            rec.filled_at = datetime.now()
            self._transition(rec, OrderStatus.FILLED,
                             f"qty={exec_qty} price={exec_price:,.0f}")

    def mark_rejected(self, order_id: str, reason: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.reject_reason = reason
            self._transition(rec, OrderStatus.REJECTED, reason)

    def mark_timeout(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            self._transition(rec, OrderStatus.TIMEOUT_UNCERTAIN)

    def filled_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if r.is_filled]

    def pending_today(self) -> List[OrderRecord]:
        return [r for r in self._orders.values() if not r.is_done]

    # ── Fill Ledger (idempotency) ────────────────────────────────

    def record_fill(self, order_no: str, side: str, code: str,
                    exec_qty: int, exec_price: float,
                    cumulative_qty: int, source: str = "CHEJAN") -> bool:
        """Record fill event. Returns False if already recorded (duplicate)."""
        fill_id = f"{order_no}_{side}_{cumulative_qty}"
        if fill_id in self._fill_ledger:
            logger.debug(f"Duplicate fill ignored: {fill_id}")
            return False
        self._fill_ledger[fill_id] = FillEvent(
            fill_id=fill_id, order_no=order_no, side=side, code=code,
            exec_qty=exec_qty, exec_price=exec_price,
            cumulative_qty=cumulative_qty,
            timestamp=datetime.now(), source=source,
        )
        return True

    def is_fill_recorded(self, order_no: str, side: str, cumulative_qty: int) -> bool:
        fill_id = f"{order_no}_{side}_{cumulative_qty}"
        return fill_id in self._fill_ledger

    def summary(self) -> dict:
        total = len(self._orders)
        filled = sum(1 for r in self._orders.values() if r.is_filled)
        rejected = sum(1 for r in self._orders.values()
                       if r.status == OrderStatus.REJECTED)
        uncertain = sum(1 for r in self._orders.values()
                        if r.status == OrderStatus.TIMEOUT_UNCERTAIN)
        return {
            "total": total, "filled": filled, "rejected": rejected,
            "uncertain": uncertain, "fills_in_ledger": len(self._fill_ledger),
        }
