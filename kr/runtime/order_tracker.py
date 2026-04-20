"""
order_tracker.py — Order registry + fill idempotency + JSONL journal
=====================================================================
Adapted from Gen3 core/order_tracker.py (186L).

Key features:
  - OrderRecord: tracks order lifecycle (NEW → SUBMITTED → FILLED/REJECTED)
  - FillEvent + Fill Ledger: prevents double-fill from chejan retransmissions
  - JSONL journal: persistent log of all order events for crash forensics
"""
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger("gen4.tracker")


class OrderStatus(Enum):
    NEW               = "NEW"
    SUBMITTED         = "SUBMITTED"
    PARTIAL_FILLED    = "PARTIAL_FILLED"
    FILLED            = "FILLED"
    TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"
    PENDING_EXTERNAL  = "PENDING_EXTERNAL"
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
    base_qty:      int = 0       # BUY 직전 보유 수량 (reconcile delta 계산용)

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_done(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.REJECTED,
                               OrderStatus.CANCELLED)


class OrderTracker:
    """Daily order registry + fill idempotency + JSONL journal."""

    def __init__(self, journal_dir: Optional[Path] = None,
                 trading_mode: str = "mock"):
        self._orders: Dict[str, OrderRecord] = {}
        self._seq: int = 0
        self._fill_ledger: Dict[str, FillEvent] = {}
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._trading_mode = trading_mode
        # Ops evidence counters (session-scope).
        # - duplicate_fills: record_fill이 이미 ledger에 있는 fill_id를 재수신한 횟수.
        # - timeouts: TIMEOUT_UNCERTAIN + PENDING_EXTERNAL 누적 카운트 (중복 없이 터미널 transition만).
        # Total count와 recent/unresolved를 분리. _unresolved_* 는 현재 open 상태 기준 snapshot에서 재계산.
        self._duplicate_fills_count: int = 0
        self._order_timeouts_count: int = 0

        # JSONL journal for crash recovery forensics
        self._journal_path: Optional[Path] = None
        if journal_dir:
            journal_dir = Path(journal_dir)
            journal_dir.mkdir(parents=True, exist_ok=True)
            self._journal_path = journal_dir / f"order_journal_{self._session_id}.jsonl"

    def _journal_write(self, event: str, **kwargs) -> None:
        """Append event to JSONL journal (best-effort, never raises)."""
        if not self._journal_path:
            return
        try:
            entry = {
                "ts": datetime.now().isoformat(),
                "session_id": self._session_id,
                "trading_mode": self._trading_mode,
                "event": event,
                **kwargs,
            }
            with open(self._journal_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            logging.getLogger("gen4.tracker").warning(
                f"[JOURNAL_WRITE_FAIL] event={event}: {e}")  # never break trading

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
        self._journal_write("SUBMIT_ATTEMPT", order_id=oid, code=code,
                            side=side, requested_qty=quantity, reason=reason)
        return rec

    def mark_submitted(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.submitted_at = datetime.now()
            self._transition(rec, OrderStatus.SUBMITTED)
            self._journal_write("SUBMITTED", order_id=order_id,
                                code=rec.code, side=rec.side)

    def mark_filled(self, order_id: str, exec_price: float, exec_qty: int) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.exec_price = exec_price
            rec.exec_qty = exec_qty
            rec.filled_at = datetime.now()
            self._transition(rec, OrderStatus.FILLED,
                             f"qty={exec_qty} price={exec_price:,.0f}")
            self._journal_write("FILLED", order_id=order_id,
                                code=rec.code, side=rec.side,
                                exec_qty=exec_qty, exec_price=exec_price)

    def mark_rejected(self, order_id: str, reason: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            rec.reject_reason = reason
            self._transition(rec, OrderStatus.REJECTED, reason)
            self._journal_write("REJECTED", order_id=order_id,
                                code=rec.code, side=rec.side, reason=reason)

    def mark_timeout(self, order_id: str) -> None:
        rec = self._orders.get(order_id)
        if rec:
            if rec.status != OrderStatus.TIMEOUT_UNCERTAIN:
                self._order_timeouts_count += 1
            self._transition(rec, OrderStatus.TIMEOUT_UNCERTAIN)
            self._journal_write("TIMEOUT_UNCERTAIN", order_id=order_id,
                                code=rec.code, side=rec.side,
                                requested_qty=rec.quantity)

    def mark_pending_external(self, order_id: str) -> None:
        """Timeout with uncertain fill — order may be live on broker."""
        rec = self._orders.get(order_id)
        if rec:
            if rec.status not in (OrderStatus.TIMEOUT_UNCERTAIN,
                                  OrderStatus.PENDING_EXTERNAL):
                self._order_timeouts_count += 1
            self._transition(rec, OrderStatus.PENDING_EXTERNAL)
            self._journal_write("PENDING_EXTERNAL", order_id=order_id,
                                code=rec.code, side=rec.side,
                                requested_qty=rec.quantity)

    def mark_ghost_settled(self, order_id: str, cum_filled: int,
                           avg_price: float) -> None:
        """Ghost fill resolved — upgrade to FILLED only if cum >= requested."""
        rec = self._orders.get(order_id)
        if not rec:
            return
        if cum_filled >= rec.quantity:
            rec.exec_price = avg_price
            rec.exec_qty = cum_filled
            rec.filled_at = datetime.now()
            self._transition(rec, OrderStatus.FILLED,
                             f"(ghost settled) qty={cum_filled} price={avg_price:,.0f}")
            self._journal_write("GHOST_SETTLED", order_id=order_id,
                                code=rec.code, side=rec.side,
                                exec_qty=cum_filled, exec_price=avg_price)
        else:
            rec.exec_qty = cum_filled
            rec.exec_price = avg_price
            logger.info(f"[GHOST_PARTIAL] {order_id} cum={cum_filled}/{rec.quantity} "
                        f"— still PENDING_EXTERNAL")

    def mark_reconcile_settled(self, order_id: str, final_qty: int,
                               avg_price: float,
                               terminal: str = "FILLED") -> None:
        """EOD reconcile — broker snapshot based final settlement."""
        rec = self._orders.get(order_id)
        if not rec:
            return
        rec.exec_qty = final_qty
        rec.exec_price = avg_price
        rec.filled_at = datetime.now()
        if terminal == "FILLED":
            self._transition(rec, OrderStatus.FILLED,
                             f"(reconcile) qty={final_qty}")
        elif terminal == "CANCELLED":
            self._transition(rec, OrderStatus.CANCELLED,
                             f"(reconcile) unfilled")
        self._journal_write("RECONCILE_SETTLED", order_id=order_id,
                            code=rec.code, side=rec.side,
                            exec_qty=final_qty, terminal=terminal)

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
            self._duplicate_fills_count += 1
            logger.debug(f"Duplicate fill ignored: {fill_id}")
            return False
        self._fill_ledger[fill_id] = FillEvent(
            fill_id=fill_id, order_no=order_no, side=side, code=code,
            exec_qty=exec_qty, exec_price=exec_price,
            cumulative_qty=cumulative_qty,
            timestamp=datetime.now(), source=source,
        )
        self._journal_write(
            "GHOST_FILLED" if source == "GHOST" else "PARTIAL_FILLED",
            order_no=order_no, code=code, side=side,
            exec_qty=exec_qty, exec_price=exec_price,
            cumulative_qty=cumulative_qty, source=source)
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
        pending_ext = sum(1 for r in self._orders.values()
                          if r.status == OrderStatus.PENDING_EXTERNAL)
        return {
            "total": total, "filled": filled, "rejected": rejected,
            "uncertain": uncertain, "pending_external": pending_ext,
            "fills_in_ledger": len(self._fill_ledger),
        }

    def ops_snapshot(self) -> dict:
        """Structured ops evidence for promotion collector.

        All counts are session-scope (tracker lifetime). Consumer can combine
        with persisted history for longer windows.

        Total count vs unresolved/recent is explicitly separated:
          - *_total: cumulative since session start
          - pending_external_unresolved: currently open (not settled)
        """
        uncertain_open = sum(1 for r in self._orders.values()
                             if r.status == OrderStatus.TIMEOUT_UNCERTAIN)
        pending_open = sum(1 for r in self._orders.values()
                           if r.status == OrderStatus.PENDING_EXTERNAL)
        return {
            "duplicate_execution_incident_count_total": self._duplicate_fills_count,
            "order_timeout_events_total": self._order_timeouts_count,
            "pending_external_unresolved_count": pending_open,
            "timeout_uncertain_unresolved_count": uncertain_open,
            "session_id": self._session_id,
            "trading_mode": self._trading_mode,
        }
