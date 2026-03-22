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

    # v7.4: 전일 종가 (GAP_DOWN 판정 기준 — 장중 current_price와 분리)
    prev_close: float = 0.0

    # v7.5: 진입 이후 최고가 (trailing stop 기준)
    high_watermark: float = 0.0

    # v7.1: 포지션 메타데이터 (재시작 시 stage 복원, 주문 추적)
    stage:             str = ""    # "A" (Early) | "B" (Main) — stage_manager에서 설정
    order_no:          str = ""    # 최초 진입 주문번호
    source_signal_date:str = ""    # 시그널 파일 날짜 (YYYYMMDD)

    # v7.4: 매도가능수량 분리 (T+2 결제, broker restriction 대응)
    qty_sellable:      int = -1    # -1 = 미조회 (기본값=quantity 사용), 0+ = 브로커 확인값
    qty_pending_sell:  int = 0     # 매도 주문 중인 수량

    # v7.6: 수량 신뢰도 및 제약 상태 (ghost/timeout/rejected sell 대응)
    qty_confidence:    str = "HIGH"     # HIGH | LOW | UNKNOWN
    restricted_reason: str = ""         # "" | "GHOST_FILL" | "TIMEOUT_UNCERTAIN" | "SELL_REJECTED" | "POSITION_MISMATCH" | "RISK_UNINITIALIZED"
    needs_reconcile:   bool = False     # True → 청산/추가매수 전 반드시 reconcile 필요

    # v7.8: 보조 수량 필드 (1차: 읽기 전용 추적, 기존 quantity 유지)
    requested_qty:        int = 0    # 최초 주문 요청 수량
    filled_buy_qty:       int = 0    # 누적 매수 체결 수량
    filled_sell_qty:      int = 0    # 누적 매도 체결 수량
    broker_confirmed_qty: int = -1   # 최근 broker 확인 수량 (-1=미조회)

    # v7.9: partial exit 추적
    pending_sell_order_no: str = ""  # 진행 중 매도 주문번호
    pending_sell_qty_orig: int = 0   # 원래 매도 요청 수량
    pending_sell_qty_filled: int = 0 # 현재까지 체결된 매도 수량
    pending_sell_remaining: int = 0  # 미체결 잔량

    @property
    def net_qty(self) -> int:
        """v7.8: 보조 수량 — filled_buy_qty - filled_sell_qty.
        quantity와 동일해야 함 (불일치 시 디버깅 단서)."""
        return self.filled_buy_qty - self.filled_sell_qty

    @property
    def effective_sellable(self) -> int:
        """실제 매도 가능 수량. qty_sellable 미조회 시 quantity 사용."""
        if self.qty_sellable < 0:
            return self.quantity
        return self.qty_sellable

    @property
    def is_restricted(self) -> bool:
        """자동 매매(청산/추가매수) 금지 상태인지 여부."""
        return self.qty_confidence != "HIGH" or self.needs_reconcile

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

    @property
    def has_pending_sell(self) -> bool:
        """v7.9: 진행 중인 매도 주문이 있는지 확인."""
        return self.pending_sell_remaining > 0

    def set_pending_sell(self, order_no: str, qty: int) -> None:
        """v7.9: partial exit 시작 시 호출."""
        self.pending_sell_order_no = order_no
        self.pending_sell_qty_orig = qty
        self.pending_sell_qty_filled = 0
        self.pending_sell_remaining = qty
        self.qty_pending_sell = qty

    def update_pending_sell(self, filled_qty: int) -> None:
        """v7.9: partial fill 반영."""
        self.pending_sell_qty_filled += filled_qty
        self.pending_sell_remaining = max(0, self.pending_sell_qty_orig - self.pending_sell_qty_filled)
        self.qty_pending_sell = self.pending_sell_remaining

    def clear_pending_sell(self) -> None:
        """v7.9: 매도 완료 or 취소 시 호출."""
        self.pending_sell_order_no = ""
        self.pending_sell_qty_orig = 0
        self.pending_sell_qty_filled = 0
        self.pending_sell_remaining = 0
        self.qty_pending_sell = 0

    def mark_restricted(self, reason: str, confidence: str = "LOW") -> None:
        """포지션을 제약 상태로 전환."""
        self.qty_confidence = confidence
        self.restricted_reason = reason
        self.needs_reconcile = True

    def mark_reconciled(self, sellable_qty: int = -1) -> None:
        """reconcile 완료 후 제약 해제."""
        self.qty_confidence = "HIGH"
        self.restricted_reason = ""
        self.needs_reconcile = False
        if sellable_qty >= 0:
            self.qty_sellable = sellable_qty
