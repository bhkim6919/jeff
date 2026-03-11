"""
OrderExecutor
=============
개별 주문 실행 엔진.
paper_trading=True → 시뮬레이션 / False → Kiwoom 실거래 (안정화 후 구현)

6중 게이트(PortfolioManager.can_enter)를 통과한 주문만 체결 처리.
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from data.name_lookup import get_name

_log = logging.getLogger("OrderExecutor")


MIN_DAILY_VOLUME = 2_000_000_000  # 일 거래대금 20억 미만 → 진입 금지


@dataclass
class Order:
    code:     str
    sector:   str
    side:     str      # "BUY" | "SELL"
    quantity: int
    price:    float


@dataclass
class TradeResult:
    code:         str
    side:         str
    quantity:     int
    exec_price:   float
    slippage_pct: float
    timestamp:    datetime
    rejected:     bool = False
    reject_reason: str = ""

    def __str__(self):
        label = f"{get_name(self.code)}({self.code})"
        if self.rejected:
            return f"[REJECTED] {label} — {self.reject_reason}"
        return (
            f"[{self.side}] {label} "
            f"{self.quantity}주 @ {self.exec_price:,.0f}원 "
            f"(슬리피지 {self.slippage_pct:.3%})"
        )


class OrderExecutor:

    def __init__(self, provider, portfolio, paper_trading: bool = True):
        self.provider      = provider
        self.portfolio     = portfolio
        self.paper_trading = paper_trading

    def execute(self, order: Order) -> TradeResult:
        # ── Risk Mode 체크 ──────────────────────────────────────────────
        mode = self.portfolio.risk_mode()

        if mode == "HARD_STOP" and order.side == "BUY":
            return self._rejected(order, "HARD_STOP — 월 DD 한도 초과, BUY 전면 금지")

        if mode == "DAILY_KILL" and order.side == "BUY":
            return self._rejected(order, "DAILY_KILL — 일 DD -4% 초과, 신규 진입 완전 차단")

        if mode == "SOFT_STOP" and order.side == "BUY":
            return self._rejected(order, "SOFT_STOP — 일 손실 한도 초과, 신규 진입 금지")

        # ── 유동성 체크 ─────────────────────────────────────────────────
        try:
            avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
            if avg_vol < MIN_DAILY_VOLUME:
                return self._rejected(order, f"유동성 부족 (5일 평균 {avg_vol:,.0f}원)")
        except Exception as e:
            _log.debug("[OrderExecutor] %s 유동성 조회 실패 → 통과 허용: %s", order.code, e)

        # ── 6중 게이트 (BUY) ────────────────────────────────────────────
        if order.side == "BUY":
            amount = order.price * order.quantity
            ok, reason = self.portfolio.can_enter(order.code, amount, order.sector)
            if not ok:
                return self._rejected(order, reason)

        # ── 실행 ────────────────────────────────────────────────────────
        if self.paper_trading:
            avg_vol = 0
            try:
                avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
            except Exception as e:
                _log.debug("[OrderExecutor] %s 슬리피지용 거래대금 조회 실패 → 0 사용: %s", order.code, e)
            return self._simulate(order, avg_vol)
        else:
            return self._send_to_kiwoom(order)

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _calc_slippage(self, order: Order, avg_daily_volume: float) -> float:
        """
        거래대금 기반 슬리피지 모델:
          대형주 (거래대금 200억+): 0.3~0.7%
          중형주 (거래대금 50~200억): 0.7~1.5%
          소형주 (거래대금 20~50억): 1.5~3.0%
        """
        order_amount    = order.price * order.quantity
        liquidity_ratio = order_amount / avg_daily_volume if avg_daily_volume > 0 else 1.0

        if avg_daily_volume >= 20_000_000_000:      # 200억+ 대형주
            base = 0.003
            cap  = 0.007
        elif avg_daily_volume >= 5_000_000_000:      # 50~200억 중형주
            base = 0.007
            cap  = 0.015
        else:                                        # 50억 미만 소형주
            base = 0.015
            cap  = 0.030

        liq_penalty = liquidity_ratio * 0.10
        return min(base + liq_penalty, cap)

    def _simulate(self, order: Order, avg_daily_volume: float) -> TradeResult:
        slippage   = self._calc_slippage(order, avg_daily_volume)
        exec_price = (
            order.price * (1 + slippage) if order.side == "BUY"
            else order.price * (1 - slippage)
        )
        self.portfolio.update_position(
            order.code, order.sector, order.quantity, exec_price, order.side
        )
        return TradeResult(
            code=order.code, side=order.side,
            quantity=order.quantity, exec_price=exec_price,
            slippage_pct=slippage, timestamp=datetime.now(),
        )

    def _send_to_kiwoom(self, order: Order) -> TradeResult:
        """Kiwoom API 실주문 (시장가)."""
        if not hasattr(self.provider, 'send_order'):
            return self._rejected(order, "KiwoomProvider가 아님 — 실거래 불가")

        result = self.provider.send_order(
            code=order.code,
            side=order.side,
            quantity=order.quantity,
            price=0,          # 시장가
            hoga_type="03",   # 시장가
        )

        if result["error"]:
            _log.error("[Kiwoom] %s %s 실패: %s", order.side, order.code, result["error"])
            return self._rejected(order, f"Kiwoom: {result['error']}")

        exec_price = result["exec_price"]
        exec_qty   = result["exec_qty"]
        slippage   = abs(exec_price - order.price) / order.price if order.price > 0 else 0.0

        self.portfolio.update_position(
            order.code, order.sector, exec_qty, exec_price, order.side,
        )

        _log.info(
            "[Kiwoom 체결] %s %s %d주 @ %,.0f원 (슬리피지 %.2f%%)",
            order.side, order.code, exec_qty, exec_price, slippage * 100,
        )

        return TradeResult(
            code=order.code, side=order.side,
            quantity=exec_qty, exec_price=exec_price,
            slippage_pct=slippage, timestamp=datetime.now(),
        )

    @staticmethod
    def _rejected(order: Order, reason: str) -> TradeResult:
        return TradeResult(
            code=order.code, side=order.side,
            quantity=0, exec_price=0, slippage_pct=0,
            timestamp=datetime.now(),
            rejected=True, reject_reason=reason,
        )
