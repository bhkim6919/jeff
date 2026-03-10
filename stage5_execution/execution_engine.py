from dataclasses import dataclass
from datetime import datetime


MIN_DAILY_VOLUME = 2_000_000_000  # 5억원 미만 → 진입 금지


@dataclass
class Order:
    code: str
    sector: str
    side: str        # "BUY" | "SELL"
    quantity: int
    price: float


@dataclass
class TradeResult:
    code: str
    side: str
    quantity: int
    exec_price: float
    slippage_pct: float
    timestamp: datetime
    rejected: bool = False
    reject_reason: str = ""

    def __str__(self):
        if self.rejected:
            return f"[REJECTED] {self.code} — {self.reject_reason}"
        return (
            f"[{self.side}] {self.code} "
            f"{self.quantity}주 @ {self.exec_price:,.0f}원 "
            f"(슬리피지 {self.slippage_pct:.3%})"
        )


class ExecutionEngine:
    def __init__(self, provider, portfolio, paper_trading: bool = True):
        self.provider      = provider
        self.portfolio     = portfolio
        self.paper_trading = paper_trading

    def execute(self, order: Order) -> TradeResult:
        # ── Risk Mode 체크 ──────────────────────────────
        mode = self.portfolio.risk_mode()

        if mode == "HARD_STOP" and order.side == "BUY":
            return self._rejected(order, "HARD_STOP — 월 DD 한도 초과, BUY 전면 금지")

        if mode == "SOFT_STOP" and order.side == "BUY":
            return self._rejected(order, "SOFT_STOP — 일 손실 한도 초과, 신규 진입 금지")

        # ── 유동성 체크 ─────────────────────────────────
        avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
        if avg_vol < MIN_DAILY_VOLUME:
            return self._rejected(order, f"유동성 부족 (5일 평균 거래대금 {avg_vol:,.0f}원)")

        # ── 6중 게이트 (BUY만) ──────────────────────────
        if order.side == "BUY":
            amount = order.price * order.quantity
            ok, reason = self.portfolio.can_enter(order.code, amount, order.sector)
            if not ok:
                return self._rejected(order, reason)

        # ── 실행 ────────────────────────────────────────
        if self.paper_trading:
            return self._simulate(order, avg_vol)
        else:
            return self._send_to_kiwoom(order)

    def _calc_slippage(self, order: Order, avg_daily_volume: float) -> float:
        """동적 슬리피지 — 유동성 기반"""
        order_amount    = order.price * order.quantity
        liquidity_ratio = order_amount / avg_daily_volume if avg_daily_volume > 0 else 1.0

        base        = 0.001                      # 기본 0.1%
        liq_penalty = liquidity_ratio * 0.05     # 유동성 비율 × 5%

        return min(base + liq_penalty, 0.03)     # 최대 3% 캡

    def _simulate(self, order: Order, avg_daily_volume: float) -> TradeResult:
        slippage = self._calc_slippage(order, avg_daily_volume)

        exec_price = (
            order.price * (1 + slippage) if order.side == "BUY"
            else order.price * (1 - slippage)
        )

        self.portfolio.update_position(
            order.code, order.sector,
            order.quantity, exec_price, order.side
        )

        return TradeResult(
            code=order.code, side=order.side,
            quantity=order.quantity, exec_price=exec_price,
            slippage_pct=slippage, timestamp=datetime.now()
        )

    def _send_to_kiwoom(self, order: Order) -> TradeResult:
        raise NotImplementedError("실거래 연결은 안정화 후 구현 — data/kiwoom_provider.py 참고")

    @staticmethod
    def _rejected(order: Order, reason: str) -> TradeResult:
        return TradeResult(
            code=order.code, side=order.side,
            quantity=0, exec_price=0, slippage_pct=0,
            timestamp=datetime.now(),
            rejected=True, reject_reason=reason
        )
