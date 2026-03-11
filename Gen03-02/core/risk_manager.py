"""
RiskManager (Core)
==================
포트폴리오 수준 리스크 관리자.
매 사이클 시작 시 evaluate() → NORMAL / SOFT_STOP / HARD_STOP 결정.
HARD_STOP 시 전 포지션 강제 청산.

※ 포지션 사이징(TP/SL 계산)은 strategy/stage_manager.py 에서 수행.
"""

from core.portfolio_manager import PortfolioManager


class RiskManager:

    def __init__(self, portfolio: PortfolioManager, executor):
        self.portfolio = portfolio
        self.executor  = executor

    def evaluate(self) -> str:
        """
        현재 리스크 모드를 평가하고 필요한 조치(청산 등)를 즉시 실행한다.
        반환: 'NORMAL' | 'SOFT_STOP' | 'HARD_STOP'
        """
        mode = self.portfolio.risk_mode()

        if mode == "HARD_STOP":
            print(f"[HARD_STOP] 월 DD {self.portfolio.get_monthly_dd_pct():.2%} "
                  f"— 전 포지션 강제 청산")
            self._force_liquidate_all()

        elif mode == "DAILY_KILL":
            print(f"[DAILY_KILL] 일 DD {self.portfolio.get_daily_pnl_pct():.2%} "
                  f"— 신규 진입 완전 차단 (포지션 유지)")

        elif mode == "SOFT_STOP":
            print(f"[SOFT_STOP] 일 손실 {self.portfolio.get_daily_pnl_pct():.2%} "
                  f"— 신규 진입 중단 / 손실 최대 종목 1개 청산")
            self._reduce_exposure()

        return mode

    def _force_liquidate_all(self) -> None:
        from runtime.order_executor import Order
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            print("[HARD_STOP] 청산 대상 없음")
            return
        for code in targets:
            pos = self.portfolio.positions.get(code)
            if not pos:
                continue
            order = Order(code=code, sector=pos.sector, side="SELL",
                          quantity=pos.quantity, price=pos.current_price)
            result = self.executor.execute(order)
            print(f"  → {result}")

    def _reduce_exposure(self) -> None:
        from runtime.order_executor import Order
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            return
        code = targets[0]
        pos  = self.portfolio.positions.get(code)
        if not pos:
            return
        order = Order(code=code, sector=pos.sector, side="SELL",
                      quantity=pos.quantity, price=pos.current_price)
        result = self.executor.execute(order)
        print(f"[SOFT_STOP 축소] {result}")
