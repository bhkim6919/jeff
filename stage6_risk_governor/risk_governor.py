from stage5_execution.execution_engine import Order


class RiskGovernor:
    """
    파이프라인 최우선 리스크 관리자.
    매 사이클 시작 시 evaluate() 호출 → 운용 모드 결정 + 강제 청산 실행.

    NORMAL    → 정상 파이프라인 진행
    SOFT_STOP → 신규 진입 금지, 기존 포지션 유지
    HARD_STOP → 전 포지션 강제 청산
    """

    def __init__(self, portfolio, execution_engine):
        self.portfolio = portfolio
        self.engine    = execution_engine

    def evaluate(self) -> str:
        mode = self.portfolio.risk_mode()

        if mode == "HARD_STOP":
            print(f"[HARD_STOP] 월 DD {self.portfolio.get_monthly_dd_pct():.2%} — 전 포지션 청산 시작")
            self._force_liquidate_all()

        elif mode == "SOFT_STOP":
            print(f"[SOFT_STOP] 일 손실 {self.portfolio.get_daily_pnl_pct():.2%} — 신규 진입 중단")
            self._reduce_exposure()

        return mode

    def _force_liquidate_all(self):
        """HARD_STOP: 전 포지션 시장가 청산 (손실 큰 순서)"""
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            print("[HARD_STOP] 청산 대상 없음")
            return

        for code in targets:
            pos = self.portfolio.positions.get(code)
            if not pos:
                continue
            order = Order(
                code=code, sector=pos.sector,
                side="SELL", quantity=pos.quantity, price=pos.current_price
            )
            result = self.engine.execute(order)
            print(f"  → {result}")

    def _reduce_exposure(self):
        """SOFT_STOP: 손실 가장 큰 종목 1개 청산"""
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            return

        code = targets[0]
        pos  = self.portfolio.positions.get(code)
        if not pos:
            return

        order = Order(
            code=code, sector=pos.sector,
            side="SELL", quantity=pos.quantity, price=pos.current_price
        )
        result = self.engine.execute(order)
        print(f"[SOFT_STOP 축소] {result}")
