from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Position:
    code: str
    sector: str
    quantity: int
    avg_price: float
    current_price: float
    tp: float = 0.0
    sl: float = 0.0
    q_score: float = 0.0
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


class Portfolio:
    def __init__(self, config):
        self.positions: dict[str, Position] = {}
        self.cash: float = config.initial_cash

        # ── 기준 시점 ─────────────────────────────────────
        self.prev_close_equity: float = config.initial_cash  # 전일 종가 기준 (daily PnL)
        self.peak_equity: float       = config.initial_cash  # 역대 고점 (monthly DD)

        # ── 리스크 한도 ───────────────────────────────────
        self.daily_loss_limit  = config.daily_loss_limit
        self.monthly_dd_limit  = config.monthly_dd_limit
        self.max_exposure      = config.max_exposure
        self.max_per_stock     = config.max_per_stock
        self.max_positions     = config.max_positions
        self.max_sector_exp    = config.max_sector_exp

    # ── 평가 ─────────────────────────────────────────────

    def get_current_equity(self) -> float:
        """현금 + 보유 종목 평가금액"""
        stock_value = sum(p.market_value for p in self.positions.values())
        return self.cash + stock_value

    def get_daily_pnl_pct(self) -> float:
        """전일 종가 대비 일간 손익률"""
        if self.prev_close_equity == 0:
            return 0.0
        return (self.get_current_equity() - self.prev_close_equity) / self.prev_close_equity

    def get_monthly_dd_pct(self) -> float:
        """고점 대비 낙폭 — 단순 누적 아님"""
        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        if self.peak_equity == 0:
            return 0.0
        return (equity - self.peak_equity) / self.peak_equity

    def get_exposure_pct(self) -> float:
        """현재 총 노출도"""
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        return (equity - self.cash) / equity

    def _get_sector_exposure(self, sector: str) -> float:
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        sector_value = sum(
            p.market_value for p in self.positions.values()
            if p.sector == sector
        )
        return sector_value / equity

    # ── Risk Mode ────────────────────────────────────────

    def risk_mode(self) -> str:
        """
        HARD_STOP : 월 DD 한도 초과 → 전면 매매 금지 + 청산 모드
        SOFT_STOP : 일 손실 한도 초과 → 신규 진입 금지
        NORMAL    : 정상 운용
        """
        if self.get_monthly_dd_pct() <= self.monthly_dd_limit:
            return "HARD_STOP"
        if self.get_daily_pnl_pct() <= self.daily_loss_limit:
            return "SOFT_STOP"
        return "NORMAL"

    def get_liquidation_targets(self) -> list[str]:
        """HARD_STOP 시 청산 대상 — 손실 큰 순서대로"""
        return sorted(
            self.positions.keys(),
            key=lambda c: self.positions[c].unrealized_pnl
        )

    # ── 6중 게이트 ────────────────────────────────────────

    def can_enter(self, code: str, amount: float, sector: str) -> tuple[bool, str]:
        equity = self.get_current_equity()

        # 1. 일일 손실 한도
        if self.get_daily_pnl_pct() <= self.daily_loss_limit:
            return False, f"일일 손실 한도 초과 ({self.get_daily_pnl_pct():.2%})"

        # 2. 월간 DD 한도
        if self.get_monthly_dd_pct() <= self.monthly_dd_limit:
            return False, f"월간 DD 한도 초과 ({self.get_monthly_dd_pct():.2%})"

        # 3. 최대 보유 종목 수 — 신규 진입만 체크
        if code not in self.positions and len(self.positions) >= self.max_positions:
            return False, f"최대 보유 종목 수 초과 ({self.max_positions}개)"

        # 4. 종목당 최대 비중
        current_val = self.positions[code].market_value if code in self.positions else 0.0
        if equity > 0 and (current_val + amount) / equity > self.max_per_stock:
            return False, f"종목당 최대 비중 초과 ({self.max_per_stock:.0%})"

        # 5. 섹터 노출 한도
        if self._get_sector_exposure(sector) + (amount / equity if equity > 0 else 0) > self.max_sector_exp:
            return False, f"섹터 노출 한도 초과 ({self.max_sector_exp:.0%})"

        # 6. 총 노출도 한도
        if self.get_exposure_pct() + (amount / equity if equity > 0 else 0) > self.max_exposure:
            return False, f"총 노출도 한도 초과 ({self.max_exposure:.0%})"

        return True, "OK"

    # ── 포지션 업데이트 ───────────────────────────────────

    def update_position(self, code: str, sector: str,
                        quantity: int, price: float, side: str):
        if side == "BUY":
            if code in self.positions:
                pos = self.positions[code]
                total_qty  = pos.quantity + quantity
                total_cost = pos.avg_price * pos.quantity + price * quantity
                pos.avg_price     = total_cost / total_qty
                pos.quantity      = total_qty
                pos.current_price = price
            else:
                self.positions[code] = Position(
                    code=code, sector=sector,
                    quantity=quantity, avg_price=price, current_price=price
                )
            self.cash -= price * quantity

        elif side == "SELL":
            if code in self.positions:
                pos = self.positions[code]
                pos.quantity -= quantity
                self.cash += price * quantity
                if pos.quantity <= 0:
                    del self.positions[code]

    def update_prices(self, price_map: dict[str, float]):
        """현재가 일괄 갱신"""
        for code, price in price_map.items():
            if code in self.positions:
                self.positions[code].current_price = price

    # ── 장 종료 처리 ──────────────────────────────────────

    def end_of_day_update(self):
        """장 종료 후 반드시 호출 — daily PnL 기준 초기화"""
        self.prev_close_equity = self.get_current_equity()

    def register_plan(self, code: str, tp: float, sl: float, q_score: float, rr: float):
        """포지션에 TP/SL 및 전략 정보 등록"""
        if code in self.positions:
            pos = self.positions[code]
            pos.tp = tp
            pos.sl = sl
            pos.q_score = q_score
            pos.rr_ratio = rr


    # ── 상태 요약 ─────────────────────────────────────────

    def summary(self) -> dict:
        equity = self.get_current_equity()
        return {
            "총평가금액":  f"{equity:,.0f}원",
            "현금":       f"{self.cash:,.0f}원",
            "보유종목수":  len(self.positions),
            "총노출도":   f"{self.get_exposure_pct():.1%}",
            "일간손익":   f"{self.get_daily_pnl_pct():.2%}",
            "월간DD":     f"{self.get_monthly_dd_pct():.2%}",
            "리스크모드":  self.risk_mode(),
        }
