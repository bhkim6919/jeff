"""
PortfolioManager
================
현금 + 보유 포지션 관리. Gen3에서 position_tracker.Position을 사용한다.

6중 게이트 (can_enter):
  1. 일일 손실 한도
  2. 월간 DD 한도
  3. 최대 보유 종목 수
  4. 종목당 최대 비중
  5. 섹터 노출 한도
  6. 총 노출도 한도
"""

from datetime import date
from typing import Dict, List, Tuple

from config import Gen3Config
from core.position_tracker import Position


class PortfolioManager:

    def __init__(self, config: Gen3Config):
        self.positions: Dict[str, Position] = {}
        self.cash: float = config.initial_cash

        self.prev_close_equity: float = config.initial_cash
        self.peak_equity:       float = config.initial_cash
        self._peak_month:       int   = date.today().month

        # 리스크 한도
        self.daily_loss_limit = config.daily_loss_limit
        self.monthly_dd_limit = config.monthly_dd_limit
        self.max_exposure     = config.max_exposure
        self.max_per_stock    = config.max_per_stock
        self.max_positions    = config.max_positions
        self.max_sector_exp   = config.max_sector_exp

    # ── 평가 ─────────────────────────────────────────────────────────────────

    def get_current_equity(self) -> float:
        return self.cash + sum(p.market_value for p in self.positions.values())

    def get_daily_pnl_pct(self) -> float:
        if self.prev_close_equity == 0:
            return 0.0
        return (self.get_current_equity() - self.prev_close_equity) / self.prev_close_equity

    def get_monthly_dd_pct(self) -> float:
        today = date.today()
        if today.month != self._peak_month:
            self._peak_month = today.month
            self.peak_equity = self.get_current_equity()
        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        if self.peak_equity == 0:
            return 0.0
        return (equity - self.peak_equity) / self.peak_equity

    def get_exposure_pct(self) -> float:
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        return (equity - self.cash) / equity

    def _sector_exposure(self, sector: str) -> float:
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        sec_val = sum(p.market_value for p in self.positions.values() if p.sector == sector)
        return sec_val / equity

    def get_sector_exposures(self) -> Dict[str, float]:
        """섹터별 노출도 반환 {sector: pct}, 노출도 내림차순."""
        equity = self.get_current_equity()
        if equity == 0:
            return {}
        sector_vals: Dict[str, float] = {}
        for pos in self.positions.values():
            sector_vals[pos.sector] = sector_vals.get(pos.sector, 0.0) + pos.market_value
        return dict(sorted(
            {s: v / equity for s, v in sector_vals.items()}.items(),
            key=lambda x: -x[1]
        ))

    def sector_capacity_remaining(self, sector: str) -> float:
        """해당 섹터에 추가 투자 가능한 금액 (max_sector_exp 기준)."""
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        used = self._sector_exposure(sector)
        remaining_pct = max(0.0, self.max_sector_exp - used)
        return remaining_pct * equity

    # ── Risk Mode ────────────────────────────────────────────────────────────

    def risk_mode(self) -> str:
        """HARD_STOP | SOFT_STOP | NORMAL"""
        if self.get_monthly_dd_pct() < self.monthly_dd_limit:
            return "HARD_STOP"
        if self.get_daily_pnl_pct() < self.daily_loss_limit:
            return "SOFT_STOP"
        return "NORMAL"

    def get_liquidation_targets(self) -> List[str]:
        """HARD_STOP 시 청산 대상 (손실 큰 순)."""
        return sorted(self.positions.keys(),
                      key=lambda c: self.positions[c].unrealized_pnl)

    # ── 6중 게이트 ────────────────────────────────────────────────────────────

    def can_enter(self, code: str, amount: float, sector: str) -> Tuple[bool, str]:
        equity = self.get_current_equity()

        if self.get_daily_pnl_pct() < self.daily_loss_limit:
            return False, f"일일 손실 한도 초과 ({self.get_daily_pnl_pct():.2%})"

        if self.get_monthly_dd_pct() < self.monthly_dd_limit:
            return False, f"월간 DD 한도 초과 ({self.get_monthly_dd_pct():.2%})"

        if code not in self.positions and len(self.positions) >= self.max_positions:
            return False, f"최대 보유 종목 수 초과 ({self.max_positions}개)"

        cur_val = self.positions[code].market_value if code in self.positions else 0.0
        if equity > 0 and (cur_val + amount) / equity > self.max_per_stock:
            return False, f"종목당 최대 비중 초과 ({self.max_per_stock:.0%})"

        if self._sector_exposure(sector) + (amount / equity if equity > 0 else 0) > self.max_sector_exp:
            return False, f"섹터 노출 한도 초과 ({self.max_sector_exp:.0%})"

        if self.get_exposure_pct() + (amount / equity if equity > 0 else 0) > self.max_exposure:
            return False, f"총 노출도 한도 초과 ({self.max_exposure:.0%})"

        return True, "OK"

    # ── 포지션 업데이트 ───────────────────────────────────────────────────────

    def update_position(self, code: str, sector: str,
                        quantity: int, price: float, side: str) -> None:
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
                    quantity=quantity, avg_price=price, current_price=price,
                )
            self.cash -= price * quantity

        elif side == "SELL":
            if code in self.positions:
                pos = self.positions[code]
                pos.quantity -= quantity
                self.cash    += price * quantity
                if pos.quantity <= 0:
                    del self.positions[code]

    def update_prices(self, price_map: Dict[str, float]) -> None:
        for code, price in price_map.items():
            if code in self.positions:
                self.positions[code].current_price = price

    def register_plan(self, code: str, tp: float, sl: float,
                      q_score: float = 0.0, rr_ratio: float = 0.0) -> None:
        pos = self.positions.get(code)
        if pos and pos.tp == 0.0 and pos.sl == 0.0:
            pos.tp       = tp
            pos.sl       = sl
            pos.q_score  = q_score
            pos.rr_ratio = rr_ratio

    def has_position(self, code: str) -> bool:
        pos = self.positions.get(code)
        return bool(pos and pos.quantity > 0)

    # ── 장 종료 ───────────────────────────────────────────────────────────────

    def end_of_day_update(self) -> None:
        self.prev_close_equity = self.get_current_equity()

    # ── 요약 ─────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        equity = self.get_current_equity()
        sec_exp = self.get_sector_exposures()
        # 한도(30%) 초과 위험 섹터 강조
        sec_info = {
            s: f"{v:.1%}{'(!!)' if v >= self.max_sector_exp * 0.8 else ''}"
            for s, v in list(sec_exp.items())[:5]
        }
        return {
            "총평가금액": f"{equity:,.0f}원",
            "현금":       f"{self.cash:,.0f}원",
            "보유종목수": len(self.positions),
            "총노출도":   f"{self.get_exposure_pct():.1%}",
            "일간손익":   f"{self.get_daily_pnl_pct():.2%}",
            "월간DD":     f"{self.get_monthly_dd_pct():.2%}",
            "리스크모드": self.risk_mode(),
            "섹터노출도": sec_info,
        }
