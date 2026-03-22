"""
PositionMonitor
===============
장중 포지션 모니터링. ExitLogic을 주기적으로 호출하여
TP/SL/MA20/MAX_HOLD 조건 발동 시 자동 청산한다.

런타임에서는 RuntimeEngine이 1회 호출(장 초 포지션 점검)하는 형태로 사용.
향후 루프 기반 인트라데이 모니터링으로 확장 가능.
"""

from typing import List

from strategy.exit_logic import ExitLogic
from core.portfolio_manager import PortfolioManager
from config import Gen3Config


class PositionMonitor:

    def __init__(self, provider, executor, portfolio: PortfolioManager, config: Gen3Config,
                 trade_logger=None):
        self.exit_logic = ExitLogic(provider, executor, portfolio, config, trade_logger)
        self.portfolio  = portfolio

    def check_exits(self) -> list:
        """
        보유 포지션 전체에 청산 조건 체크.
        청산 발동된 TradeResult 리스트 반환.
        """
        if not self.portfolio.positions:
            print("[PositionMonitor] 보유 포지션 없음")
            return []

        print(f"[PositionMonitor] {len(self.portfolio.positions)}개 포지션 점검")
        results = self.exit_logic.check_and_exit()

        if results:
            closed = [r for r in results if not r.rejected]
            print(f"[PositionMonitor] 청산 완료: {len(closed)}건")
        else:
            print("[PositionMonitor] 청산 조건 미발동")

        return results

    def print_positions(self) -> None:
        """현재 보유 포지션 출력 (v7.6: hold/sellable/confidence 분리 표시)."""
        if not self.portfolio.positions:
            print("[PositionMonitor] 보유 포지션 없음")
            return
        print(f"[PositionMonitor] 보유 {len(self.portfolio.positions)}개:")
        from data.name_lookup import get_name
        for code, pos in self.portfolio.positions.items():
            pnl_pct = pos.unrealized_pnl_pct
            sign    = "▲" if pnl_pct >= 0 else "▼"
            label   = f"{get_name(code)}({code})"
            # v7.8: hold/sell/net/broker 수량 통일 출력
            sell_str = f"{pos.qty_sellable}" if pos.qty_sellable >= 0 else "?"
            net_str = f"{pos.net_qty}" if hasattr(pos, 'net_qty') else "?"
            bc_str = f"{pos.broker_confirmed_qty}" if getattr(pos, 'broker_confirmed_qty', -1) >= 0 else "?"
            conf = getattr(pos, 'qty_confidence', 'HIGH')
            conf_tag = "" if conf == "HIGH" else f" [{conf}]"
            reason_tag = ""
            if getattr(pos, 'restricted_reason', ''):
                reason_tag = f" ({pos.restricted_reason})"
            print(f"  {label}  hold={pos.quantity}/sell={sell_str}/net={net_str}/bkr={bc_str}  "
                  f"평균 {pos.avg_price:,.0f}원  "
                  f"현재 {pos.current_price:,.0f}원  "
                  f"{sign}{abs(pnl_pct):.2%}  "
                  f"TP={pos.tp:,}  SL={pos.sl:,}  "
                  f"보유{pos.held_days}일{conf_tag}{reason_tag}")
