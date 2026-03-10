"""
ExitLogic (v7)
==============
v7 청산 우선순위:
  1. SL         — 손실 제한 최우선
  2. RAL CRASH  — CRASH 모드 + rs_composite < 0.45 강제청산
  3. RS 청산    — 월초 rs_composite < 0.40 (RS_EXIT_THRESH)
  4. MAX_HOLD   — 최대 보유일 초과 (60일)

변경 (v7):
  - MA20 청산 제거 (RS 청산으로 대체)
  - TP 청산 제거 (추세추종 → 충분히 버티는 설계)
  - RAL CRASH 강제청산 추가
  - 월초 RS 청산 추가
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional

import pandas as pd

from core.portfolio_manager import PortfolioManager
from core.position_tracker import Position
from config import Gen3Config


class ExitLogic:

    def __init__(self, provider, executor, portfolio: PortfolioManager, config: Gen3Config,
                 trade_logger=None, ral_mode: str = "NORMAL"):
        self.provider     = provider
        self.executor     = executor
        self.portfolio    = portfolio
        self.config       = config
        self.trade_logger = trade_logger
        self.ral_mode     = ral_mode   # "CRASH" | "SURGE" | "NORMAL"

    def check_and_exit(self, signals_today: list = None) -> list:
        """
        보유 전 포지션 순회 → 청산 조건 체크 → 해당 시 청산.
        signals_today: EntrySignal.load_today() 결과 (rs_composite 조회용)
        """
        if not self.portfolio.positions:
            return []

        price_map = self._fetch_latest_prices()
        self.portfolio.update_prices(price_map)

        # 당일 RS 맵 (code → rs_composite)
        rs_map = {}
        if signals_today:
            for s in signals_today:
                rs_map[s["code"]] = float(s.get("rs_composite", s.get("qscore", 1.0)))

        is_month_start = self._is_month_start()

        exits = []  # (code, pos, current_price, close_type)
        for code, pos in list(self.portfolio.positions.items()):
            current    = price_map.get(code, pos.current_price)
            close_type = self._eval_exit(code, pos, current, rs_map, is_month_start)
            if close_type:
                exits.append((code, pos, current, close_type))

        if not exits:
            return []

        # 우선순위: SL → RAL_CRASH → RS_EXIT → MAX_HOLD
        priority = {"SL": 0, "RAL_CRASH": 1, "RS_EXIT": 2, "MAX_HOLD": 3}
        exits.sort(key=lambda x: priority.get(x[3], 9))

        print(f"[ExitLogic] 청산 대상 {len(exits)}개")
        results = []
        for code, pos, current, close_type in exits:
            result = self._execute_exit(code, pos, current, close_type)
            results.append(result)

        return results

    # ── 내부 ─────────────────────────────────────────────────────────────────

    def _eval_exit(
        self,
        code: str,
        pos: Position,
        current: float,
        rs_map: dict,
        is_month_start: bool,
    ) -> Optional[str]:
        """청산 조건 판별. 우선순위 순으로 확인."""

        # 1. SL
        if pos.sl > 0 and current <= pos.sl:
            return "SL"

        # 2. RAL CRASH 강제청산 (rs_composite < 0.45)
        if self.ral_mode == "CRASH":
            rs = rs_map.get(code, 1.0)  # signals에 없으면 청산 안 함
            if rs < self.config.RAL_CRASH_CLOSE_RS:
                return "RAL_CRASH"

        # 3. 월초 RS 청산 (rs_composite < 0.40)
        if is_month_start:
            rs = rs_map.get(code, 1.0)
            if rs < self.config.RS_EXIT_THRESH:
                return "RS_EXIT"

        # 4. 최대 보유일 초과
        if pos.held_days >= self.config.MAX_HOLD_DAYS:
            return "MAX_HOLD"

        return None

    def _is_month_start(self) -> bool:
        """오늘이 월초(1~5 거래일)인지 확인."""
        today = date.today()
        # 간단 구현: 1일부터 7일 이내 (영업일 기준 1~5일째 커버)
        return today.day <= 7

    def _fetch_latest_prices(self) -> dict:
        price_map = {}
        for code in self.portfolio.positions:
            try:
                p = self.provider.get_current_price(code)
                if p and p > 0:
                    price_map[code] = float(p)
            except Exception as e:
                print(f"  [ExitLogic] {code} 가격 조회 실패: {e}")
        return price_map

    def _execute_exit(self, code: str, pos: Position, current: float, close_type: str):
        from runtime.order_executor import Order
        order = Order(
            code=code, sector=pos.sector, side="SELL",
            quantity=pos.quantity, price=current,
        )
        result = self.executor.execute(order)

        tag = {
            "SL":        "[-]",
            "RAL_CRASH": "[R]",
            "RS_EXIT":   "[~]",
            "MAX_HOLD":  "[T]",
        }.get(close_type, "[?]")
        print(f"  {tag} [{close_type}] {result}")

        if self.trade_logger and not result.rejected:
            pnl = (current - pos.avg_price) * pos.quantity
            try:
                self.trade_logger.log_close(
                    code        = code,
                    close_type  = close_type,
                    entry_price = pos.avg_price,
                    close_price = current,
                    pnl         = pnl,
                )
            except Exception as e:
                print(f"  [ExitLogic] close_log 기록 실패 (비치명): {e}")

        return result
