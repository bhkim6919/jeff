"""
StopManager (공식 청산 엔진)
============================
Stage 0.5 — RiskGovernor 직후, 신규 진입 전에 실행.

역할:
  - 보유 포지션의 현재가를 provider에서 직접 조회 (stale price 방지)
  - TP / SL / MA20 / (Gen2: 보유일 초과) 조건 체크
  - 조건 충족 시 ExecutionEngine으로 시장가 청산 주문
  - TradeLogger에 청산 기록 (close_log.csv)

청산 우선순위 (Gen2 포함):
  1. SL 먼저      (손실 제한 최우선)
  2. MAX_HOLD     (보유일 초과 — 시간 리스크 정리)
  3. MA20 돌파    (추세 이탈)
  4. TP           (익절)
"""

import pandas as pd
from typing import Optional
from datetime import datetime, date

from stage5_execution.execution_engine import ExecutionEngine, Order, TradeResult
from stage5_execution.trade_logger import TradeLogger
from core.portfolio import Portfolio
from core.data_provider import DataProvider
from config import QTronConfig


class StopManager:

    def __init__(
        self,
        provider: DataProvider,
        engine: ExecutionEngine,
        portfolio: Portfolio,
        trade_logger: TradeLogger,
        config: QTronConfig,
    ):
        self.provider     = provider
        self.engine       = engine
        self.portfolio    = portfolio
        self.logger       = trade_logger
        self.config       = config

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def check_and_exit(self) -> list[TradeResult]:
        """
        보유 전 포지션을 순회하며 TP/SL/MA20/보유일 조건 체크 후 청산.
        반환: 청산 시도된 TradeResult 리스트 (미발동 시 빈 리스트)
        """
        if not self.portfolio.positions:
            return []

        # ── 최신가 일괄 조회 & 포트폴리오 갱신 ───────────────────────────────
        price_map = self._fetch_latest_prices()
        self.portfolio.update_prices(price_map)

        # ── 청산 대상 판별 ────────────────────────────────────────────────────
        exits: list[tuple] = []   # (code, pos, current, close_type)

        for code, pos in list(self.portfolio.positions.items()):
            current = price_map.get(code, pos.current_price)

            # TP/SL이 미설정된 포지션은 건너뜀 (단, Gen2 보유일 초과는 별도 체크)
            if pos.tp == 0.0 and pos.sl == 0.0:
                # 보유일 초과만 따로 보려면 여기에서 _is_max_hold_exceeded를 추가로 체크할 수 있음
                continue

            close_type = self._eval_exit(code, pos, current)
            if close_type:
                exits.append((code, pos, current, close_type))

        if not exits:
            return []

        # ── SL → MAX_HOLD → MA20 → TP 순서로 정렬 후 청산 실행 ───────────────
        priority = {"SL": 0, "MAX_HOLD": 1, "MA20": 2, "TP": 3}
        exits.sort(key=lambda x: priority.get(x[3], 9))

        print(f"[StopManager] 청산 대상 {len(exits)}개 발견")
        results = []
        for code, pos, current, close_type in exits:
            result = self._execute_exit(code, pos, current, close_type)
            results.append(result)

        return results

    # ── 내부 로직 ─────────────────────────────────────────────────────────────

    def _fetch_latest_prices(self) -> dict[str, float]:
        """보유 종목 전체의 최신가를 provider에서 직접 조회."""
        price_map = {}
        for code in self.portfolio.positions:
            try:
                price = self.provider.get_current_price(code)
                if price and price > 0:
                    price_map[code] = float(price)
            except Exception as e:
                print(f"  [StopManager] {code} 가격 조회 실패: {e}")
        return price_map

    def _eval_exit(self, code: str, pos, current: float) -> Optional[str]:
        """
        청산 조건 판별. 우선순위: SL > MAX_HOLD > MA20 > TP
        반환: 'SL' | 'MAX_HOLD' | 'MA20' | 'TP' | None
        """
        # 1. SL
        if pos.sl > 0 and current <= pos.sl:
            return "SL"

        # 2. 보유일 초과 (Gen2 Core v1.0)
        if self._is_max_hold_exceeded(pos):
            return "MAX_HOLD"

        # 3. MA20 이탈 (종가 기준 — intraday 흔들림 방지)
        if self._is_below_ma20(code, current):
            return "MA20"

        # 4. TP
        if pos.tp > 0 and current >= pos.tp:
            return "TP"

        return None

    def _is_below_ma20(self, code: str, current: float) -> bool:
        """20일 이동평균선 하향 이탈 여부."""
        try:
            df = self.provider.get_stock_ohlcv(code, days=25)
            if df is None or len(df) < 20:
                return False
            ma20 = float(df["close"].rolling(20).mean().iloc[-1])
            return current < ma20
        except Exception:
            return False

    def _is_max_hold_exceeded(self, pos) -> bool:
        """
        Gen2용 보유일 초과 여부 체크.
        - config.GEN2_MAX_HOLD_DAYS 또는 config.MAX_HOLD_DAYS 를 사용
        - pos.entry_date 또는 pos.opened_at 가 date/datetime 이면 계산
        - 필드가 없으면 False (청산 조건 미적용)
        """
        days_limit = getattr(self.config, "GEN2_MAX_HOLD_DAYS", None) or getattr(self.config, "MAX_HOLD_DAYS", None)
        if not days_limit:
            return False

        entry = getattr(pos, "entry_date", None) or getattr(pos, "opened_at", None)
        if entry is None:
            return False

        if isinstance(entry, datetime):
            entry_date = entry.date()
        elif isinstance(entry, date):
            entry_date = entry
        else:
            # 예상치 못한 타입이면 안전하게 무시
            return False

        held_days = (date.today() - entry_date).days
        return held_days >= days_limit

    def _execute_exit(
        self, code: str, pos, current: float, close_type: str
    ) -> TradeResult:
        """청산 주문 실행 + 로그 기록."""
        order = Order(
            code=code,
            sector=pos.sector,
            side="SELL",
            quantity=pos.quantity,
            price=current,
        )
        result = self.engine.execute(order)

        tag = {"SL": "🔴", "MA20": "🟡", "TP": "🟢", "MAX_HOLD": "⚪"}.get(close_type, "⚪")
        print(f"  {tag} [{close_type}] {result}")

        # close_log.csv 기록
        if not result.rejected:
            pnl = (result.exec_price - pos.avg_price) * pos.quantity
            self.logger.log_close(
                code        = code,
                close_type  = close_type,
                entry_price = pos.avg_price,
                close_price = result.exec_price,
                pnl         = pnl,
            )

        return result