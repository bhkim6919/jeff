# -*- coding: utf-8 -*-
"""
BacktestLoop
=============
bar-by-bar 백테스트 엔진.

두 가지 모드 비교:
  A) "Gen3Only" — TrendStrategy를 모든 레짐에서 사용 (기존 Gen3 동작 재현)
  B) "MultiStrategy" — 레짐별 전략 자동 선택 (Trend/MR/Defense)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.strategy_base import Signal, Strategy, StrategySelector
from backtest.historical_provider import HistoricalProvider, BacktestRegimeDetector


# ── 비용 상수 ──────────────────────────────────────────────────────────────

FEE = 0.00015
SLIPPAGE = 0.001
TAX = 0.0018
ENTRY_COST = FEE + SLIPPAGE
EXIT_COST = FEE + SLIPPAGE + TAX


@dataclass
class Position:
    code: str
    entry_price: float
    quantity: int
    sl: float
    tp: float
    sector: str
    strategy_name: str
    entry_date: pd.Timestamp
    prev_close: float = 0.0
    max_hold: int = 60
    hold_days: int = 0
    high_watermark: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def market_value(self) -> float:
        return self.entry_price * self.quantity

    def to_exit_dict(self) -> dict:
        return {
            "sl": self.sl, "tp": self.tp,
            "prev_close": self.prev_close,
            "hold_days": self.hold_days,
            "max_hold": self.max_hold,
        }


@dataclass
class Trade:
    code: str
    strategy_name: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    entry_price: float
    exit_price: float
    quantity: int
    pnl_pct: float
    pnl_won: float
    exit_type: str
    hold_days: int
    sector: str
    regime_at_entry: str


class BacktestEngine:
    """
    bar-by-bar 포트폴리오 백테스트.

    매 거래일:
      1. 기존 포지션 청산 점검 (SL/TP/MAX_HOLD/GAP_DOWN)
      2. 레짐 감지
      3. 전략 선택 → 시그널 생성
      4. 포지션 사이징 + 진입
      5. prev_close 업데이트
    """

    def __init__(
        self,
        provider: HistoricalProvider,
        selector: StrategySelector,
        regime_detector: BacktestRegimeDetector,
        *,
        initial_cash: float = 100_000_000,
        max_positions: int = 20,
        weight_per_pos: float = 0.07,
        signal_interval: int = 5,
        label: str = "Backtest",
    ):
        self.provider = provider
        self.selector = selector
        self.regime_det = regime_detector
        self.initial_cash = initial_cash
        self.max_positions = max_positions
        self.weight_per_pos = weight_per_pos
        self.signal_interval = signal_interval
        self.label = label

        # 포트폴리오 상태
        self.cash: float = initial_cash
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_curve: List[dict] = []

        # 레짐 이력
        self.regime_history: List[dict] = []

    def run(self, start: str, end: str, progress: bool = True) -> Dict[str, Any]:
        """
        start~end 기간 백테스트 실행.
        signal_interval 거래일마다 시그널 생성 (매일 청산은 수행).
        """
        dates = self.provider.get_trade_dates(start, end)
        if not dates:
            print(f"[{self.label}] 거래일 없음: {start}~{end}")
            return {}

        # 시그널 생성에 필요한 과거 데이터 확보를 위해 처음 200일은 워밍업
        warmup = 200
        all_index_dates = self.provider.index_df["date"].tolist()
        start_idx = 0
        for i, d in enumerate(all_index_dates):
            if d >= dates[0]:
                start_idx = i
                break
        if start_idx < warmup:
            actual_start_idx = warmup
            if actual_start_idx < len(all_index_dates):
                actual_start = all_index_dates[actual_start_idx]
                dates = [d for d in dates if d >= actual_start]
                if not dates:
                    print(f"[{self.label}] 워밍업 후 거래일 없음")
                    return {}
                print(f"[{self.label}] 워밍업 {warmup}일 → 실제 시작: {dates[0].strftime('%Y-%m-%d')}")

        total = len(dates)
        print(f"[{self.label}] {dates[0].strftime('%Y-%m-%d')} ~ "
              f"{dates[-1].strftime('%Y-%m-%d')} ({total} 거래일)")

        for i, bar_date in enumerate(dates):
            # 1. 기존 포지션 청산 점검
            self._check_exits(bar_date)

            # 2. 레짐 감지 (매 시그널 생성일에만, 성능 최적화)
            if i % self.signal_interval == 0:
                index_cut = self.provider.get_index_at(bar_date)
                universe = self.provider.get_universe_at(bar_date)
                regime = self.regime_det.detect(index_cut, universe)

                self.regime_history.append({
                    "date": bar_date, "regime": regime,
                })

                # 3. 전략 선택 → 시그널 생성
                strategy = self.selector.select(regime)
                signals = strategy.generate_signals(
                    bar_date, universe, index_cut, regime,
                    self.provider.sector_map,
                )

                # 4. 진입
                self._enter_positions(signals, bar_date, regime, strategy)

            # 5. prev_close 업데이트 + equity 기록
            self._update_day_end(bar_date)

            if progress and (i + 1) % 50 == 0:
                eq = self._equity(bar_date)
                pnl = (eq / self.initial_cash - 1) * 100
                regime_str = self.regime_history[-1]["regime"] if self.regime_history else "?"
                print(f"\r  [{self.label}] {i+1}/{total} "
                      f"{bar_date.strftime('%Y-%m-%d')} "
                      f"equity={eq:,.0f} ({pnl:+.1f}%) "
                      f"pos={len(self.positions)} regime={regime_str}     ", end="")

        # 잔여 포지션 강제 청산
        if dates:
            self._force_close_all(dates[-1])

        if progress:
            print()

        return self._build_result()

    # ── 청산 점검 ──────────────────────────────────────────────────────────

    def _check_exits(self, bar_date: pd.Timestamp) -> None:
        to_close = []
        for code, pos in self.positions.items():
            bar = self.provider.get_bar(code, bar_date)
            if bar is None:
                continue

            pos.hold_days += 1
            strategy = self.selector.select(
                self.regime_history[-1]["regime"] if self.regime_history else "BEAR"
            )
            reason = strategy.exit_check(pos.to_exit_dict(), bar,
                                         self.regime_history[-1]["regime"] if self.regime_history else "BEAR")
            if reason:
                # 청산 가격 결정
                if reason == "SL":
                    exit_price = min(pos.sl, bar["close"])
                elif reason == "TP":
                    exit_price = max(pos.tp, bar["close"])
                else:
                    exit_price = bar["close"]

                to_close.append((code, exit_price, reason, bar_date))
            else:
                # prev_close 갱신 (다음날 GAP_DOWN 판정용)
                pos.prev_close = bar["close"]

        for code, exit_price, reason, dt in to_close:
            self._close_position(code, exit_price, reason, dt)

    def _close_position(self, code: str, exit_price: float,
                        reason: str, exit_date: pd.Timestamp) -> None:
        pos = self.positions.pop(code, None)
        if pos is None:
            return

        proceeds = exit_price * pos.quantity
        cost = proceeds * EXIT_COST
        net = proceeds - cost
        self.cash += net

        pnl_pct = (exit_price / pos.entry_price - 1) - ENTRY_COST - EXIT_COST
        pnl_won = net - pos.entry_price * pos.quantity * (1 + ENTRY_COST)

        regime_at = ""
        for rh in reversed(self.regime_history):
            if rh["date"] <= pos.entry_date:
                regime_at = rh["regime"]
                break

        self.trades.append(Trade(
            code=code, strategy_name=pos.strategy_name,
            entry_date=pos.entry_date, exit_date=exit_date,
            entry_price=pos.entry_price, exit_price=exit_price,
            quantity=pos.quantity,
            pnl_pct=pnl_pct, pnl_won=pnl_won,
            exit_type=reason, hold_days=pos.hold_days,
            sector=pos.sector, regime_at_entry=regime_at,
        ))

    # ── 진입 ─────────────────────────────────────────────────────────────

    def _enter_positions(self, signals: List[Signal], bar_date: pd.Timestamp,
                         regime: str, strategy: Strategy) -> None:
        available_slots = self.max_positions - len(self.positions)
        if available_slots <= 0:
            return

        # Defense 전략 포지션 수 제한
        if hasattr(strategy, 'max_pos'):
            available_slots = min(available_slots, strategy.max_pos)

        for sig in signals[:available_slots]:
            if sig.code in self.positions:
                continue  # 중복 진입 방지

            equity = self._equity(bar_date)
            weight = self.weight_per_pos
            if sig.extra.get("weight_mult"):
                weight *= sig.extra["weight_mult"]

            amount = equity * weight
            amount = min(amount, self.cash * 0.95)  # 현금 여유 5%
            if amount <= 0:
                continue

            qty = int(amount // sig.entry)
            if qty <= 0:
                continue

            cost = sig.entry * qty * (1 + ENTRY_COST)
            if cost > self.cash:
                continue

            self.cash -= cost
            self.positions[sig.code] = Position(
                code=sig.code, entry_price=sig.entry, quantity=qty,
                sl=sig.sl, tp=sig.tp, sector=sig.sector,
                strategy_name=sig.strategy_name, entry_date=bar_date,
                prev_close=sig.entry, high_watermark=sig.entry,
                max_hold=sig.extra.get("max_hold", 60),
                extra=sig.extra,
            )

    # ── 일일 마감 ──────────────────────────────────────────────────────────

    def _update_day_end(self, bar_date: pd.Timestamp) -> None:
        eq = self._equity(bar_date)
        regime = self.regime_history[-1]["regime"] if self.regime_history else "?"
        self.equity_curve.append({
            "date": bar_date, "equity": eq, "cash": self.cash,
            "positions": len(self.positions), "regime": regime,
        })

    def _equity(self, bar_date: pd.Timestamp) -> float:
        pos_value = 0.0
        for code, pos in self.positions.items():
            bar = self.provider.get_bar(code, bar_date)
            if bar:
                pos_value += bar["close"] * pos.quantity
            else:
                pos_value += pos.entry_price * pos.quantity
        return self.cash + pos_value

    def _force_close_all(self, last_date: pd.Timestamp) -> None:
        codes = list(self.positions.keys())
        for code in codes:
            bar = self.provider.get_bar(code, last_date)
            price = bar["close"] if bar else self.positions[code].entry_price
            self._close_position(code, price, "EOD_FORCE", last_date)

    # ── 결과 빌드 ──────────────────────────────────────────────────────────

    def _build_result(self) -> Dict[str, Any]:
        if not self.equity_curve:
            return {}

        eq_series = [e["equity"] for e in self.equity_curve]
        total_return = eq_series[-1] / self.initial_cash - 1

        # MDD
        peak = eq_series[0]
        mdd = 0.0
        for e in eq_series:
            peak = max(peak, e)
            dd = (e - peak) / peak
            mdd = min(mdd, dd)

        # 거래 통계
        n_trades = len(self.trades)
        wins = [t for t in self.trades if t.pnl_pct > 0]
        losses = [t for t in self.trades if t.pnl_pct <= 0]
        win_rate = len(wins) / n_trades if n_trades else 0
        avg_win = sum(t.pnl_pct for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.pnl_pct for t in losses) / len(losses) if losses else 0
        avg_hold = sum(t.hold_days for t in self.trades) / n_trades if n_trades else 0

        # 레짐 분포
        regime_counts = {}
        for rh in self.regime_history:
            r = rh["regime"]
            regime_counts[r] = regime_counts.get(r, 0) + 1

        return {
            "label": self.label,
            "total_return": total_return,
            "mdd": mdd,
            "n_trades": n_trades,
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "avg_hold_days": avg_hold,
            "final_equity": eq_series[-1],
            "regime_counts": regime_counts,
            "trades": self.trades,
            "equity_curve": self.equity_curve,
        }
