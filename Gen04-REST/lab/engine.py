"""
engine.py — Lab 체결 엔진
==========================
전략별 독립 StrategyState. ExitPolicy 기반 종료.
BASELINE_SPEC 기준 체결 규칙 적용.
"""
from __future__ import annotations
import copy
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

import pandas as pd

from lab.snapshot import DailySnapshot
from lab.lab_config import LabConfig, FillTiming
from lab.lab_errors import LabStrategyError
from lab.strategies.base import BaseStrategy, Signal

logger = logging.getLogger("lab.engine")


@dataclass
class PendingOrder:
    ticker: str
    target_idx: int          # fill at this day_idx
    per_pos: float           # allocation per position


@dataclass
class StrategyState:
    """전략별 완전 독립 상태. 전략 간 공유 금지."""
    name: str
    cash: float
    positions: Dict[str, dict] = field(default_factory=dict)
    pending_buys: List[PendingOrder] = field(default_factory=list)
    trades: List[dict] = field(default_factory=list)
    equity_history: Dict[pd.Timestamp, float] = field(default_factory=dict)
    strategy: Optional[BaseStrategy] = None

    # 통계
    total_buy_amount: float = 0.0
    total_sell_amount: float = 0.0


def process_pending_fills(state: StrategyState, snapshot: DailySnapshot,
                          config: LabConfig) -> None:
    """T+1 open에서 pending buy 체결. BASELINE_SPEC 기준."""
    filled = []
    for pb in state.pending_buys:
        if snapshot.day_idx != pb.target_idx:
            continue

        tk = pb.ticker
        if tk in state.positions:
            filled.append(pb)
            continue

        entry_price = float(snapshot.open.get(tk, 0))
        if pd.isna(entry_price) or entry_price <= 0:
            filled.append(pb)
            continue

        # BASELINE_SPEC: qty = floor(amount / buy_cost_total)
        buy_cost_total = entry_price * (1 + config.BUY_COST)
        available = min(pb.per_pos, state.cash * config.CASH_BUFFER)
        qty = int(available / buy_cost_total)

        if qty <= 0 or qty * buy_cost_total > state.cash:
            filled.append(pb)
            continue

        # Fill
        state.cash -= qty * buy_cost_total
        state.total_buy_amount += qty * entry_price
        state.positions[tk] = dict(
            ticker=tk,
            qty=qty,
            entry_price=entry_price,
            entry_idx=snapshot.day_idx,
            high_wm=entry_price,
            buy_cost_total=qty * entry_price * config.BUY_COST,
        )
        filled.append(pb)

        if state.strategy:
            state.strategy.on_fill(tk, entry_price, qty, snapshot.day_idx)

    for pb in filled:
        if pb in state.pending_buys:
            state.pending_buys.remove(pb)

    # Expire stale
    state.pending_buys = [
        pb for pb in state.pending_buys
        if pb.target_idx >= snapshot.day_idx
    ]


def process_exit_policy(state: StrategyState, snapshot: DailySnapshot,
                        config: LabConfig) -> None:
    """ExitPolicy 기반 종료 처리. close 기준 매도."""
    if not state.strategy:
        return

    for tk in list(state.positions.keys()):
        pos = state.positions[tk]
        pos_copy = {**pos, "ticker": tk}

        reason = state.strategy.exit_policy.check_exit(
            snapshot, pos_copy, state.strategy._state)

        # HWM 업데이트 반영 (exit_policy에서 수정됨)
        if "high_wm" in pos_copy:
            state.positions[tk]["high_wm"] = pos_copy["high_wm"]

        if reason:
            _close_position(state, tk, snapshot, config, reason)


def process_sell_signals(state: StrategyState, signals: List[Signal],
                         snapshot: DailySnapshot, config: LabConfig) -> None:
    """SELL 신호 처리. close 기준 매도."""
    for sig in signals:
        if sig.direction != "SELL":
            continue
        if sig.ticker in state.positions:
            _close_position(state, sig.ticker, snapshot, config, sig.reason)


def process_buy_signals(state: StrategyState, signals: List[Signal],
                        snapshot: DailySnapshot, config: LabConfig,
                        fill_timing: FillTiming, end_idx: int) -> None:
    """BUY 신호 처리. fill_timing에 따라 queue 또는 즉시 체결."""
    max_pos = state.strategy.config.max_positions if state.strategy else 20
    current_count = len(state.positions) + len(state.pending_buys)

    # 총 equity 계산
    pv = state.cash
    for tk, pos in state.positions.items():
        c = float(snapshot.close.get(tk, 0))
        if c > 0 and not pd.isna(c):
            pv += pos["qty"] * c
    per_pos = pv / max_pos if max_pos > 0 else 0

    buy_signals = sorted(
        [s for s in signals if s.direction == "BUY"],
        key=lambda s: -s.priority
    )

    for sig in buy_signals:
        if current_count >= max_pos:
            break
        if sig.ticker in state.positions:
            continue
        # 이미 pending에 있으면 skip
        if any(pb.ticker == sig.ticker for pb in state.pending_buys):
            continue

        if fill_timing == FillTiming.NEXT_OPEN:
            if snapshot.day_idx + 1 <= end_idx:
                state.pending_buys.append(PendingOrder(
                    ticker=sig.ticker,
                    target_idx=snapshot.day_idx + 1,
                    per_pos=per_pos,
                ))
                current_count += 1
        elif fill_timing == FillTiming.SAME_DAY_CLOSE:
            # Experimental: 당일 종가 체결
            _fill_at_close(state, sig.ticker, per_pos, snapshot, config)
            current_count += 1


def record_equity(state: StrategyState, snapshot: DailySnapshot) -> None:
    """Equity = cash + sum(qty * close)."""
    pv = state.cash
    for tk, pos in state.positions.items():
        c = float(snapshot.close.get(tk, 0))
        if c > 0 and not pd.isna(c):
            pv += pos["qty"] * c
    state.equity_history[snapshot.date] = pv


# ── Internal helpers ─────────────────────────────────────────────

def _close_position(state: StrategyState, ticker: str,
                    snapshot: DailySnapshot, config: LabConfig,
                    reason: str) -> None:
    """포지션 종료 (close 기준). BASELINE_SPEC: sell-then-buy."""
    if ticker not in state.positions:
        return
    pos = state.positions[ticker]
    p = float(snapshot.close.get(ticker, 0))
    if p <= 0 or pd.isna(p):
        p = pos["entry_price"]  # fallback

    net = pos["qty"] * p * (1 - config.SELL_COST)
    invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
    pnl = (net - invested) / invested if invested > 0 else 0

    state.cash += net
    state.total_sell_amount += pos["qty"] * p

    state.trades.append(dict(
        ticker=ticker,
        entry_date=str(snapshot.close_matrix.index[pos["entry_idx"]].date()),
        exit_date=str(snapshot.date.date()),
        entry_price=pos["entry_price"],
        exit_price=p,
        pnl_pct=pnl,
        pnl_amount=net - invested,
        hold_days=snapshot.day_idx - pos["entry_idx"],
        exit_reason=reason,
    ))

    if state.strategy:
        state.strategy.on_exit(ticker, p, reason, snapshot.day_idx)

    del state.positions[ticker]


def _fill_at_close(state: StrategyState, ticker: str, per_pos: float,
                   snapshot: DailySnapshot, config: LabConfig) -> None:
    """Same-day close 체결 (experimental only)."""
    p = float(snapshot.close.get(ticker, 0))
    if pd.isna(p) or p <= 0:
        return

    buy_cost_total = p * (1 + config.BUY_COST)
    available = min(per_pos, state.cash * config.CASH_BUFFER)
    qty = int(available / buy_cost_total)

    if qty <= 0 or qty * buy_cost_total > state.cash:
        return

    state.cash -= qty * buy_cost_total
    state.total_buy_amount += qty * p
    state.positions[ticker] = dict(
        ticker=ticker,
        qty=qty,
        entry_price=p,
        entry_idx=snapshot.day_idx,
        high_wm=p,
        buy_cost_total=qty * p * config.BUY_COST,
    )

    if state.strategy:
        state.strategy.on_fill(ticker, p, qty, snapshot.day_idx)
