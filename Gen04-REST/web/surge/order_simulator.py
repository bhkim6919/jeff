# -*- coding: utf-8 -*-
"""
order_simulator.py -- Conservative Fill Simulation
=====================================================
보수적 체결 가정: ask+슬리피지 매수, bid-슬리피지 매도.
결정론적 모드 지원 (REPLAY 재현 가능).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from web.surge.config import SurgeConfig
from web.surge.signal_rules import get_time_slippage


@dataclass
class SimFill:
    code: str
    side: str               # "BUY" or "SELL"
    requested_qty: int
    fill_qty: int
    fill_price: int          # 체결 가격 (슬리피지 반영)
    market_price: int        # 원래 시장 가격 (ask or bid)
    slippage_pct: float      # 적용된 슬리피지 %
    fee: float               # 수수료
    tax: float               # 세금 (매도만)
    total_cost: float        # fee + tax
    timestamp: float


def simulate_buy(
    code: str,
    qty: int,
    snapshot: dict,
    now: float,
    config: SurgeConfig,
) -> Optional[SimFill]:
    """
    보수적 매수 체결 시뮬레이션.

    Returns None if fill impossible (depth insufficient).
    """
    ask = snapshot.get("ask", 0)
    ask_size = snapshot.get("ask_size", 0)

    if ask <= 0:
        return None

    # Fill safety: ask_size >= qty * K
    required = qty * config.fill_safety_k
    if ask_size < required:
        return None

    # Slippage
    slip = get_time_slippage(now, config)
    fill_price = math.ceil(ask * (1 + slip))

    # Fee
    fee = fill_price * qty * config.fee_rate

    return SimFill(
        code=code,
        side="BUY",
        requested_qty=qty,
        fill_qty=qty,
        fill_price=fill_price,
        market_price=ask,
        slippage_pct=slip * 100,
        fee=round(fee, 2),
        tax=0.0,
        total_cost=round(fee, 2),
        timestamp=now,
    )


def simulate_sell(
    code: str,
    qty: int,
    snapshot: dict,
    now: float,
    config: SurgeConfig,
) -> Optional[SimFill]:
    """
    보수적 매도 체결 시뮬레이션.
    매도는 항상 체결 가능 가정 (시뮬레이터).
    """
    bid = snapshot.get("bid", 0)
    if bid <= 0:
        # fallback: 현재가 사용
        bid = snapshot.get("price", 0)
    if bid <= 0:
        return None

    # Slippage
    slip = get_time_slippage(now, config)
    fill_price = math.floor(bid * (1 - slip))
    if fill_price <= 0:
        fill_price = 1

    # Fee + Tax
    fee = fill_price * qty * config.fee_rate
    tax = fill_price * qty * config.tax_rate

    return SimFill(
        code=code,
        side="SELL",
        requested_qty=qty,
        fill_qty=qty,
        fill_price=fill_price,
        market_price=bid,
        slippage_pct=slip * 100,
        fee=round(fee, 2),
        tax=round(tax, 2),
        total_cost=round(fee + tax, 2),
        timestamp=now,
    )
