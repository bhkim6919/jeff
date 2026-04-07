# -*- coding: utf-8 -*-
"""
signal_rules.py -- Entry / Exit Condition Evaluators (pure functions)
======================================================================
상태 없음. 모든 판단은 전달받은 snapshot + config 기반.
"""
from __future__ import annotations

import math
from datetime import datetime, time as dt_time
from typing import Tuple

from web.surge.config import SurgeConfig

# Market close: 15:15 이후 강제 청산 판단 시작
FORCE_EXIT_TIME = dt_time(15, 15)


def check_entry(
    candidate_tr_ts: float,
    snap: dict,
    qty: int,
    config: SurgeConfig,
    now: float,
) -> Tuple[bool, str]:
    """
    진입 조건 종합 검사.

    Args:
        candidate_tr_ts: candidate가 TR에서 수신된 시각 (time.time())
        snap: frozen price snapshot {price, bid, ask, bid_size, ask_size, ts}
        qty: 주문 수량
        config: SurgeConfig
        now: time.time()

    Returns:
        (passed, reason_str)
    """
    # 1. Stale TR guard
    tr_age = now - candidate_tr_ts
    if tr_age > config.max_tr_lag_sec:
        return False, f"STALE_TR({tr_age:.1f}s)"

    # 2. 시간대 차단
    now_dt = datetime.fromtimestamp(now)
    now_hhmm = now_dt.strftime("%H:%M")
    for start, end in config.blocked_periods:
        if start <= now_hhmm < end:
            return False, f"BLOCKED_PERIOD({start}-{end})"

    # 3. 가격 필터
    price = snap.get("price", 0)
    if price < config.min_price:
        return False, f"PRICE_FILTER({price}<{config.min_price})"

    # 4. ask 존재 여부
    ask = snap.get("ask", 0)
    if ask <= 0:
        return False, "NO_ASK"

    # 5. 체결 안전계수: ask_size >= qty * K
    ask_size = snap.get("ask_size", 0)
    required = qty * config.fill_safety_k
    if ask_size < required:
        return False, f"INSUFFICIENT_DEPTH(ask_size={ask_size}<{required:.0f})"

    # 6. Hoga stale guard
    snap_ts = snap.get("ts_epoch", now)
    hoga_age = now - snap_ts
    if hoga_age > config.max_hoga_stale_sec:
        return False, f"STALE_HOGA({hoga_age:.1f}s)"

    return True, "PASS"


def check_tp(entry_fill_price: int, snap_bid: int, config: SurgeConfig) -> bool:
    """TP 판정: bid 기준."""
    if entry_fill_price <= 0 or snap_bid <= 0:
        return False
    pnl_pct = (snap_bid / entry_fill_price - 1) * 100
    return pnl_pct >= config.tp_pct


def check_sl(entry_fill_price: int, snap_bid: int, config: SurgeConfig) -> bool:
    """SL 판정: bid 기준."""
    if entry_fill_price <= 0 or snap_bid <= 0:
        return False
    pnl_pct = (snap_bid / entry_fill_price - 1) * 100
    return pnl_pct <= -config.sl_pct


def check_time_exit(entry_ts: float, now: float, config: SurgeConfig) -> bool:
    """최대 보유 시간 초과."""
    return (now - entry_ts) >= config.max_hold_sec


def check_force_exit(now: float) -> bool:
    """장 종료 전 강제 청산 (15:15 이후)."""
    now_dt = datetime.fromtimestamp(now)
    return now_dt.time() >= FORCE_EXIT_TIME


def get_time_slippage(now: float, config: SurgeConfig) -> float:
    """시간대별 슬리피지 조회. 매칭 구간 없으면 최소값 사용."""
    now_hhmm = datetime.fromtimestamp(now).strftime("%H:%M")
    for start, end, rate in config.slippage_schedule:
        if start <= now_hhmm < end:
            return rate
    # fallback: 정상 구간 슬리피지
    return 0.0015


def calc_buy_fill_price(ask_price: int, now: float, config: SurgeConfig) -> int:
    """매수 체결 시뮬레이션 가격: ask + 슬리피지 (올림)."""
    slip = get_time_slippage(now, config)
    return math.ceil(ask_price * (1 + slip))


def calc_sell_fill_price(bid_price: int, now: float, config: SurgeConfig) -> int:
    """매도 체결 시뮬레이션 가격: bid - 슬리피지 (내림)."""
    slip = get_time_slippage(now, config)
    return math.floor(bid_price * (1 - slip))


def calc_pnl_pct(
    entry_fill_price: int,
    exit_fill_price: int,
    config: SurgeConfig,
) -> Tuple[float, float]:
    """
    PnL 계산 (gross, net).

    Returns:
        (gross_pnl_pct, net_pnl_pct)
    """
    if entry_fill_price <= 0:
        return 0.0, 0.0
    gross = (exit_fill_price / entry_fill_price - 1) * 100
    cost = (config.fee_rate * 2 + config.tax_rate) * 100  # % 단위
    net = gross - cost
    return round(gross, 4), round(net, 4)
