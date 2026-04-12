# -*- coding: utf-8 -*-
"""
calendar.py — Trading day calculation (핵심 인프라)
====================================================
pykrx 기반 거래일 캐시. 실패 시 weekday fallback + stale 표시.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

logger = logging.getLogger("gen4.regime.calendar")

_TRADING_DAYS_CACHE: Optional[List[date]] = None
_CACHE_STALE = False


def _load_trading_days() -> List[date]:
    """pykrx에서 최근 2년 거래일 조회. 실패 시 weekday fallback."""
    global _TRADING_DAYS_CACHE, _CACHE_STALE
    if _TRADING_DAYS_CACHE is not None:
        return _TRADING_DAYS_CACHE

    try:
        from pykrx import stock
        end = date.today()
        start = end - timedelta(days=800)
        days_str = stock.get_previous_business_days(
            fromdate=start.strftime("%Y%m%d"),
            todate=end.strftime("%Y%m%d"),
        )
        days = [d.date() if hasattr(d, 'date') else d for d in days_str]
        if len(days) > 100:
            _TRADING_DAYS_CACHE = sorted(days)
            _CACHE_STALE = False
            logger.info(f"[Calendar] Loaded {len(days)} trading days from pykrx")
            return _TRADING_DAYS_CACHE
    except Exception as e:
        logger.warning(f"[Calendar] pykrx failed: {e}, using weekday fallback")

    # Weekday fallback
    _CACHE_STALE = True
    days = []
    d = date.today() - timedelta(days=800)
    while d <= date.today() + timedelta(days=30):
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
        d += timedelta(days=1)
    _TRADING_DAYS_CACHE = days
    return _TRADING_DAYS_CACHE


def previous_trading_day(d: date) -> date:
    """d 직전 거래일 (d 포함 안 함)."""
    days = _load_trading_days()
    for td in reversed(days):
        if td < d:
            return td
    # Fallback: just go back to previous weekday
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def next_trading_day(d: date) -> date:
    """d 다음 거래일 (d 포함 안 함)."""
    days = _load_trading_days()
    for td in days:
        if td > d:
            return td
    # Fallback: next weekday
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def is_after_market_close(now: Optional[datetime] = None) -> bool:
    """15:30 KST 이후인지 판정."""
    if now is None:
        now = datetime.now()
    return now.hour > 15 or (now.hour == 15 and now.minute >= 30)


def is_calendar_stale() -> bool:
    """거래일 캐시가 weekday fallback인지."""
    _load_trading_days()
    return _CACHE_STALE
