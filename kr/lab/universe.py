"""
universe.py — 공통 유니버스 빌더
=================================
backtester.get_universe()와 동일 로직. frozenset 반환.
"""
from __future__ import annotations

import pandas as pd


def build_universe(
    close: pd.DataFrame,
    vol: pd.DataFrame,
    day_idx: int,
    min_close: int = 2000,
    min_amount: float = 2e9,
) -> frozenset:
    """day_idx 시점 거래 가능 유니버스 반환.

    조건:
      - close >= min_close (2000원)
      - 20일 평균 거래대금 >= min_amount (20억)
      - close > 0
    """
    if day_idx < 20:
        return frozenset()
    c = close.iloc[day_idx]
    amt = (close.iloc[max(0, day_idx - 19):day_idx + 1]
           * vol.iloc[max(0, day_idx - 19):day_idx + 1]).mean()
    ok = (c >= min_close) & (amt >= min_amount) & (c > 0)
    return frozenset(ok[ok].index.tolist())
