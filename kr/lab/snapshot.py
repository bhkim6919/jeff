"""
snapshot.py — DailySnapshot (frozen, immutable)
=================================================
모든 전략이 동일 snapshot을 공유. Look-ahead 2중 방어.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, FrozenSet, Optional

import pandas as pd


@dataclass(frozen=True)
class DailySnapshot:
    """하루 1회 생성, 모든 전략이 동일 인스턴스 사용."""
    date: pd.Timestamp
    day_idx: int
    # 당일 Series
    close: pd.Series        # ffilled close
    open: pd.Series         # NaN = 거래 불가
    high: pd.Series
    low: pd.Series
    volume: pd.Series
    # History up to today (inclusive) — look-ahead assert 적용
    close_matrix: pd.DataFrame
    volume_matrix: pd.DataFrame
    # Universe & metadata
    universe: FrozenSet[str]
    sector_map: Dict
    index_close: float
    index_series: pd.Series
    fundamental: Optional[pd.DataFrame]
    # OHLC matrices for HA filter (None = 하위호환, A군 전략은 사용 안 함)
    open_matrix: Optional[pd.DataFrame] = None
    high_matrix: Optional[pd.DataFrame] = None
    low_matrix: Optional[pd.DataFrame] = None

    def __post_init__(self):
        # Level 1: snapshot assert — close_matrix rows == day_idx + 1
        assert self.close_matrix.shape[0] == self.day_idx + 1, (
            f"Look-ahead violation: close_matrix has {self.close_matrix.shape[0]} rows "
            f"but day_idx={self.day_idx} (expected {self.day_idx + 1})"
        )


def safe_slice(matrix: pd.DataFrame, day_idx: int) -> pd.DataFrame:
    """Level 2: 지표 계산용 — 당일 제외 [:day_idx].

    당일 종가를 지표 계산에 포함하면 look-ahead에 가까우므로
    전략 내부에서 지표 계산 시 반드시 safe_slice 사용.
    """
    return matrix.iloc[:day_idx]


def build_snapshot(
    day_idx: int,
    dates: pd.Series,
    close: pd.DataFrame,
    opn: pd.DataFrame,
    high: pd.DataFrame,
    low: pd.DataFrame,
    vol: pd.DataFrame,
    universe: frozenset,
    sector_map: dict,
    idx_close_series: pd.Series,
    fundamental: Optional[pd.DataFrame] = None,
) -> DailySnapshot:
    """Build frozen daily snapshot. 하루 1회 호출."""
    dt = dates[day_idx]
    return DailySnapshot(
        date=dt,
        day_idx=day_idx,
        close=close.iloc[day_idx],
        open=opn.iloc[day_idx],
        high=high.iloc[day_idx],
        low=low.iloc[day_idx],
        volume=vol.iloc[day_idx],
        close_matrix=close.iloc[:day_idx + 1],
        volume_matrix=vol.iloc[:day_idx + 1],
        open_matrix=opn.iloc[:day_idx + 1],
        high_matrix=high.iloc[:day_idx + 1],
        low_matrix=low.iloc[:day_idx + 1],
        universe=universe,
        sector_map=sector_map,
        index_close=float(idx_close_series.iloc[day_idx])
            if day_idx < len(idx_close_series) else 0.0,
        index_series=idx_close_series.iloc[:day_idx + 1],
        fundamental=fundamental,
    )
