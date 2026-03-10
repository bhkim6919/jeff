"""
PriceScorer
===========
가격 구조 기반 서브스코어 (0.0 ~ 1.0).

구성 지표 (각 0.25점):
  1. MA 정배열 강도     : ma5 > ma20 > ma60 (3단계 모두 충족)
  2. 52주 신고가 근접도 : 현재가 >= 52주 고가 * 0.85
  3. 저항 돌파          : 최근 20일 고가(전고점) 상향 돌파
  4. 지지선 유지        : 현재가 >= MA20 * 0.97 (지지선 이탈 아님)
"""

import pandas as pd
from core.data_provider import DataProvider


class PriceScorer:

    def __init__(self, provider: DataProvider):
        self.provider = provider

    def score(self, code: str) -> float:
        df = self.provider.get_stock_ohlcv(code, days=120)
        if df is None or len(df) < 60:
            return 0.0

        score = 0.0
        if self._ma_alignment(df):        score += 0.25
        if self._near_52w_high(df):       score += 0.25
        if self._resistance_breakout(df): score += 0.25
        if self._above_support(df):       score += 0.25
        return score

    # ── 지표별 판단 ──────────────────────────────────────────────────────────

    def _ma_alignment(self, df: pd.DataFrame) -> bool:
        """MA 정배열: ma5 > ma20 > ma60."""
        close = df["close"]
        ma5   = close.rolling(5).mean().iloc[-1]
        ma20  = close.rolling(20).mean().iloc[-1]
        ma60  = close.rolling(60).mean().iloc[-1]
        return ma5 > ma20 > ma60

    def _near_52w_high(self, df: pd.DataFrame) -> bool:
        """현재가 >= 52주(120일) 고가 * 0.85."""
        high_52w = df["high"].max()
        current  = float(df["close"].iloc[-1])
        return current >= high_52w * 0.85

    def _resistance_breakout(self, df: pd.DataFrame) -> bool:
        """
        저항 돌파: 현재가 > 직전 20일(D-1 ~ D-20) 최고가.
        오늘 종가가 이전 20일간의 고점을 넘으면 돌파로 판단.
        """
        if len(df) < 22:
            return False
        prev_high = df["high"].iloc[-21:-1].max()   # D-1 ~ D-20
        current   = float(df["close"].iloc[-1])
        return current > prev_high

    def _above_support(self, df: pd.DataFrame) -> bool:
        """지지선 유지: 현재가 >= MA20 * 0.97."""
        close = df["close"]
        ma20  = float(close.rolling(20).mean().iloc[-1])
        curr  = float(close.iloc[-1])
        return curr >= ma20 * 0.97
