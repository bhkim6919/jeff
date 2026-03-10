"""
TechnicalScorer
===============
기술적 지표 기반 서브스코어 (0.0 ~ 1.0).

구성 (합산 후 0~1 클리핑):
  MACD > Signal          → +0.35  (추세 방향)
  RSI 40~65              → +0.35  (상승 초입, 과열 아님)
  종가 > 볼린저 중심선   → +0.30  (중심 위 위치)
  볼린저 상단 돌파       → -0.20  (과열 구간 감점)

설계 의도: "상승 초입 + 과열 아님" 구조
  - MACD 골든크로스/유지 + RSI 중립~상승 구간 + 중심선 위
  - 단, 볼린저 상단을 돌파한 과열 구간은 감점으로 억제
"""

import pandas as pd
from core.data_provider import DataProvider


class TechnicalScorer:

    def __init__(self, provider: DataProvider):
        self.provider = provider

    def score(self, code: str) -> float:
        df = self.provider.get_stock_ohlcv(code, days=60)
        if df is None or len(df) < 30:
            return 0.0

        score = 0.0

        if self._macd_bullish(df):          score += 0.35
        if self._rsi_healthy(df):           score += 0.35
        if self._above_bb_middle(df):       score += 0.30
        if self._bb_upper_breakout(df):     score -= 0.20   # 과열 감점

        return max(0.0, min(1.0, score))

    # ── 지표별 판단 ──────────────────────────────────────────────────────────

    def _macd_bullish(self, df: pd.DataFrame) -> bool:
        """MACD > Signal Line (골든크로스 또는 유지)."""
        close  = df["close"]
        ema12  = close.ewm(span=12, adjust=False).mean()
        ema26  = close.ewm(span=26, adjust=False).mean()
        macd   = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        return float(macd.iloc[-1]) > float(signal.iloc[-1])

    def _rsi_healthy(self, df: pd.DataFrame) -> bool:
        """RSI(14) 40~65 구간: 상승 초입, 과열 아님."""
        rsi = self._calc_rsi(df["close"], period=14)
        if rsi is None:
            return False
        return 40.0 <= rsi <= 65.0

    def _above_bb_middle(self, df: pd.DataFrame) -> bool:
        """종가 > 볼린저 중심선(MA20)."""
        close = df["close"]
        ma20  = close.rolling(20).mean()
        return float(close.iloc[-1]) > float(ma20.iloc[-1])

    def _bb_upper_breakout(self, df: pd.DataFrame) -> bool:
        """볼린저 상단 돌파 여부 (과열 감점 트리거)."""
        close = df["close"]
        ma20  = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper = ma20 + 2 * std20
        return float(close.iloc[-1]) > float(upper.iloc[-1])

    # ── 계산 헬퍼 ────────────────────────────────────────────────────────────

    @staticmethod
    def _calc_rsi(close: pd.Series, period: int = 14):
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        if loss.iloc[-1] == 0:
            return 100.0
        rs  = gain.iloc[-1] / loss.iloc[-1]
        rsi = 100 - (100 / (1 + rs))
        return float(rsi) if not pd.isna(rsi) else None
