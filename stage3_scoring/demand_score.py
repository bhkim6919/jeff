"""
DemandScorer
============
수급 기반 서브스코어 (0.0 ~ 1.0).

구성 지표 (각 0.25점):
  1. 외국인 순매수  : 최근 5일 누적 순매수 > 0
  2. 기관 순매수    : 최근 5일 누적 순매수 > 0
  3. 외국인+기관 동반 : 둘 다 양수 (동반 매수 → 추가 0.25)
  4. 거래량 증가율  : 최근 5일 평균 > 20일 평균 * 1.2
"""

import pandas as pd
from core.data_provider import DataProvider


class DemandScorer:

    def __init__(self, provider: DataProvider):
        self.provider = provider

    def score(self, code: str) -> float:
        fi_df = self.provider.get_foreign_institution_data(code, days=20)
        if fi_df is None or len(fi_df) < 5:
            return 0.0

        ohlcv = self.provider.get_stock_ohlcv(code, days=20)
        if ohlcv is None or len(ohlcv) < 20:
            return 0.0

        score = 0.0
        foreign_pos    = self._foreign_net_positive(fi_df)
        institution_pos = self._institution_net_positive(fi_df)

        if foreign_pos:                          score += 0.25
        if institution_pos:                      score += 0.25
        if foreign_pos and institution_pos:      score += 0.25  # 동반 매수 보너스
        if self._volume_increasing(ohlcv):       score += 0.25

        return min(score, 1.0)

    # ── 지표별 판단 ──────────────────────────────────────────────────────────

    def _foreign_net_positive(self, df: pd.DataFrame) -> bool:
        """외국인 최근 5일 누적 순매수 > 0."""
        return float(df["foreign_net"].iloc[-5:].sum()) > 0

    def _institution_net_positive(self, df: pd.DataFrame) -> bool:
        """기관 최근 5일 누적 순매수 > 0."""
        return float(df["institution_net"].iloc[-5:].sum()) > 0

    def _volume_increasing(self, df: pd.DataFrame) -> bool:
        """최근 5일 평균 거래량 > 20일 평균 * 1.2."""
        vol    = df["volume"]
        avg5   = vol.iloc[-5:].mean()
        avg20  = vol.mean()
        return avg5 > avg20 * 1.2
