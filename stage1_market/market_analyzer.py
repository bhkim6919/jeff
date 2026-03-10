"""
MarketAnalyzer
==============
KOSPI + KOSDAQ 듀얼 분석 → MarketState 결정.

스코어링 (각 0~1점, 합산 0~4점):
  1. MA 정배열   : ma5 > ma20 > ma60
  2. 거래량 추세 : 5일 평균 > 20일 평균
  3. 모멘텀      : 20일 수익률 > 0
  4. 변동성 안정 : ATR(14) <= 60일 평균 ATR

KOSPI / KOSDAQ 평균 점수:
  >= bull_threshold → BULL
  <= bear_threshold → BEAR
  나머지 → SIDEWAYS
"""

import logging
from typing import Dict

import pandas as pd

from stage1_market.market_state import MarketState
from data.kiwoom_provider import TrTimeoutError

logger = logging.getLogger(__name__)


class MarketAnalyzer:

    def __init__(self, provider, config):
        self.provider        = provider
        self.bull_threshold  = config.bull_threshold   # 예: 2.5
        self.bear_threshold  = config.bear_threshold   # 예: 1.5

    def analyze(self) -> MarketState:
        """
        KOSPI, KOSDAQ 지수 데이터를 바탕으로 시장 상태(BULL/SIDEWAYS/BEAR)를 판단.

        - 각 지수는 TrTimeoutError를 개별적으로 처리
        - 둘 다 실패하면 '보수적 SIDEWAYS'로 가정
        """
        scores: Dict[str, float] = {}
        errors: Dict[str, Exception] = {}

        # ── 1) KOSPI 점수 ────────────────────────────────────────────────
        try:
            kospi_score = self._score_index("KOSPI")
            scores["KOSPI"] = kospi_score
        except TrTimeoutError as e:
            logger.warning("[MarketAnalyzer] KOSPI 지수 TR 타임아웃: %s", e)
            errors["KOSPI"] = e
            kospi_score = None

        # ── 2) KOSDAQ 점수 ───────────────────────────────────────────────
        try:
            kosdaq_score = self._score_index("KOSDAQ")
            scores["KOSDAQ"] = kosdaq_score
        except TrTimeoutError as e:
            logger.warning("[MarketAnalyzer] KOSDAQ 지수 TR 타임아웃: %s", e)
            errors["KOSDAQ"] = e
            kosdaq_score = None

        # ── 3) 둘 다 실패한 경우 → 보수적으로 SIDEWAYS 가정 ─────────
        if not scores:
            logger.warning(
                "[MarketAnalyzer] KOSPI/KOSDAQ 지수 TR 모두 실패 — 시장 상태를 SIDEWAYS로 보수적으로 가정합니다."
            )
            print("[Stage1 WARNING] 지수 데이터 모두 실패 → 시장 상태: SIDEWAYS (보수적 가정)")
            return MarketState.SIDEWAYS

        # ── 4) 점수 로그 출력 ────────────────────────────────────────────
        if "KOSPI" in scores and "KOSDAQ" in scores:
            avg = (scores["KOSPI"] + scores["KOSDAQ"]) / 2.0
            print(
                f"[Stage1] KOSPI={scores['KOSPI']:.2f}  "
                f"KOSDAQ={scores['KOSDAQ']:.2f}  AVG={avg:.2f}"
            )
        else:
            # 한쪽 지수만 사용
            name = list(scores.keys())[0]
            avg  = scores[name]
            print(f"[Stage1] {name} 단독 사용 → score={avg:.2f}")

        # ── 5) 평균 점수 기준으로 MarketState 결정 ─────────────────────
        if avg >= self.bull_threshold:
            return MarketState.BULL
        if avg <= self.bear_threshold:
            return MarketState.BEAR
        return MarketState.SIDEWAYS

    # ── 내부 스코어링 ────────────────────────────────────────────────────

    def _score_index(self, index: str) -> float:
        """
        단일 지수(KOSPI 또는 KOSDAQ)에 대해 0~4점 스코어를 계산.

        TrTimeoutError는 상위(analyze)에서 개별 처리한다.
        """
        df = self.provider.get_index_ohlcv(index, days=60)

        if df is None or len(df) < 60:
            print(f"[Stage1] {index} 데이터 부족 ({len(df) if df is not None else 0}일) → 0점")
            return 0.0

        score = 0.0
        if self._is_ma_aligned(df):         score += 1.0
        if self._is_volume_rising(df):      score += 1.0
        if self._is_momentum_positive(df):  score += 1.0
        if self._is_volatility_stable(df):  score += 1.0
        return score

    def _is_ma_aligned(self, df: pd.DataFrame) -> bool:
        """
        ma5 > ma20 > ma60 이면 1점.
        """
        close = df["close"]
        ma5   = close.rolling(5).mean()
        ma20  = close.rolling(20).mean()
        ma60  = close.rolling(60).mean()

        if len(df) < 60:
            return False

        return bool(ma5.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1])

    def _is_volume_rising(self, df: pd.DataFrame) -> bool:
        """
        5일 평균 거래량 > 20일 평균 거래량이면 1점.
        """
        vol = df["volume"]
        if len(vol) < 20:
            return False

        v5  = vol.rolling(5).mean().iloc[-1]
        v20 = vol.rolling(20).mean().iloc[-1]
        return bool(v5 > v20)

    def _is_momentum_positive(self, df: pd.DataFrame) -> bool:
        """
        최근 20일 수익률 > 0 이면 1점.
        """
        close = df["close"]
        if len(close) < 20:
            return False

        ret20 = close.iloc[-1] / close.iloc[-20] - 1.0
        return bool(ret20 > 0)

    def _is_volatility_stable(self, df: pd.DataFrame) -> bool:
        """
        ATR(14) <= 60일 평균 ATR.
        ATR = max(high-low, |high-prev_close|, |low-prev_close|)의 14일 평균.
        """
        atr_series = self._calc_atr(df, period=14)
        if atr_series is None or len(atr_series) < 14:
            return False
        current_atr = atr_series.iloc[-1]
        avg_atr     = atr_series.mean()
        return current_atr <= avg_atr

    @staticmethod
    def _calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """True Range 계산 후 rolling mean."""
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        prev_close = close.shift(1)

        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        return tr.rolling(period).mean()