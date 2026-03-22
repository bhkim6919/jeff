"""
QScoreEngine
============
종목별 Q-Score 계산. 배치 파이프라인(batch/qscore_pipeline.py)과
런타임 보조 스코어링 모두에서 사용한다.

각 서브스코어 : 0.0 ~ 1.0
최종 Q-Score : 0.0 ~ 1.0 (signals.csv 저장 형식)

가중치 테이블:
  BULL     : technical 50% / demand 25% / price 15% / alpha 10%
  SIDEWAYS : technical 30% / demand 25% / price 30% / alpha 15%
  BEAR     : technical 25% / demand 40% / price 20% / alpha 15%
"""

import shutil
import pandas as pd
from typing import List, Dict, Any

from strategy.regime_detector import MarketRegime


def _progress_bar(current: int, total: int, width: int = 40) -> str:
    filled = int(width * current / total) if total else 0
    bar    = "█" * filled + "░" * (width - filled)
    pct    = current / total * 100 if total else 0
    return f"\r[{bar}] {pct:5.1f}%  {current:4d}/{total}개"


WEIGHT_TABLE = {
    MarketRegime.BULL: {
        "technical": 0.50, "demand": 0.25, "price": 0.15, "alpha": 0.10,
    },
    MarketRegime.SIDEWAYS: {
        "technical": 0.30, "demand": 0.25, "price": 0.30, "alpha": 0.15,
    },
    MarketRegime.BEAR: {
        "technical": 0.25, "demand": 0.40, "price": 0.20, "alpha": 0.15,
    },
}


class QScoreEngine:

    def __init__(self, provider):
        self.provider    = provider
        self._ohlcv_cache: Dict[str, Any] = {}   # 배치 1회 실행 동안 유지

    def score(self, candidates: List[str], regime: MarketRegime) -> List[Dict[str, Any]]:
        """
        candidates : 종목코드 리스트
        반환: Q-Score 내림차순 정렬 dict 리스트
              각 항목: code, qscore(0~1), entry, tp, sl, sector, breakdown
        """
        self._ohlcv_cache = {}   # 배치 실행마다 초기화

        weights = WEIGHT_TABLE[regime]
        results = []
        total   = len(candidates)

        for idx, code in enumerate(candidates, 1):
            # OHLCV를 최대 필요량(252일)으로 1회만 조회 → 캐시 저장
            if code not in self._ohlcv_cache:
                self._ohlcv_cache[code] = self.provider.get_stock_ohlcv(code, days=252)

            technical = self._technical_score(code)
            demand    = self._demand_score(code)
            price_s   = self._price_score(code)
            alpha     = self._alpha_score(code)

            qscore = (
                technical * weights["technical"] +
                demand    * weights["demand"]    +
                price_s   * weights["price"]     +
                alpha     * weights["alpha"]
            )

            entry_price = self.provider.get_current_price(code)
            atr         = self._calc_atr(code)

            # TP/SL 사전 계산 (배치에서 미리 계산해 signals.csv에 저장)
            if atr > 0 and entry_price > 0:
                if regime == MarketRegime.BULL:
                    sl_mult = 4.0
                elif regime == MarketRegime.BEAR:
                    sl_mult = 1.0
                else:
                    sl_mult = 2.5
                sl = int(entry_price - atr * sl_mult)
                tp = int(entry_price + (entry_price - sl) * 2.0)
            else:
                sl, tp = 0, 0

            sector = self.provider.get_stock_info(code).get("sector", "기타")

            results.append({
                "code":    code,
                "qscore":  round(qscore, 4),
                "entry":   int(entry_price),
                "tp":      tp,
                "sl":      sl,
                "sector":  sector,
                "regime":  regime.value,
                "breakdown": {
                    "technical": round(technical, 3),
                    "demand":    round(demand, 3),
                    "price":     round(price_s, 3),
                    "alpha":     round(alpha, 3),
                },
            })

            print(_progress_bar(idx, total), end="", flush=True)

        print()  # 완료 후 줄바꿈
        ranked = sorted(results, key=lambda x: x["qscore"], reverse=True)
        print(f"[QScoreEngine] 계산 완료 → {len(ranked)}개")
        return ranked

    # ── 서브스코어 ─────────────────────────────────────────────────────────────

    def _technical_score(self, code: str) -> float:
        """MA 정배열 + 거래량 + 모멘텀 복합 점수."""
        try:
            df = self._ohlcv_cache.get(code)
            if df is None or len(df) < 20:
                return 0.0
            close = df["close"]
            score = 0.0
            # MA 정배열 (ma5 > ma20)
            ma5  = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            if ma5 > ma20:
                score += 0.4
            # 20일 모멘텀
            if len(close) >= 20 and close.iloc[-1] > close.iloc[-20]:
                score += 0.4
            # 거래량 추세
            vol = df["volume"]
            if len(vol) >= 20:
                if vol.rolling(5).mean().iloc[-1] > vol.rolling(20).mean().iloc[-1]:
                    score += 0.2
            return min(score, 1.0)
        except Exception:
            return 0.0

    def _demand_score(self, code: str) -> float:
        """수급 점수: 외인/기관 순매수 비율."""
        try:
            data = self.provider.get_investor_trend(code, days=5)
            if not data:
                return 0.0
            foreign   = data.get("foreign_net", 0)
            institute = data.get("institute_net", 0)
            total_vol = data.get("total_volume", 1)
            if total_vol == 0:
                return 0.0
            net_ratio = (foreign + institute) / total_vol
            return max(0.0, min(1.0, 0.5 + net_ratio * 5))
        except Exception:
            return 0.0

    def _price_score(self, code: str) -> float:
        """가격 위치 점수: 52주 신고가 근접도."""
        try:
            df = self._ohlcv_cache.get(code)
            if df is None or len(df) < 20:
                return 0.0
            high_52w = df["high"].max()
            current  = df["close"].iloc[-1]
            if high_52w == 0:
                return 0.0
            return round(current / high_52w, 4)
        except Exception:
            return 0.0

    def _alpha_score(self, code: str) -> float:
        """
        알파 점수 — RS 모멘텀 가속도 (단기 RS 가속 여부).

        계산 방식:
          rs20  = 최근 20일 수익률 순위 (0~1)
          rs60  = 최근 60일 수익률 순위 (0~1)
          가속도 = rs20 - rs60  → [-1, +1]
          정규화 → [0, 1]: 0.5 + 가속도 * 0.5

        의미: 최근 단기 RS가 중기보다 높으면 알파 상승 (모멘텀 가속 중).
        캐시된 OHLCV를 재사용하므로 추가 API 호출 없음.
        """
        try:
            df = self._ohlcv_cache.get(code)
            if df is None or len(df) < 61:
                return 0.5   # 데이터 부족 → 중립

            close = df["close"]
            ret20  = (close.iloc[-1] / close.iloc[-21] - 1.0) if len(close) >= 21 else 0.0
            ret60  = (close.iloc[-1] / close.iloc[-61] - 1.0) if len(close) >= 61 else 0.0

            # 전체 캐시에서 분위수 계산 (캐시 활용, universe 비교)
            all_ret20 = []
            all_ret60 = []
            for c, d in self._ohlcv_cache.items():
                if d is not None and len(d) >= 61:
                    cl = d["close"]
                    all_ret20.append(cl.iloc[-1] / cl.iloc[-21] - 1.0)
                    all_ret60.append(cl.iloc[-1] / cl.iloc[-61] - 1.0)

            if not all_ret20:
                return 0.5

            rank20 = sum(r < ret20 for r in all_ret20) / len(all_ret20)
            rank60 = sum(r < ret60 for r in all_ret60) / len(all_ret60)

            # 가속도 → [0, 1] 정규화
            accel = rank20 - rank60
            return float(min(1.0, max(0.0, 0.5 + accel * 0.5)))

        except Exception:
            return 0.5

    def _calc_atr(self, code: str, period: int = 14) -> float:
        try:
            df = self._ohlcv_cache.get(code)
            if df is None or len(df) < period + 1:
                return 0.0
            high  = df["high"]
            low   = df["low"]
            close = df["close"]
            prev  = close.shift(1)
            tr    = pd.concat([high - low,
                               (high - prev).abs(),
                               (low  - prev).abs()], axis=1).max(axis=1)
            atr   = tr.rolling(period).mean().iloc[-1]
            return float(atr) if not pd.isna(atr) else 0.0
        except Exception:
            return 0.0
