"""
QScorePipeline
==============
배치 시간대(18:00 ~ 20:30)에 유니버스 전체 종목 Q-Score를 계산한다.

기술 지표 + 수급 분석 + 레짐 정보를 결합하여
signal_generator.py 에 넘길 scored list를 반환.
"""

from typing import List, Dict, Any

from strategy.qscore_engine import QScoreEngine
from strategy.regime_detector import MarketRegime


class QScorePipeline:

    def __init__(self, provider, config):
        self.provider = provider
        self.engine   = QScoreEngine(provider)
        self.config   = config

    def run(self, universe: List[str], regime: MarketRegime) -> List[Dict[str, Any]]:
        """
        universe : UniverseBuilder.build() 결과
        regime   : 전일 기준 MarketRegime (배치 시점 계산)
        반환     : Q-Score 내림차순 정렬 리스트
        """
        print(f"[QScorePipeline] {len(universe)}개 종목 Q-Score 계산 시작 (레짐: {regime.value})")
        scored = self.engine.score(universe, regime)

        # 최소 Q-Score 필터 (0.3 미만 제외 — 의미 있는 신호만)
        filtered = [s for s in scored if s["qscore"] >= 0.30]
        print(f"[QScorePipeline] qscore >= 0.30 필터 후: {len(filtered)}개")

        return filtered
