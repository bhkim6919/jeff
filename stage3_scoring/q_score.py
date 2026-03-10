"""
QScorer
=======
종목별 Q-Score 계산.
시장 상태에 따라 가중치가 동적으로 변경된다.

각 서브스코어 : 0.0 ~ 1.0
최종 Q-Score : 0.0 ~ 100.0

가중치 테이블:
  BULL     : technical 50% / demand 25% / price 15% / alpha 10%
  SIDEWAYS : technical 30% / demand 25% / price 30% / alpha 15%
  BEAR     : 호출되지 않음 (Stage2에서 차단)
"""

from stage1_market.market_state import MarketState
from stage3_scoring.technical_score import TechnicalScorer
from stage3_scoring.demand_score import DemandScorer
from stage3_scoring.price_score import PriceScorer
from core.data_provider import DataProvider


WEIGHT_TABLE = {
    MarketState.BULL: {
        "technical": 0.50,
        "demand":    0.25,
        "price":     0.15,
        "alpha":     0.10,
    },
    MarketState.BEAR: {
        "technical": 0.25,
        "demand":    0.40,
        "price":     0.20,
        "alpha":     0.15,
    },
    MarketState.SIDEWAYS: {
        "technical": 0.30,
        "demand":    0.25,
        "price":     0.30,
        "alpha":     0.15,
    },
}


class QScorer:

    def __init__(self, provider: DataProvider):
        self.technical_scorer = TechnicalScorer(provider)
        self.demand_scorer    = DemandScorer(provider)
        self.price_scorer     = PriceScorer(provider)

    def score(self, candidates: list[str], market_state: MarketState) -> list[dict]:
        """
        candidates  : 종목 코드 리스트
        반환        : Q-Score 내림차순 정렬된 dict 리스트
        """
        weights = WEIGHT_TABLE[market_state]
        results = []

        for code in candidates:
            technical = self.technical_scorer.score(code)
            demand    = self.demand_scorer.score(code)
            price     = self.price_scorer.score(code)
            alpha     = self._get_alpha_score(code)

            q_score = (
                technical * weights["technical"] +
                demand    * weights["demand"]    +
                price     * weights["price"]     +
                alpha     * weights["alpha"]
            ) * 100

            results.append({
                "code":      code,
                "q_score":   round(q_score, 2),
                "breakdown": {
                    "technical": round(technical, 3),
                    "demand":    round(demand, 3),
                    "price":     round(price, 3),
                    "alpha":     round(alpha, 3),
                },
                "weights":      weights,
                "market_state": market_state.value,
            })

            print(f"  [{code}] Q={q_score:.1f} "
                  f"(T={technical:.2f} D={demand:.2f} P={price:.2f} A={alpha:.2f})")

        ranked = sorted(results, key=lambda x: x["q_score"], reverse=True)
        print(f"[Stage3] Q-Score 계산 완료 → {len(ranked)}개")
        return ranked

    def _get_alpha_score(self, code: str) -> float:
        # TODO: 공매도 잔고 비율, 뉴스 센티먼트 등
        return 0.0
