"""
PositionSizer
=============
종목별 진입 금액 산정.

방식: Q-Score 비례 배분
  - 투자 가능 금액 = 총 현금 * max_exposure (최대 60%)
  - 종목별 비중 = 해당 종목 Q-Score / 전체 후보 Q-Score 합계
  - 단, 종목당 상한 = 총자산 * max_per_stock (최대 20%)
  - 최소 진입 금액 이하면 제외 (슬리피지 방지)

Config 연동:
  max_exposure  : 0.60 (총 노출도 60%)
  max_per_stock : 0.20 (종목당 20%)
  max_positions : 4    (최대 보유 종목)
"""

from config import QTronConfig


MIN_ENTRY_AMOUNT = 300_000   # 최소 진입 금액 30만원


class PositionSizer:

    def __init__(self, config: QTronConfig):
        self.config = config

    def allocate(
        self,
        scored: list[dict],
        available_cash: float,
        total_asset: float,
    ) -> list[dict]:
        """
        scored        : QScorer.score() 반환값 (Q-Score 내림차순)
        available_cash: 현재 사용 가능 현금
        total_asset   : 총 평가금액 (현금 + 보유 종목 평가액)

        반환: [{"code": ..., "q_score": ..., "amount": ..., "tp": ..., "sl": ...}, ...]
        """
        # 상위 max_positions 종목만 대상
        candidates = scored[:self.config.max_positions]

        # 투자 가능 총액
        investable = min(
            available_cash,
            total_asset * self.config.max_exposure,
        )

        # Q-Score 합계
        total_score = sum(c["q_score"] for c in candidates)
        if total_score == 0:
            return []

        # 종목당 상한
        per_stock_cap = total_asset * self.config.max_per_stock

        result = []
        for item in candidates:
            # 비례 배분
            ratio  = item["q_score"] / total_score
            amount = investable * ratio

            # 상한 적용
            amount = min(amount, per_stock_cap)

            # 최소 금액 미달 제외
            if amount < MIN_ENTRY_AMOUNT:
                continue

            result.append({
                "code":    item["code"],
                "q_score": item["q_score"],
                "amount":  int(amount),   # 원 단위 정수
                "breakdown": item["breakdown"],
                "market_state": item["market_state"],
            })

        return result
