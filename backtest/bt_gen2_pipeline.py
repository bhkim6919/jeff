# backtest/bt_gen2_pipeline.py
"""
Gen2 Core v1.0 (전략4) 백테스트용 파이프라인 래퍼

- Stage1: MarketAnalyzer → 시장 상태(BULL/SIDEWAYS/BEAR)
- Stage2: StockFilter     → 후보 종목 리스트
- Stage3: QScorer         → Q-Score / 엔트리 가격 등 스코어링
- Stage4: RiskManager     → TP/SL, 포지션 사이즈 결정

실제 주문/체결은 backtest.bt_engine.BacktestEngine 이 처리하므로
여기서는 'positioned' 리스트만 만들어서 돌려줍니다.
"""

from __future__ import annotations

from typing import List, Dict, Any

from config import QTronConfig
from core.data_provider import DataProvider
from stage1_market.market_analyzer import MarketAnalyzer
from stage2_filter.stock_filter import StockFilter
from stage3_scoring.q_score import QScorer
from stage4_risk.risk_manager import RiskManager
from stage1_market.market_state import MarketState


class Gen2BacktestPipeline:
    """
    BacktestEngine 에서 사용하기 위한 간단 래퍼.

    생성자 시그니처는 bt_engine.BacktestEngine 에서 기대하는 형태:
        pipeline = pipeline_factory(provider, qtron_config)
    """

    def __init__(self, provider: DataProvider, config: QTronConfig):
        self.provider = provider
        self.config   = config

        # 라이브용 Stage 모듈 재사용 (동일 로직, 데이터 소스만 CSV)
        self.market_analyzer = MarketAnalyzer(provider, config)
        self.stock_filter    = StockFilter(provider)
        self.q_scorer        = QScorer(provider)
        self.risk_manager    = RiskManager(provider, config)

    def run(self, available_cash: float, total_asset: float) -> Dict[str, Any]:
        """
        BacktestEngine 에서 호출되는 엔트리 포인트.

        반환 형식:
          {
            "status": "OK" | "NO_CANDIDATES" | "ERROR" | ...,
            "message": "...",
            "positioned": [ {code, tp, sl, size, ...}, ... ]
          }
        """
        try:
            # ── Stage1: 시장 상태 분석 ─────────────────────────────
            market_state: MarketState = self.market_analyzer.analyze()
            print(f"[BT-Stage1] 시장 상태: {market_state.value}")

            # ── Stage2: 종목 필터 ─────────────────────────────────
            candidates = self.stock_filter.filter(market_state)
            print(f"[BT-Stage2] 필터 통과 종목수: {len(candidates)}")

            if not candidates:
                return {
                    "status": "NO_CANDIDATES",
                    "message": "필터 통과 종목 없음.",
                    "positioned": [],
                }

            # ── Stage3: Q-Score 스코어링 ──────────────────────────
            scored = self.q_scorer.score(candidates, market_state)
            print(f"[BT-Stage3] 스코어링 완료 종목수: {len(scored)}")

            if not scored:
                return {
                    "status": "NO_SCORED",
                    "message": "스코어링 결과 없음.",
                    "positioned": [],
                }

            # ── Stage4: RiskManager 로 TP/SL + 사이징 ──────────────
            positioned: List[Dict[str, Any]] = self.risk_manager.apply(
                scored,
                market_state,
                available_cash=available_cash,
                total_asset=total_asset,
            )
            print(f"[BT-Stage4] 포지션 후보 최종 확정: {len(positioned)}개")

            return {
                "status": "OK",
                "message": f"{len(positioned)}개 종목 포지션 생성",
                "positioned": positioned,
            }

        except Exception as e:
            import traceback
            print("[Gen2BacktestPipeline ERROR]")
            print(traceback.format_exc())
            return {
                "status": "ERROR",
                "message": f"{type(e).__name__}: {e}",
                "positioned": [],
            }