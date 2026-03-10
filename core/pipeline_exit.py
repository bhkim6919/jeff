from config import QTronConfig
from core.portfolio import Portfolio
from core.data_provider import DataProvider
from stage5_execution.execution_engine import ExecutionEngine, Order
from stage5_execution.order_manager import OrderManager
from stage5_execution.trade_logger import TradeLogger
from stage6_risk_governor.risk_governor import RiskGovernor
from stage1_market.market_analyzer import MarketAnalyzer
from stage2_filter.stock_filter import StockFilter
from stage3_scoring.q_score import QScorer
from stage4_risk.risk_manager import RiskManager
from stage7_report.reporter import Reporter
from stage7_report.dashboard import open_latest_report


class QTronPipeline:
    def __init__(self, config: QTronConfig, provider: DataProvider):
        self.config    = config
        self.provider  = provider
        self.portfolio = Portfolio(config)
        self.engine    = ExecutionEngine(provider, self.portfolio, config.paper_trading)
        self.governor  = RiskGovernor(self.portfolio, self.engine)
        self.market_analyzer = MarketAnalyzer(provider, config)
        self.stock_filter    = StockFilter(provider)
        self.q_scorer        = QScorer(provider)
        self.risk_manager    = RiskManager(provider, config)
        self.order_manager   = OrderManager(self.engine, provider)
        self.trade_logger    = TradeLogger()
        self.reporter        = Reporter()

    def run(self) -> dict:
        # 0. Risk Governor — 최우선 체크
        mode = self.governor.evaluate()

        if mode == "HARD_STOP":
            return {
                "status": "HARD_STOP",
                "message": "월 DD 한도 초과. 전 포지션 청산 완료.",
                "portfolio": self.portfolio.summary()
            }

        # 1. 시장 분석
        market_state = self.market_analyzer.analyze()
        print(f"[Stage1] 시장 상태: {market_state.value}")

        if mode == "SOFT_STOP":
            return {
                "status": "SOFT_STOP",
                "message": "일 손실 한도 초과. 신규 진입 없음.",
                "portfolio": self.portfolio.summary()
            }

        # 2. 종목 필터
        candidates = self.stock_filter.filter(market_state)
        print(f"[Stage2] 후보 종목: {candidates}")

        # 3. Q-Score
        scored = self.q_scorer.score(candidates, market_state)

        # 4. TP/SL 설정
        positioned = self.risk_manager.apply(
            scored,
            market_state,
            available_cash=self.portfolio.cash,
            total_asset=self.portfolio.get_current_equity(),
        )

        # 4.5 포지션에 TP/SL 계획 등록
        for item in positioned:
            self.portfolio.register_plan(
                code=item["code"],
                tp=item["tp"],
                sl=item["sl"],
                q_score=item["q_score"],
                rr=item["rr_ratio"],
            )

        # 5. 실행
        print("[Stage5] 주문 실행 시작")
        results = self.order_manager.execute_all(positioned)
        self.trade_logger.log_all(results)

        # 6. 일일 리포트
        self.reporter.report_daily(
            results           = results,
            positioned        = positioned,
            market_state      = market_state,
            candidates        = candidates,
            scored            = scored,
            portfolio_summary = self.portfolio.summary(),
        )

        accepted = [r for r in results if not r.rejected]
        return {
            "status":    "NORMAL",
            "message":   f"전 스테이지 완료 — 체결: {len(accepted)}건 / 시장: {market_state.value}",
            "portfolio": self.portfolio.summary(),
        }

    def end_of_day(self, open_browser: bool = False):
        """
        장 종료 후 반드시 호출.
        - 포트폴리오 일일 기준가 갱신
        - 오늘 날짜 기준 해당 기간 리포트 자동 생성
          (금요일→주간, 월말→월간, 분기말→분기, 반기말→반기, 연말→연간)
        """
        self.portfolio.end_of_day_update()
        print("[EOD] 일일 기준가 업데이트 완료")
        print("[EOD]", self.portfolio.summary())

        # 기간별 리포트 자동 트리거
        self.reporter.auto_period_reports(open_browser=open_browser)

        if open_browser:
            open_latest_report()
