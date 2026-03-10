from config import QTronConfig
from core.portfolio import Portfolio
from core.data_provider import DataProvider
from core.early_entry_layer import EarlyEntryLayer, load_sector_map
from stage5_execution.execution_engine import ExecutionEngine, Order
from stage5_execution.order_manager import OrderManager
from stage5_execution.trade_logger import TradeLogger
from stage6_risk_governor.risk_governor import RiskGovernor
from stage4_risk.stop_manager import StopManager
from stage1_market.market_analyzer import MarketAnalyzer
from stage2_filter.stock_filter import StockFilter
from stage3_scoring.q_score import QScorer
from stage4_risk.risk_manager import RiskManager
from stage7_report.reporter import Reporter
from stage7_report.dashboard import open_latest_report
from datetime import datetime, time as dtime
import os

# ⬇️ TR 타임아웃 전용 예외
from data.kiwoom_provider import TrTimeoutError


def _is_market_open() -> bool:
    """
    한국 주식시장 장중 여부 판별.
      - 평일(월~금) 09:00 ~ 15:30 만 True
      - 주말 및 장 외 시간은 False
    공휴일은 별도 처리하지 않음 (키움 TR이 빈 데이터 반환하므로 자연 처리됨)
    """
    now = datetime.now()
    if now.weekday() >= 5:          # 토(5), 일(6)
        return False
    t = now.time()
    return dtime(9, 0) <= t <= dtime(15, 30)


class QTronPipeline:
    def __init__(self, config: QTronConfig, provider: DataProvider,
                 skip_market_hours: bool = False):
        self.config             = config
        self.provider           = provider
        self.skip_market_hours  = skip_market_hours  # True → 장외 체크 우회 (pykrx 테스트용)
        self.portfolio = Portfolio(config)
        self.engine    = ExecutionEngine(provider, self.portfolio, config.paper_trading)
        self.governor  = RiskGovernor(self.portfolio, self.engine)
        self.trade_logger    = TradeLogger()
        # ⬇️ StopManager에 config 추가
        self.stop_manager    = StopManager(provider, self.engine, self.portfolio, self.trade_logger, config)
        self.market_analyzer = MarketAnalyzer(provider, config)
        self.stock_filter    = StockFilter(provider)
        self.q_scorer        = QScorer(provider)
        self.risk_manager    = RiskManager(provider, config)
        self.order_manager   = OrderManager(self.engine, provider)
        self.reporter        = Reporter()

        # ⬇️ Early Entry 레이어 초기화
        _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sector_map_path = getattr(config, "sector_map_path", "data/sector_map.json")
        if not os.path.isabs(sector_map_path):
            sector_map_path = os.path.join(_base, sector_map_path)
        if os.path.exists(sector_map_path):
            sector_map = load_sector_map(sector_map_path)
            self.early_layer = EarlyEntryLayer(
                provider=provider,
                sector_map=sector_map,
                output_dir=getattr(config, "early_signal_dir", "data/early_signals"),
                db_path=getattr(config, "early_signal_db",  "data/early_signals.db"),
                sector_cap=getattr(config, "sector_cap",    4),
            )
            print("[Pipeline] Early Entry 레이어 초기화 완료")
        else:
            self.early_layer = None
            print(f"[Pipeline] sector_map 없음 ({sector_map_path}) → Early Entry 비활성")

    def run(self) -> dict:
        try:
            # ── 장중 여부 확인 (최우선 — TR 호출 전에 체크) ─────────────────
            if not self.skip_market_hours and not _is_market_open():
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                wday = ["월","화","수","목","금","토","일"][datetime.now().weekday()]
                print(f"[Pipeline] 장 외 시간 ({now_str} {wday}) — TR 호출 없이 종료")
                return {
                    "status":    "MARKET_CLOSED",
                    "message":   f"장 외 시간 ({now_str} {wday}). 파이프라인 스킵.",
                    "portfolio": self.portfolio.summary()
                }

            # ── Stage 0: Risk Governor (최우선) ──────────────────────────────
            mode = self.governor.evaluate()

            if mode == "HARD_STOP":
                return {
                    "status":    "HARD_STOP",
                    "message":   "월 DD 한도 초과. 전 포지션 청산 완료.",
                    "portfolio": self.portfolio.summary()
                }

            # ── Stage 0.5: 가격 갱신 → TP/SL/MA20 청산 (신규 진입 전) ───────
            print("[Stage0.5] 기존 포지션 TP/SL/MA20 체크")
            exit_results = self.stop_manager.check_and_exit()
            if exit_results:
                self.trade_logger.log_all(exit_results)

            # ── Stage 1: 시장 분석 ───────────────────────────────────────────
            try:
                market_state = self.market_analyzer.analyze()
                print(f"[Stage1] 시장 상태: {market_state.value}")
            except TrTimeoutError as e:
                # ❗ 시장 분석용 TR (지수/업종 등) 타임아웃 발생
                #    → 신규 진입은 모두 막고, 기존 포지션만 유지/청산하는 방어 모드로 종료
                msg = (
                    f"시장 분석 중 TR 타임아웃 발생: {e}. "
                    "오늘은 신규 진입 없이 기존 포지션만 유지합니다."
                )
                print(f"[Stage1 WARNING] {msg}")
                return {
                    "status":    "TR_TIMEOUT",
                    "message":   msg,
                    "portfolio": self.portfolio.summary(),
                }

            if mode == "SOFT_STOP":
                return {
                    "status":    "SOFT_STOP",
                    "message":   "일 손실 한도 초과. 신규 진입 없음.",
                    "portfolio": self.portfolio.summary()
                }

            # ── Stage 2: 종목 필터 ───────────────────────────────────────────
            candidates = self.stock_filter.filter(market_state)
            print(f"[Stage2] 후보 종목: {candidates}")

            # ── Stage 2.5: Early Entry 우선순위 적용 ─────────────────────────
            if self.early_layer is not None and market_state.value == "BULL":
                candidates = self.early_layer.prioritize_candidates(
                    candidates=candidates,
                    portfolio=self.portfolio,
                )
                print(f"[Stage2.5] Early Entry 우선순위 적용 완료")

            # ── Stage 3: Q-Score ─────────────────────────────────────────────
            scored = self.q_scorer.score(candidates, market_state)

            # ── Stage 3.5: Early Entry 플래그 + 갭업/섹터캡 필터 ─────────────
            if self.early_layer is not None:
                scored = self.early_layer.annotate_early_flag(scored)
                scored = self.early_layer.apply_entry_filters(
                    scored=scored,
                    portfolio=self.portfolio,
                )

            # ── Stage 4: TP/SL 계획 + 포지션 사이징 (기존 보유 제외) ─────────
            scored_new = [s for s in scored if not self.portfolio.has_position(s["code"])]
            if len(scored_new) < len(scored):
                print(f"[Stage4] 기존 보유 {len(scored) - len(scored_new)}개 신규 진입 제외")

            positioned = self.risk_manager.apply(
                scored_new,
                market_state,
                available_cash=self.portfolio.cash,
                total_asset=self.portfolio.get_current_equity(),
            )

            # ── Stage 5: 실행 + TP/SL 포지션 등록 ───────────────────────────
            print("[Stage5] 주문 실행 시작")
            results = self.order_manager.execute_all(positioned)
            self.trade_logger.log_all(results, positioned=positioned)

            # 체결 성공한 종목에 TP/SL 계획값 등록
            pos_map = {p["code"]: p for p in positioned}
            for result in results:
                if not result.rejected and result.side == "BUY":
                    plan = pos_map.get(result.code, {})
                    self.portfolio.register_plan(
                        code     = result.code,
                        tp       = float(plan.get("tp", 0)),
                        sl       = float(plan.get("sl", 0)),
                        q_score  = float(plan.get("q_score", 0)),
                        rr_ratio = float(plan.get("rr_ratio", 0)),
                    )

            # ── Stage 7: 일일 리포트 ─────────────────────────────────────────
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

        except Exception as e:
            import traceback
            print(f"[Pipeline ERROR] 예외 발생:\n{traceback.format_exc()}")
            return {
                "status":    "ERROR",
                "message":   f"파이프라인 오류: {type(e).__name__}: {e}",
                "portfolio": self.portfolio.summary(),
            }

    def end_of_day(self, open_browser: bool = False):
        """
        장 종료 후 반드시 호출.
        - 포트폴리오 일일 기준가 갱신
        - 기간 리포트 자동 트리거
        """
        self.portfolio.end_of_day_update()
        print("[EOD] 일일 기준가 업데이트 완료")
        print("[EOD]", self.portfolio.summary())
        self.reporter.auto_period_reports(open_browser=open_browser)
        if open_browser:
            open_latest_report()