"""
RuntimeEngine (v7)
==================
Gen3 런타임 메인 컨트롤러. 실행 시간: 09:00 ~ 15:30

v7 Runtime Flow:
  Step 0   — Risk Governor (월 DD 한도)
  Step 1   — 레짐 감지 (MA200 + Breadth, 배치 JSON 우선)
  Step 1.5 — RAL 모드 결정 (CRASH/SURGE/NORMAL)
             CRASH → SL 강화 + 신규 진입 전량 차단
             SURGE → Trailing Stop 완화
  Step 0.5 — 포지션 청산 점검 (SL/RAL_CRASH/RS_EXIT/MAX_HOLD)
  Step 3   — Stage A Early Entry (BULL + NORMAL/SURGE만)
  Step 4   — Stage B Main Entry
  Step 5   — 포지션 현황 + 상태 저장
  Step 6   — 일일 리포트
"""

from __future__ import annotations

from datetime import datetime, time as dtime
from typing import Any, Dict, List

from config import Gen3Config
from core.portfolio_manager import PortfolioManager
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from core.state_manager import StateManager
from runtime.order_executor import OrderExecutor
from runtime.position_monitor import PositionMonitor
from strategy.entry_signal import EntrySignal
from strategy.regime_detector import RegimeDetector
from strategy.stage_manager import StageManager
from strategy.ral_engine import RALEngine
from strategy.exit_logic import ExitLogic


def _is_market_open() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 0) <= t <= dtime(15, 30)


class RuntimeEngine:

    def __init__(self, config: Gen3Config, provider, skip_market_hours: bool = False,
                 fresh_state: bool = False):
        self.config            = config
        self.provider          = provider
        self.skip_market_hours = skip_market_hours
        self.fresh_state       = fresh_state

        # State (재시작 안전성: 포지션 복구 + 중복 진입 방지)
        self.state_mgr = StateManager(config)

        # Core
        self.portfolio  = PortfolioManager(config)
        self.executor   = OrderExecutor(provider, self.portfolio, config.paper_trading)
        self.risk_mgr   = RiskManager(self.portfolio, self.executor)
        self.order_mgr  = OrderManager(self.executor, provider)

        # 저장된 포지션 복원 (fresh_state=True 이면 스킵 → mock 등 클린 실행용)
        if not fresh_state:
            restored = self.state_mgr.restore_portfolio(self.portfolio)
            if restored:
                print(f"[RuntimeEngine] 이전 상태 복원: {restored}개 포지션")
        else:
            print("[RuntimeEngine] fresh_state=True — 포지션 복원 스킵 (클린 시작)")

        # Strategy
        self.regime_det = RegimeDetector(provider, config)
        self.entry_sig  = EntrySignal(config)
        self.stage_mgr  = StageManager(provider, self.portfolio, config)
        self.ral_engine = RALEngine(config)

        # Report
        from report.reporter import Reporter, TradeLogger
        self.reporter     = Reporter(config)
        self.trade_logger = TradeLogger(config)

        # PositionMonitor (SL 실시간 감시용)
        self.pos_monitor = PositionMonitor(
            provider, self.executor, self.portfolio, config,
            trade_logger=self.trade_logger,
        )

    def run(self) -> Dict[str, Any]:
        try:
            # ── 장중 여부 확인 ──────────────────────────────────────────────
            if not self.skip_market_hours and not _is_market_open():
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"장 외 시간 ({now_str}) — 파이프라인 스킵"
                print(f"[RuntimeEngine] {msg}")
                return {"status": "MARKET_CLOSED", "message": msg,
                        "portfolio": self.portfolio.summary()}

            # ── Step 0: Risk Governor ────────────────────────────────────────
            mode = self.risk_mgr.evaluate()
            if mode == "HARD_STOP":
                return {"status": "HARD_STOP",
                        "message": "월 DD 한도 초과. 전 포지션 청산 완료.",
                        "portfolio": self.portfolio.summary()}

            # ── Step 1: 레짐 감지 ────────────────────────────────────────────
            regime = self.regime_det.detect()
            print(f"[RuntimeEngine] Step 1 — 레짐: {regime.value}")

            # ── Step 1.5: RAL 모드 결정 ──────────────────────────────────────
            ral_mode = self.ral_engine.determine_mode()
            print(f"[RuntimeEngine] Step 1.5 — RAL: {ral_mode}")

            # ── Step 2: signals.csv 로드 (청산 RS 판단에도 필요) ─────────────
            signals = self.entry_sig.load_today()

            # ── Step 0.5: 포지션 청산 점검 ───────────────────────────────────
            print("[RuntimeEngine] Step 0.5 - 포지션 청산 점검")
            exit_logic = ExitLogic(
                provider     = self.provider,
                executor     = self.executor,
                portfolio    = self.portfolio,
                config       = self.config,
                trade_logger = self.trade_logger,
                ral_mode     = ral_mode,
            )
            exit_results = exit_logic.check_and_exit(signals_today=signals)
            self._log_trades(exit_results)

            # RAL SL 조정 (청산 후 잔여 포지션에 적용)
            if ral_mode == "CRASH":
                self.ral_engine.apply_crash_sl(self.portfolio, self.provider)
            elif ral_mode == "SURGE":
                self.ral_engine.apply_surge_sl(self.portfolio, self.provider)

            if mode == "DAILY_KILL":
                self._save_state(regime, ral_mode)
                return {"status": "DAILY_KILL",
                        "message": f"일 DD {self.portfolio.get_daily_pnl_pct():.2%} — 신규 진입 완전 차단",
                        "portfolio": self.portfolio.summary()}

            if mode == "SOFT_STOP":
                return {"status": "SOFT_STOP",
                        "message": "일 손실 한도 초과. 신규 진입 없음.",
                        "portfolio": self.portfolio.summary()}

            # CRASH 모드 → 신규 진입 전량 차단
            if ral_mode == "CRASH":
                print("[RuntimeEngine] RAL CRASH — 신규 진입 전량 차단")
                self._save_state(regime, ral_mode)
                return {
                    "status":    "RAL_CRASH",
                    "message":   f"RAL CRASH — 신규 차단 / 레짐: {regime.value}",
                    "portfolio": self.portfolio.summary(),
                }

            # ── Step 3: Stage A — Early Entry ────────────────────────────────
            stage_a_positioned = self.stage_mgr.run_stage_a(signals, regime)
            stage_a_results    = self.order_mgr.execute_all(stage_a_positioned)
            self._register_plans(stage_a_results, stage_a_positioned)
            self._log_trades(stage_a_results, stage_a_positioned)

            stage_a_codes = [r.code for r in stage_a_results if not r.rejected]

            # ── Step 4: Stage B — Main Strategy ──────────────────────────────
            stage_b_positioned = self.stage_mgr.run_stage_b(
                signals, regime, exclude_codes=stage_a_codes
            )
            stage_b_results = self.order_mgr.execute_all(stage_b_positioned)
            self._register_plans(stage_b_results, stage_b_positioned)
            self._log_trades(stage_b_results, stage_b_positioned)

            # ── Step 5: 포지션 현황 + 상태 저장 ─────────────────────────────
            self.pos_monitor.print_positions()

            entered_codes = stage_a_codes + [r.code for r in stage_b_results if not r.rejected]
            if entered_codes:
                self.state_mgr.mark_entered(entered_codes)
            self._save_state(regime, ral_mode)

            # ── Step 6: 일일 리포트 ──────────────────────────────────────────
            all_positioned = stage_a_positioned + stage_b_positioned
            all_results    = stage_a_results + stage_b_results
            self.reporter.report_daily(
                results    = all_results,
                positioned = all_positioned,
                regime     = regime,
                signals    = signals,
                portfolio  = self.portfolio.summary(),
            )

            accepted = [r for r in all_results if not r.rejected]
            return {
                "status":    "NORMAL",
                "message":   (f"완료 — 체결 {len(accepted)}건 / "
                              f"레짐: {regime.value} / RAL: {ral_mode}"),
                "portfolio": self.portfolio.summary(),
            }

        except Exception as e:
            import traceback
            print(f"[RuntimeEngine ERROR]\n{traceback.format_exc()}")
            return {
                "status":    "ERROR",
                "message":   f"{type(e).__name__}: {e}",
                "portfolio": self.portfolio.summary(),
            }

    def end_of_day(self) -> None:
        self.portfolio.end_of_day_update()
        self.state_mgr.save_portfolio(self.portfolio)
        self.state_mgr.clear_runtime()
        print("[EOD] 일일 기준가 업데이트 완료")
        print("[EOD]", self.portfolio.summary())
        self.reporter.auto_period_reports()

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────────

    def _save_state(self, regime, ral_mode: str) -> None:
        import datetime as _dt
        self.state_mgr.save_portfolio(self.portfolio)
        self.state_mgr.save_runtime({
            "regime":   regime.value,
            "ral_mode": ral_mode,
            "date":     str(_dt.date.today()),
        })

    def _register_plans(self, results: list, positioned: List[Dict]) -> None:
        pos_map = {p["code"]: p for p in positioned}
        for r in results:
            if not r.rejected and r.side == "BUY":
                plan = pos_map.get(r.code, {})
                self.portfolio.register_plan(
                    code     = r.code,
                    tp       = float(plan.get("tp", 0)),
                    sl       = float(plan.get("sl", 0)),
                    q_score  = float(plan.get("qscore", 0)),
                    rr_ratio = float(plan.get("rr_ratio", 0)),
                )

    def _log_trades(self, results: list, positioned: List[Dict] = None) -> None:
        try:
            from report.reporter import TradeLogger
            logger = TradeLogger(self.config)
            logger.log_all(results, positioned or [])
        except Exception as e:
            print(f"[RuntimeEngine] 거래 로그 기록 실패 (비치명): {e}")
