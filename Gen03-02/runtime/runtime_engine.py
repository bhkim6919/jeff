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

import datetime as _dt
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from strategy.tick_analyzer import TickAnalyzer

from config import Gen3Config
from core.portfolio_manager import PortfolioManager
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from core.state_manager import StateManager
from core.position_tracker import Position
from runtime.order_executor import OrderExecutor
from runtime.position_monitor import PositionMonitor
from runtime.signal_tracker import SignalTracker
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
        print("[ENGINE] INIT → RESTORE")
        if not fresh_state:
            restored = self.state_mgr.restore_portfolio(self.portfolio)
            if restored:
                print(f"[ENGINE] 이전 상태 복원: {restored}개 포지션")
        else:
            print("[ENGINE] fresh_state=True — 포지션 복원 스킵 (클린 시작)")

        # SYNC_UNCERTAIN 플래그 (holdings 신뢰불가 + 큰 차이 시 신규 진입 차단)
        self._sync_uncertain = False
        # 매도 실패 종목 당일 재시도 차단 (T+2 결제 등) — 영속화: 재시작 시 복원
        self._sell_blocked: set = self.state_mgr.get_sell_blocked()
        if self._sell_blocked:
            print(f"[RuntimeEngine] sell_blocked 복원: {self._sell_blocked}")

        # LIVE 모드: Kiwoom 실계좌와 내부 포트폴리오 동기화
        if not config.paper_trading and not fresh_state:
            print("[ENGINE] RESTORE → SYNC_ACCOUNT")
            self._sync_with_kiwoom()
            # GHOST_FILL 자동 반영 (sync 직후 1회)
            _ghost_n = self._reconcile_ghost_fills()
            if _ghost_n:
                print(f"[RuntimeEngine] GHOST_FILL 반영: {_ghost_n}건")

        # v7.5: Orphan 포지션 자동 SL 설정 + prev_close 백필
        if not fresh_state and self.portfolio.positions:
            _repair = ExitLogic(provider, self.executor, self.portfolio, config)
            _repaired = _repair.repair_orphan_positions(regime="BULL")
            if _repaired:
                print(f"[RuntimeEngine] Orphan repair: {_repaired}개 포지션 수정")
                self.state_mgr.save_portfolio(self.portfolio)

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

        # SignalTracker (종목별 진행 상태 추적)
        self.tracker = SignalTracker()

        # 체결강도 데이터 소스 (LIVE | MOCK | MOCK_FALLBACK | NEUTRAL_PASS | N/A)
        self._tick_source: str = "N/A"

        # v7.9: 당일 청산 종목 (ghost fill 재오픈 방지, 영속 복원)
        self._closed_today: set = self.state_mgr.get_closed_today()
        if self._closed_today:
            print(f"[ENGINE] closed_today 복원: {self._closed_today}")
        # v7.8: reconcile broker 미보유 연속 카운트 {code: count}
        self._reconcile_miss_count: Dict[str, int] = {}

        # MarginState 리셋 (이전 사이클 잔여 상태 해제)
        self.order_mgr.reset_margin_state()
        print("[ENGINE] INIT → READY")

    # ── 체결강도 관측 페이즈 ──────────────────────────────────────────────────
    def run_observation_phase(self) -> "Optional[TickAnalyzer]":
        """
        Phase 0.5: 체결강도 관측 시작.
        시그널 로드 → 관측 대상 코드 등록 → TickAnalyzer 반환.
        main.py에서 관측 대기 후 run_entries(tick_analyzer) 호출.
        """
        from strategy.tick_analyzer import TickAnalyzer

        if not getattr(self.config, 'TICK_ENABLED', False):
            return None

        signals = self.entry_sig.load_today()
        if not signals:
            print("[RuntimeEngine] 시그널 없음 — 체결강도 관측 스킵")
            return None

        is_mock = not hasattr(self.provider, 'register_real')
        analyzer = TickAnalyzer(self.provider, self.config, signals, mock=is_mock)
        analyzer.start_observation()

        self._preloaded_signals = signals
        return analyzer

    # ── 하위 호환: 기존 run() 호출자를 위한 래퍼 ──────────────────────────────
    def run(self) -> Dict[str, Any]:
        """하위 호환용 래퍼 (mock/pykrx 등 1회 실행 모드)."""
        return self.run_entries()

    def run_entries(self, tick_analyzer=None) -> Dict[str, Any]:
        """장 시작 1회: 레짐 감지 → 시그널 로드 → 매수 → 청산 체크."""
        try:
            print("[ENGINE] READY → RUNNING")
            # ── 장중 여부 확인 ──────────────────────────────────────────────
            if not self.skip_market_hours and not _is_market_open():
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"장 외 시간 ({now_str}) — 파이프라인 스킵"
                print(f"[RuntimeEngine] {msg}")
                return {"status": "MARKET_CLOSED", "message": msg,
                        "portfolio": self.portfolio.summary()}

            # ── 일일 1회 진입 제한: 이미 오늘 진입 라운드 완료 시 청산만 실행 ──
            if self.state_mgr.is_entries_done():
                done_at = self.state_mgr.load_runtime().get("entries_done_at", "")
                print(f"[RuntimeEngine] *** 오늘 진입 라운드 이미 완료 ({done_at}) ***")
                print(f"[RuntimeEngine] 신규 진입 스킵 → 청산 체크만 실행")
                if tick_analyzer:
                    tick_analyzer.stop_observation()
                regime = self.regime_det.detect()
                self._regime = regime
                ral_mode = self.ral_engine.determine_mode()
                self._ral_mode = ral_mode
                signals = self.entry_sig.load_today()
                self._signals = signals
                # 가격 스냅샷
                _snap_ts = datetime.now().strftime("%H:%M:%S")
                _pre_prices = {}
                for _c in self.portfolio.positions:
                    try:
                        _p = self.provider.get_current_price(_c)
                        if _p and _p > 0:
                            _pre_prices[_c] = float(_p)
                    except Exception:
                        pass
                if _pre_prices:
                    self.portfolio.update_prices(_pre_prices)
                # 청산 체크
                exit_logic = ExitLogic(
                    provider=self.provider, executor=self.executor,
                    portfolio=self.portfolio, config=self.config,
                    trade_logger=self.trade_logger, ral_mode=ral_mode,
                )
                exit_results = exit_logic.check_and_exit(
                    signals_today=signals, regime=regime.value,
                    price_snapshot=_pre_prices)
                self._log_trades(exit_results)
                self._record_sl_cooldown(exit_results)
                self._save_state(regime, ral_mode)
                return {"status": "ENTRIES_DONE", "snap_ts": _snap_ts,
                        "message": f"오늘 진입 완료 상태 — 청산만 실행 / 레짐: {regime.value}",
                        "portfolio": self.portfolio.summary()}

            # ── Step 0-pre: SYNC_UNCERTAIN → 신규 진입 차단, 청산만 허용 ─────
            if self._sync_uncertain:
                print("[AccountSync] SYNC_UNCERTAIN — 신규 진입 차단 (청산/모니터링만 허용)")
                if tick_analyzer:
                    tick_analyzer.stop_observation()
                # 청산 로직은 실행 (기존 포지션 SL/TP 관리)
                regime = self.regime_det.detect()
                self._regime = regime
                signals = self.entry_sig.load_today()
                exit_logic = ExitLogic(
                    provider=self.provider, executor=self.executor,
                    portfolio=self.portfolio, config=self.config,
                    trade_logger=self.trade_logger, ral_mode="NORMAL",
                )
                # SYNC_UNCERTAIN에서도 가격 스냅샷 사용
                _unc_prices = {}
                for _c in self.portfolio.positions:
                    try:
                        _p = self.provider.get_current_price(_c)
                        if _p and _p > 0:
                            _unc_prices[_c] = float(_p)
                    except Exception:
                        pass
                if _unc_prices:
                    self.portfolio.update_prices(_unc_prices)
                exit_results = exit_logic.check_and_exit(
                    signals_today=signals, regime=regime.value,
                    price_snapshot=_unc_prices)
                self._log_trades(exit_results)
                self._save_state(regime, "NORMAL")
                return {"status": "SYNC_UNCERTAIN",
                        "message": "holdings 신뢰불가 — 신규 진입 차단, 청산만 허용",
                        "portfolio": self.portfolio.summary()}

            # ── Step 0: Risk Governor ────────────────────────────────────────
            # 가격 스냅샷 1회 취득 → 이후 evaluate + check_and_exit 모두 이 스냅샷 사용
            _snap_ts = datetime.now().strftime("%H:%M:%S")
            _pre_prices = {}
            _snap_fails = 0
            for _c in self.portfolio.positions:
                try:
                    _p = self.provider.get_current_price(_c)
                    if _p and _p > 0:
                        _pre_prices[_c] = float(_p)
                    else:
                        _snap_fails += 1
                except Exception as _e:
                    _snap_fails += 1
                    print(f"[PRICE_FAIL] {_c} {type(_e).__name__}: {_e}")
            if _pre_prices:
                self.portfolio.update_prices(_pre_prices)
                print(f"[PriceSnap {_snap_ts}] {len(_pre_prices)}종목 가격 갱신"
                      f"{f' ({_snap_fails}건 실패)' if _snap_fails else ''}")
            mode = self.risk_mgr.evaluate(snap_ts=_snap_ts)
            if mode == "HARD_STOP":
                # BUG-6: 실시간 구독 해제 (관측 중이었다면)
                if tick_analyzer:
                    tick_analyzer.stop_observation()
                return {"status": "HARD_STOP", "snap_ts": _snap_ts,
                        "message": "월 DD 한도 초과. 전 포지션 청산 완료.",
                        "portfolio": self.portfolio.summary()}

            # ── Step 1: 레짐 감지 ────────────────────────────────────────────
            regime = self.regime_det.detect()
            print(f"[RuntimeEngine] Step 1 — 레짐: {regime.value}")

            # 레짐별 포지션 한도 적용 (BULL=20, BEAR=8)
            self.portfolio.set_regime_limits(regime.value)

            # ── Step 1.5: RAL 모드 결정 ──────────────────────────────────────
            ral_mode = self.ral_engine.determine_mode()
            print(f"[RuntimeEngine] Step 1.5 — RAL: {ral_mode}")

            # ── Step 2: signals.csv 로드 (청산 RS 판단에도 필요) ─────────────
            # run_observation_phase()에서 미리 로드한 경우 재사용
            if tick_analyzer and hasattr(self, '_preloaded_signals') and self._preloaded_signals:
                signals = self._preloaded_signals
                print(f"[RuntimeEngine] Step 2 — 시그널 재사용 ({len(signals)}개, 관측 페이즈)")
            else:
                signals = self.entry_sig.load_today()

            # ── Tracker 초기화 ────────────────────────────────────────────
            self.tracker.init_signals(signals)
            self.tracker.update_prices(self.portfolio)  # 보유 포지션 즉시 HOLDING 반영

            # ── Step 2.5: 체결강도 필터 적용 ───────────────────────────────
            if tick_analyzer:
                pre_count = len(signals)
                pre_signals = list(signals)
                tick_analyzer.stop_observation()
                signals = tick_analyzer.filter_signals(signals)
                self.tracker.update_tick_from_signals(pre_signals, signals)
                self._tick_source = tick_analyzer.tick_source
                print(f"[RuntimeEngine] Step 2.5 — 체결강도 필터: "
                      f"{pre_count}개 -> {len(signals)}개 "
                      f"tick_source={self._tick_source}")

            # 장중 모니터링 루프에서 재사용할 수 있도록 캐싱
            self._regime   = regime
            self._ral_mode = ral_mode
            self._signals  = signals

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
            exit_results = exit_logic.check_and_exit(
                signals_today=signals, regime=regime.value,
                price_snapshot=_pre_prices)
            self._log_trades(exit_results)
            self._record_sl_cooldown(exit_results)
            self.tracker.update_exit_results(exit_results)

            # RAL SL 조정 (청산 후 잔여 포지션에 적용)
            if ral_mode == "CRASH":
                self.ral_engine.apply_crash_sl(self.portfolio, self.provider)
            elif ral_mode == "SURGE":
                self.ral_engine.apply_surge_sl(self.portfolio, self.provider)

            if mode == "DAILY_KILL":
                self._save_state(regime, ral_mode)
                return {"status": "DAILY_KILL", "snap_ts": _snap_ts,
                        "message": f"일 DD {self.portfolio.get_daily_pnl_pct():.2%} — 신규 진입 완전 차단",
                        "portfolio": self.portfolio.summary()}

            if mode == "SOFT_STOP":
                self._save_state(regime, ral_mode)
                return {"status": "SOFT_STOP", "snap_ts": _snap_ts,
                        "message": "일 손실 한도 초과. 신규 진입 없음.",
                        "portfolio": self.portfolio.summary()}

            # CRASH 모드 → 신규 진입 전량 차단
            if ral_mode == "CRASH":
                print("[RuntimeEngine] RAL CRASH — 신규 진입 전량 차단")
                self._save_state(regime, ral_mode)
                return {
                    "status":    "RAL_CRASH", "snap_ts": _snap_ts,
                    "message":   f"RAL CRASH — 신규 차단 / 레짐: {regime.value}",
                    "portfolio": self.portfolio.summary(),
                }

            # ── 진입 시간대 제한 (v7.4): ENTRY_CUTOFF_HOUR 이후 신규 진입 차단 ──
            cutoff_h = getattr(self.config, 'ENTRY_CUTOFF_HOUR', 0)
            cutoff_m = getattr(self.config, 'ENTRY_CUTOFF_MINUTE', 0)
            if cutoff_h > 0:
                now_t = datetime.now().time()
                cutoff_t = dtime(cutoff_h, cutoff_m)
                if now_t >= cutoff_t:
                    print(f"[RuntimeEngine] 진입 시간대 종료 ({cutoff_h:02d}:{cutoff_m:02d} 이후) — 신규 진입 차단")
                    self._save_state(regime, ral_mode)
                    return {
                        "status":    "ENTRY_CUTOFF", "snap_ts": _snap_ts,
                        "message":   f"진입 시간대 종료 ({cutoff_h:02d}:{cutoff_m:02d}~) / 레짐: {regime.value}",
                        "portfolio": self.portfolio.summary(),
                    }

            # ── 당일 이미 진입한 종목 조회 (중복 진입 방지) ──────────────────
            _rt = self.state_mgr.load_runtime()
            already_entered = set(
                _rt.get("today_entries", [])
                if _rt.get("date") == str(_dt.date.today())
                else []
            )
            if already_entered:
                print(f"[RuntimeEngine] 당일 진입 이력: {len(already_entered)}개 → 중복 매수 차단")

            # ── SL cooldown: 당일 SL/GAP_DOWN 청산 종목 재진입 금지 ─────
            sl_cooldown = self.state_mgr.get_sl_cooldown()
            if sl_cooldown:
                print(f"[RuntimeEngine] SL cooldown: {len(sl_cooldown)}개 → 당일 재진입 금지 {sl_cooldown}")
            already_entered = already_entered | sl_cooldown

            # Tracker: cooldown / already entered 반영
            self.tracker.mark_cooldown(sl_cooldown)
            self.tracker.mark_already_entered(already_entered)

            # ── Step 3: Stage A — Early Entry ────────────────────────────────
            stage_a_positioned = self.stage_mgr.run_stage_a(signals, regime,
                                                             exclude_codes=list(already_entered))
            self._mark_ready(stage_a_positioned)
            stage_a_results    = self.order_mgr.execute_all(stage_a_positioned)
            self._register_plans(stage_a_results, stage_a_positioned)
            self._log_trades(stage_a_results, stage_a_positioned)
            self.tracker.update_order_results(stage_a_results, stage_a_positioned)

            stage_a_codes = [r.code for r in stage_a_results if not r.rejected]

            # ── margin_exhausted 전파: Stage A 거부 확인 ──────────────────
            margin_state = self.order_mgr.margin_state
            if margin_state["exhausted"]:
                print(f"[RuntimeEngine] Stage A 증거금 부족 감지 — Stage B 스킵 "
                      f"(사유: {margin_state['reason']}, "
                      f"scope: {margin_state['scope']})")
                stage_b_positioned = []
                stage_b_results = []
            else:
                # ── Step 4: Stage B — Main Strategy ──────────────────────────
                exclude_all = list(already_entered | set(stage_a_codes))
                stage_b_positioned = self.stage_mgr.run_stage_b(
                    signals, regime, exclude_codes=exclude_all
                )
                self._mark_ready(stage_b_positioned)
                stage_b_results = self.order_mgr.execute_all(stage_b_positioned)

            self._register_plans(stage_b_results, stage_b_positioned)
            self._log_trades(stage_b_results, stage_b_positioned)
            self.tracker.update_order_results(stage_b_results, stage_b_positioned)

            # ── Step 5: 포지션 현황 + 상태 저장 ─────────────────────────────
            self.tracker.update_prices(self.portfolio)
            self.tracker.finalize_entries()
            self.tracker.print_dashboard()
            self.pos_monitor.print_positions()

            entered_codes = stage_a_codes + [r.code for r in stage_b_results if not r.rejected]
            if entered_codes:
                self.state_mgr.mark_entered(entered_codes)
            # 일일 1회 진입 완료 표시 (재시작 시 중복 진입 방지)
            self.state_mgr.mark_entries_done()
            self._save_state(regime, ral_mode)

            # ── Step 6: 일일 리포트 ──────────────────────────────────────────
            all_positioned = stage_a_positioned + stage_b_positioned
            all_results    = stage_a_results + stage_b_results
            # EOD fallback용 캐싱
            self._all_positioned = all_positioned
            self._all_results    = all_results
            # v7.6: 제약 포지션 정보 수집
            restricted_pos = []
            for code, pos in self.portfolio.positions.items():
                if getattr(pos, 'is_restricted', False) or getattr(pos, 'qty_confidence', 'HIGH') != 'HIGH':
                    sell_qty = pos.qty_sellable if pos.qty_sellable >= 0 else "?"
                    restricted_pos.append({
                        "code": code,
                        "hold_qty": pos.quantity,
                        "sellable_qty": sell_qty,
                        "qty_confidence": getattr(pos, 'qty_confidence', '?'),
                        "reason": getattr(pos, 'restricted_reason', ''),
                    })
            self.reporter.report_daily(
                results    = all_results,
                positioned = all_positioned,
                regime     = regime,
                signals    = signals,
                portfolio  = self.portfolio.summary(),
                restricted_positions = restricted_pos,
            )

            accepted = [r for r in all_results if not r.rejected]
            return {
                "status":    "NORMAL", "snap_ts": _snap_ts,
                "tick_source": self._tick_source,
                "message":   (f"완료 — 체결 {len(accepted)}건 / "
                              f"레짐: {regime.value} / RAL: {ral_mode}"),
                "portfolio": self.portfolio.summary(),
            }

        except Exception as e:
            import traceback
            print(f"[ENGINE] RUNNING → ERROR {type(e).__name__}: {e}")
            print(traceback.format_exc())
            return {
                "status":    "ERROR",
                "message":   f"{type(e).__name__}: {e}",
                "portfolio": self.portfolio.summary(),
            }

    # ── 장중 모니터링 (반복 호출용) ──────────────────────────────────────────

    def run_monitor_cycle(self) -> Dict[str, Any]:
        """
        장중 반복: 가격 업데이트 → SL/TP 청산 체크 → 상태 저장.
        run_entries() 이후 60초 간격으로 호출.
        """
        try:
            # GHOST_FILL 확인 (매 사이클 시작 시)
            self._reconcile_ghost_fills()

            # v7.9: 실시간 피드 헬스체크 — 5분 미수신 종목 재등록
            #   우선순위: real re-register > account snapshot > per-symbol TR
            #   re-register 쿨다운: 동일 종목 300초 이내 재등록 방지
            if hasattr(self.provider, 'check_real_feed_health'):
                import time as _time
                stale_feeds = self.provider.check_real_feed_health(stale_sec=300)
                if stale_feeds and hasattr(self.provider, 're_register_real'):
                    if not hasattr(self, '_feed_reregister_ts'):
                        self._feed_reregister_ts = {}
                    now_mono = _time.monotonic()
                    _to_register = [c for c in stale_feeds
                                    if now_mono - self._feed_reregister_ts.get(c, 0) > 300]
                    if _to_register:
                        print(f"[FEED:STALE] {len(_to_register)}종목 시세 미수신 5분+ → 재등록: {_to_register}")
                        self.provider.re_register_real(_to_register)
                        for c in _to_register:
                            self._feed_reregister_ts[c] = now_mono

            # v7.6: restricted 포지션 자동 재검증 (매 사이클)
            self._try_reconcile_restricted()

            # 가격 스냅샷 1회 취득 → evaluate + check_and_exit 동일 스냅샷 사용
            _snap_ts = datetime.now().strftime("%H:%M:%S")
            _pre_prices = {}
            _price_fail_count = 0
            _price_fail_codes = []
            for _c in self.portfolio.positions:
                try:
                    _p = self.provider.get_current_price(_c)
                    if _p and _p > 0:
                        _pre_prices[_c] = float(_p)
                    else:
                        _price_fail_count += 1
                        _price_fail_codes.append(_c)
                except Exception as _e:
                    _price_fail_count += 1
                    _price_fail_codes.append(_c)
                    # v7.9: 예외 구조화 로그 (silent failure 방지)
                    if not hasattr(self, '_price_fail_logged'):
                        self._price_fail_logged = {}
                    _key = f"{_c}_{type(_e).__name__}"
                    _cnt = self._price_fail_logged.get(_key, 0) + 1
                    self._price_fail_logged[_key] = _cnt
                    if _cnt <= 3:
                        print(f"[PRICE_FAIL] {_c} {type(_e).__name__}: {_e} "
                              f"(count={_cnt}, stale_fallback=True)")
            # v7.9: 전량 실패 시 DEGRADED 승격
            _n_pos = len(self.portfolio.positions)
            if _n_pos > 0 and _price_fail_count >= _n_pos:
                if not getattr(self, '_all_price_fail_warned', False):
                    print(f"[PRICE_FAIL:ALL] {_price_fail_count}/{_n_pos}종목 전량 가격 조회 실패 "
                          f"→ DEGRADED_LIVE 진입")
                    self._all_price_fail_warned = True
                    self.risk_mgr.risk_confidence = "DEGRADED"
            elif _price_fail_count > 0 and _price_fail_count < _n_pos:
                self._all_price_fail_warned = False
            if _pre_prices:
                self.portfolio.update_prices(_pre_prices)
            # Risk 재평가 (일중 DD 변동 반영)
            mode = self.risk_mgr.evaluate(snap_ts=_snap_ts)
            if mode == "HARD_STOP":
                return {"status": "HARD_STOP",
                        "message": "월 DD 한도 초과. 전 포지션 청산 완료.",
                        "portfolio": self.portfolio.summary()}

            # v7.9: stale equity 강화 방어
            _stale_age = self.portfolio.get_stale_age_sec()
            if self.risk_mgr.risk_confidence == "DEGRADED":
                if not getattr(self, '_degraded_logged', False):
                    print(f"[DEGRADED_LIVE] 시세 갱신 중단 — 리스크 판단 신뢰 불가, "
                          f"청산만 허용 (stale_age={_stale_age:.0f}s)")
                    self._degraded_logged = True
                # v7.9: 10분 이상 stale → 강제 재조회 (쿨다운 내장)
                if _stale_age >= 600:
                    # _force_price_refresh 내부에 300초 쿨다운 + in_flight guard 있음
                    self._force_price_refresh()
            else:
                self._degraded_logged = False
                self._stale_force_refreshed = False

            # 캐싱된 레짐/시그널 사용 (장중 불변)
            regime   = getattr(self, "_regime", None)
            ral_mode = getattr(self, "_ral_mode", "NORMAL")
            signals  = getattr(self, "_signals", [])

            if regime is None:
                return {"status": "SKIP", "message": "run_entries() 미실행",
                        "exits": 0, "positions": len(self.portfolio.positions)}

            # 청산 체크 (ExitLogic 내부에서 get_current_price 호출)
            exit_logic = ExitLogic(
                provider     = self.provider,
                executor     = self.executor,
                portfolio    = self.portfolio,
                config       = self.config,
                trade_logger = self.trade_logger,
                ral_mode     = ral_mode,
            )
            _regime_val = regime.value if regime else "BULL"
            exit_results = exit_logic.check_and_exit(
                signals_today=signals, regime=_regime_val,
                skip_codes=self._sell_blocked, price_snapshot=_pre_prices)
            self._log_trades(exit_results)
            self._record_sell_blocked(exit_results)
            self._record_sl_cooldown(exit_results)
            self.tracker.update_exit_results(exit_results)
            self.tracker.update_prices(self.portfolio)

            # EOD report용 모니터 청산 결과 누적
            if exit_results:
                if not hasattr(self, '_all_results'):
                    self._all_results = []
                self._all_results.extend(exit_results)

            # 포지션 현황 + 상태 저장
            n_exits = len([r for r in exit_results if not r.rejected]) if exit_results else 0
            if n_exits > 0:
                self.tracker.print_dashboard(compact=True)
                self.pos_monitor.print_positions()
            self._save_state(regime, ral_mode)

            n_pos = len(self.portfolio.positions)
            eq = self.portfolio.get_current_equity()
            pnl = self.portfolio.get_daily_pnl_pct()

            # 청산 발생 시에만 즉시 출력 (heartbeat는 main.py에서 관리)
            if n_exits > 0:
                print(f"[Monitor] src=live snap={_snap_ts} 청산 {n_exits}건 | "
                      f"잔여 {n_pos}개 | 총자산 {eq:,.0f}원 | pnl={pnl:.2%}")

            return {
                "status":    "MONITORING",
                "exits":     n_exits,
                "positions": n_pos,
                "equity":    eq,
                "pnl":       pnl,
                "portfolio": self.portfolio.summary(),
            }

        except Exception as e:
            import traceback
            print(f"[Monitor ERROR] {type(e).__name__}: {e}")
            print(traceback.format_exc())
            # 에러 발생해도 포지션 상태는 저장
            try:
                self.state_mgr.save_portfolio(self.portfolio)
            except OSError:
                pass
            return {
                "status":    "ERROR",
                "message":   f"{type(e).__name__}: {e}",
                "exits":     0,
                "positions": len(self.portfolio.positions),
            }

    def end_of_day(self) -> None:
        print("[ENGINE] RUNNING → SHUTDOWN (EOD)")
        self._degraded_logged = False  # v7.7: reset

        # v7.7: summary + report BEFORE baseline update (P4: EOD PnL 0% 수정)
        self.tracker.update_prices(self.portfolio)
        self.tracker.print_dashboard()
        self.tracker.print_block_summary()
        eod_summary = self.portfolio.summary()
        print("[EOD]", eod_summary)

        # EOD daily report (baseline 갱신 전이므로 PnL 정상 반영)
        regime   = getattr(self, "_regime", None)
        signals  = getattr(self, "_signals", [])
        restricted_pos = []
        for code, pos in self.portfolio.positions.items():
            if getattr(pos, 'is_restricted', False) or getattr(pos, 'qty_confidence', 'HIGH') != 'HIGH':
                sell_qty = pos.qty_sellable if pos.qty_sellable >= 0 else "?"
                restricted_pos.append({
                    "code": code, "hold_qty": pos.quantity,
                    "sellable_qty": sell_qty,
                    "qty_confidence": getattr(pos, 'qty_confidence', '?'),
                    "reason": getattr(pos, 'restricted_reason', ''),
                })
        try:
            self.reporter.report_daily(
                results    = getattr(self, "_all_results", []),
                positioned = getattr(self, "_all_positioned", []),
                regime     = regime,
                signals    = signals,
                portfolio  = eod_summary,
                restricted_positions = restricted_pos,
            )
        except Exception as e:
            print(f"[EOD] report_daily 생성 실패: {e}")

        # v7.9: EOD 검증 수준 명시
        print("[EOD:VERIFICATION] 현재 포지션: 증권사 TR 기준 동기화 완료 (시작 시 reconcile)")
        print("[EOD:VERIFICATION] 체결 원장(trades.csv): 별도 주문/체결 내역 조회로 검증 필요")

        # NOW reset baseline (순서 중요: report 이후)
        self.portfolio.end_of_day_update()
        self.risk_mgr.reset_daily()
        self.state_mgr.save_portfolio(self.portfolio)
        self.state_mgr.clear_runtime()
        print("[EOD] 일일 기준가 업데이트 완료")

        self.reporter.auto_period_reports()

    # ── Kiwoom 계좌 동기화 ────────────────────────────────────────────────────

    def _sync_with_kiwoom(self) -> None:
        """
        LIVE 모드 시작 시 Kiwoom 실계좌 → 내부 포트폴리오 동기화.

        해결하는 문제:
          - 내부 state와 실계좌 잔고 불일치 (RC4025 증거금 부족)
          - 이전 세션 포지션이 실계좌에 남아있지만 state가 리셋된 경우
          - 실계좌 현금이 config.initial_cash와 다른 경우

        BUG-5 v2: 모의투자 서버는 holdings multi-row를 반환하지 않을 수 있음.
          holdings_reliable=False 일 때 예탁자산 교차검증으로 판단:
          - 예탁자산 ≈ Engine 총자산 → state 유지 (포지션 건드리지 않음)
          - 예탁자산 ≠ Engine 총자산 → 예탁자산 기준 현금 갱신 + 포지션 제거
        """
        if not hasattr(self.provider, 'query_account_summary'):
            print("[AccountSync] KiwoomProvider가 아님 — 동기화 스킵")
            return

        print("[AccountSync] Kiwoom 실계좌 조회 중...")
        summary = self.provider.query_account_summary()

        # v7.9: empty_account는 실패가 아니라 "빈 계좌 정상 응답"
        _err = summary.get("error", "")
        if _err and _err != "empty_account":
            print(f"[AccountSync] 계좌 조회 실패: {_err} — state 파일 기준 계속 진행")
            return
        if _err == "empty_account":
            # 빈 계좌인데 엔진에 포지션이 있으면 phantom 정리
            if self.portfolio.positions:
                print(f"[AccountSync] 빈 계좌 + 엔진 포지션 {len(self.portfolio.positions)}개 "
                      f"→ phantom 정리 필요")
                recon = self.portfolio.reconcile_with_broker({})
                if recon["removed"]:
                    print(f"[AccountSync] phantom 제거: {recon['removed']}")
                    self.state_mgr.save_portfolio(self.portfolio)
            else:
                print("[AccountSync] 빈 계좌 + 빈 엔진 — 정상")
            return

        kiwoom_deposit  = summary["추정예탁자산"]
        kiwoom_cash     = summary["available_cash"]
        kiwoom_holdings = summary["holdings"]
        holdings_reliable = summary.get("holdings_reliable", True)

        engine_cash     = self.portfolio.cash
        engine_pos      = len(self.portfolio.positions)
        engine_equity   = self.portfolio.get_current_equity()

        print(f"[AccountSync] Kiwoom: 예탁자산={kiwoom_deposit:,}원, "
              f"가용현금={kiwoom_cash:,}원, 보유={len(kiwoom_holdings)}개"
              f"{' (holdings 신뢰불가)' if not holdings_reliable else ''}")
        print(f"[AccountSync] Engine: 현금={engine_cash:,.0f}원, 보유={engine_pos}개, "
              f"총자산={engine_equity:,.0f}원")

        # ── holdings_reliable=False: 예탁자산 교차검증 ─────────────────────
        if not holdings_reliable:
            if kiwoom_deposit <= 0:
                print("[AccountSync] 예탁자산=0 + holdings 신뢰불가 → state 유지")
                return

            # 예탁자산과 Engine 총자산 비교 (10% 허용 오차)
            diff_pct = abs(kiwoom_deposit - engine_equity) / max(engine_equity, 1) * 100

            if diff_pct < 10:
                # 오차 10% 미만 → 포지션 그대로 유지, 현금만 미세 조정
                print(f"[AccountSync] 예탁자산≈Engine총자산 (차이 {diff_pct:.1f}%) → state 유지")
                return
            else:
                # 큰 차이 + holdings 신뢰불가 → SYNC_UNCERTAIN 모드
                print(f"[AccountSync] *** 예탁자산 vs Engine 총자산 차이 {diff_pct:.1f}% ***")
                print(f"[AccountSync]   Kiwoom 예탁={kiwoom_deposit:,} vs Engine 총자산={engine_equity:,.0f}")
                print(f"[AccountSync]   holdings 신뢰불가 + 큰 차이 → SYNC_UNCERTAIN 모드 진입")

                # 현금 = 예탁자산 - 보유포지션 시가 (포지션 보존하므로 차감 필수)
                pos_mv = sum(p.market_value for p in self.portfolio.positions.values())
                safe_cash = max(0.0, float(kiwoom_deposit) - pos_mv)
                print(f"[AccountSync]   cash = deposit({kiwoom_deposit:,}) - pos_mv({pos_mv:,.0f}) = {safe_cash:,.0f}")
                self.portfolio.cash = safe_cash
                # 기준가 보호: 이미 설정된 경우 변경 금지 (EOD만 갱신 허용)
                if self.portfolio.prev_close_equity == 0:
                    self.portfolio.prev_close_equity = float(kiwoom_deposit)
                    self.portfolio.peak_equity = float(kiwoom_deposit)
                    print(f"[AccountSync] 기준가 초기화 (SYNC_UNCERTAIN, 신규): {kiwoom_deposit:,}")
                else:
                    print(f"[AccountSync] 기준가 보호 (SYNC_UNCERTAIN): "
                          f"prev_close={self.portfolio.prev_close_equity:,.0f} 유지")
                # 포지션은 보존 (삭제하지 않음)
                # 신규 진입 차단 플래그 설정
                self._sync_uncertain = True
                self.state_mgr.save_portfolio(self.portfolio)
                print(f"[AccountSync] 현금={kiwoom_deposit:,}원, 포지션 보존 ({engine_pos}개)")
                print(f"[AccountSync] *** SYNC_UNCERTAIN — 신규 진입 차단, 청산만 허용 ***")
                return

        # ── holdings_reliable=True: 정상 동기화 ──────────────────────────
        synced = False

        # 1) 현금 동기화: Kiwoom 실계좌 기준으로 덮어쓰기
        #    음수(미수금) → 0으로 설정 (매수 불가 상태 반영)
        effective_cash = max(0, kiwoom_cash)
        if abs(engine_cash - effective_cash) > 10000:
            print(f"[RECON] 현금 불일치: Engine={engine_cash:,.0f} → Kiwoom={kiwoom_cash:,}"
                  f"{' (미수금 → 0 처리)' if kiwoom_cash < 0 else ''}")
            self.portfolio.cash = float(effective_cash)
            synced = True

        # 2) 포지션 동기화: reconcile_with_broker (broker = truth)
        kiwoom_map = {}
        for h in kiwoom_holdings:
            sector = "기타"
            try:
                info = self.provider.get_stock_info(h["code"])
                sector = info.get("sector", "기타")
            except Exception:
                pass
            kiwoom_map[h["code"]] = {
                "qty": h["qty"],
                "avg_price": h["avg_price"],
                "cur_price": h["cur_price"],
                "name": h.get("name", ""),
                "sector": sector,
            }

        recon = self.portfolio.reconcile_with_broker(kiwoom_map)
        # v7.8: adopted 포지션 risk field 검증
        for _adopted_code in recon.get("added", []):
            self.portfolio.ensure_position_risk_fields(_adopted_code)
        if recon["added"] or recon["removed"] or recon["qty_fixed"]:
            synced = True
            # v7.7: reconcile diff log 저장
            diff_log = recon.get("_diff_log")
            if diff_log:
                try:
                    import json as _json
                    _diff_path = self.config.abs_path("data/logs") / \
                        f"reconcile_{_dt.date.today().strftime('%Y%m%d')}.json"
                    with open(_diff_path, "a", encoding="utf-8") as _f:
                        _f.write(_json.dumps(diff_log, ensure_ascii=False) + "\n")
                    print(f"[RECON] diff log saved → {_diff_path.name}")
                except Exception as _e:
                    print(f"[RECON] diff log 저장 실패: {_e}")

        # 3) 기준가 보호: prev_close/peak는 READ-ONLY — EOD만 갱신 허용
        #    AccountSync가 장중 재시작마다 덮어쓰면 DD 계산이 왜곡됨.
        #    유일한 예외: prev_close_equity == 0 (신규 계좌/초기 상태)
        if kiwoom_deposit > 0:
            new_equity = self.portfolio.get_current_equity()
            if self.portfolio.prev_close_equity == 0:
                # 초기 상태 — 기준가 없으므로 현재 equity로 설정
                print(f"[AccountSync] 기준가 초기화 (신규): prev_close={new_equity:,.0f}, "
                      f"peak={new_equity:,.0f}")
                self.portfolio.prev_close_equity = new_equity
                self.portfolio.peak_equity = new_equity
                synced = True
            else:
                # READ-ONLY: 값 변경 금지, 로그만 출력
                diff_pct = (new_equity - self.portfolio.prev_close_equity) / self.portfolio.prev_close_equity * 100
                if abs(diff_pct) > 30:
                    print(f"[AccountSync] WARNING 기준가 vs 현재 equity 큰 차이 {diff_pct:+.1f}% — "
                          f"prev_close={self.portfolio.prev_close_equity:,.0f} 변경하지 않음 "
                          f"(EOD만 갱신 허용)")
                else:
                    print(f"[AccountSync] 기준가 유지: prev_close={self.portfolio.prev_close_equity:,.0f}, "
                          f"peak={self.portfolio.peak_equity:,.0f}, "
                          f"현재equity={new_equity:,.0f} (diff={diff_pct:+.2f}%)")

        # v7.8: sync 당일 broker equity 기준 저장 (DAILY_KILL 정확도)
        _sync_eq = self.portfolio.get_current_equity()
        self.portfolio.synced_broker_equity = _sync_eq
        print(f"[AccountSync] synced_broker_equity={_sync_eq:,.0f}원 (DAILY_KILL 기준)")

        if synced:
            self.state_mgr.save_portfolio(self.portfolio)
            eq = self.portfolio.get_current_equity()
            print(f"[AccountSync] *** 동기화 완료 *** "
                  f"총자산={eq:,.0f}원, 현금={self.portfolio.cash:,.0f}원, "
                  f"포지션={len(self.portfolio.positions)}개")
        else:
            print("[AccountSync] OK — 내부 상태 = 실계좌 (동기화 불필요)")

        # ── today_entries 정리: 미보유 종목 제거 (HARD_STOP 매도 후 재진입 허용) ──
        _rt = self.state_mgr.load_runtime()
        today_str = str(_dt.date.today())
        if _rt.get("date") == today_str:
            old_entries = set(_rt.get("today_entries", []))
            held_codes = set(self.portfolio.positions.keys())
            pruned = old_entries & held_codes
            removed = old_entries - held_codes
            if removed:
                _rt["today_entries"] = list(pruned)
                self.state_mgr.save_runtime(_rt)
                print(f"[AccountSync] today_entries 정리: {len(old_entries)}개 → {len(pruned)}개 "
                      f"(미보유 {len(removed)}개 제거)")

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────────────

    def _save_state(self, regime, ral_mode: str) -> None:
        """FIX-1: today_entries + sl_cooldown + entries_done을 보존하면서 런타임 상태 저장."""
        self.state_mgr.save_portfolio(self.portfolio)
        existing = self.state_mgr.load_runtime()
        today_str = str(_dt.date.today())
        is_today = existing.get("date") == today_str
        today_entries   = existing.get("today_entries", []) if is_today else []
        sl_cooldown     = existing.get("sl_cooldown", [])   if is_today else []
        sell_blocked    = existing.get("sell_blocked", [])   if is_today else []
        entries_done    = existing.get("entries_done", False) if is_today else False
        entries_done_at = existing.get("entries_done_at", "") if is_today else ""
        state = {
            "regime":        regime.value,
            "ral_mode":      ral_mode,
            "date":          today_str,
            "today_entries": today_entries,
            "sl_cooldown":   sl_cooldown,
            "sell_blocked":  sell_blocked,
        }
        if entries_done:
            state["entries_done"] = True
            state["entries_done_at"] = entries_done_at
        self.state_mgr.save_runtime(state)

    def _mark_ready(self, positioned: List[Dict]) -> None:
        """StageManager 통과 종목을 READY 상태로 표시."""
        for p in positioned:
            self.tracker.mark_ready(
                p["code"],
                float(p.get("entry_price", 0)),
                int(p.get("sl", 0)),
            )

    def _register_plans(self, results: list, positioned: List[Dict]) -> None:
        pos_map = {p["code"]: p for p in positioned}
        for r in results:
            if not r.rejected and r.side == "BUY":
                plan = pos_map.get(r.code, {})
                plan_sl = float(plan.get("sl", 0))
                plan_tp = float(plan.get("tp", 0))
                plan_entry = float(plan.get("entry_price", 0))

                # 체결가 기준 SL/TP 재조정 (슬리피지 보정)
                if r.exec_price and plan_entry > 0 and plan_sl > 0:
                    exec_p = float(r.exec_price)
                    shift = exec_p - plan_entry
                    if abs(shift) > 1:
                        plan_sl = int(max(1, plan_sl + shift))
                        plan_tp = int(max(exec_p + 1, plan_tp + shift))
                        # MAX_LOSS_CAP 클램프 (체결가 기준)
                        max_loss_cap = abs(getattr(self.config, 'MAX_LOSS_CAP', -0.08))
                        sl_floor = int(exec_p * (1 - max_loss_cap))
                        if plan_sl < sl_floor:
                            plan_sl = sl_floor
                            plan_tp = int(exec_p + (exec_p - plan_sl) * 2.0)
                        print(f"  [SL:ADJUST] {r.code} 체결가={exec_p:.0f} "
                              f"(시그널={plan_entry:.0f}, diff={shift:+.0f}) "
                              f"→ SL={plan_sl}, TP={plan_tp}")

                self.portfolio.register_plan(
                    code     = r.code,
                    tp       = plan_tp,
                    sl       = plan_sl,
                    q_score  = float(plan.get("qscore", 0)),
                    rr_ratio = float(plan.get("rr_ratio", 0)),
                )

    def _log_trades(self, results: list, positioned: List[Dict] = None) -> None:
        """BUG-8 FIX: self.trade_logger 사용 (매번 새 인스턴스 생성하지 않음)."""
        try:
            self.trade_logger.log_all(results, positioned or [])
        except (OSError, ValueError, TypeError) as e:
            print(f"[RuntimeEngine] 거래 로그 기록 실패 (비치명): {e}")

    def _record_sl_cooldown(self, exit_results: list) -> None:
        """SL/GAP_DOWN 청산 종목을 당일 재진입 금지 목록에 등록."""
        if not exit_results:
            return
        sl_codes = []
        closed_codes = []
        for r in exit_results:
            if r.rejected:
                continue
            # v7.9: 모든 매도 체결 종목을 _closed_today에 등록 + 영속화
            if r.side == "SELL" and r.code not in self.portfolio.positions:
                self._closed_today.add(r.code)
                closed_codes.append(r.code)
            close_type = getattr(r, "close_type", "")
            if close_type in ("SL", "GAP_DOWN", "RAL_CRASH"):
                sl_codes.append(r.code)
        if closed_codes:
            self.state_mgr.mark_closed_today(closed_codes)
        if sl_codes:
            self.state_mgr.mark_sl_cooldown(sl_codes)
            print(f"[RuntimeEngine] SL cooldown 등록: {sl_codes}")

    def _reconcile_ghost_fills(self) -> int:
        """GHOST_FILLED 주문을 포트폴리오에 반영. 반환: 반영 건수.

        v7.8 개선:
          - Fill Ledger로 중복 반영 방지 (idempotency)
          - APPLIED 상태 전환 (무한루프 방지)
          - 당일 CLOSED 종목에 BUY ghost → RECONCILE_PENDING 보류
        """
        if not hasattr(self.provider, 'get_ghost_orders'):
            return 0
        ghosts = self.provider.get_ghost_orders()
        reconciled = 0
        for g in ghosts:
            if g.get("status") != "GHOST_FILLED":
                continue
            code      = g.get("code", "")
            side      = g.get("side", "")
            qty       = g.get("filled_qty", 0)
            price     = g.get("avg_fill_price", 0.0)
            order_no  = g.get("order_no", "")
            if not code or qty <= 0 or price <= 0:
                continue

            # v7.8: Fill Ledger 중복 체크 — 이미 반영된 fill이면 APPLIED로 전환
            if self.order_mgr.tracker.is_fill_recorded(order_no, side, qty):
                g["status"] = "APPLIED"
                continue

            # v7.9: 당일 CLOSED 종목에 BUY ghost → 즉시 OPEN 금지 (메모리 + 영속 모두 확인)
            if side == "BUY" and (code in self._closed_today
                                  or code in self.state_mgr.get_closed_today()):
                g["status"] = "RECONCILE_PENDING"
                self._closed_today.add(code)   # 메모리 캐시 동기화
                print(f"[GHOST_FILL] {code} 당일 청산 종목 — RECONCILE_PENDING 보류 "
                      f"(order_no={order_no}, {qty}주 @ {price:,.0f}원)")
                continue

            # Idempotency: BUY → 이미 같은 order_no로 포지션이 있으면 스킵
            if side == "BUY":
                existing = self.portfolio.positions.get(code)
                if existing and getattr(existing, "order_no", "") == order_no:
                    g["status"] = "APPLIED"
                    continue
            # SELL → 이미 포지션이 없으면 스킵 (이미 청산됨)
            elif side == "SELL":
                if code not in self.portfolio.positions:
                    g["status"] = "APPLIED"
                    continue

            # BUG-2 FIX: TIMEOUT_UNCERTAIN에서 예약 차감한 cash만 복원 (2중 차감 방지)
            if side == "BUY" and code in self.order_mgr.timeout_reserved:
                reserve_amount = self.order_mgr.timeout_reserved.pop(code)
                self.portfolio.cash += reserve_amount
                print(f"[GHOST_FILL] cash 예약 복원 {reserve_amount:,.0f}원 (update_position에서 재차감)")

            # 포트폴리오 반영
            sector = ""
            try:
                info = self.provider.get_stock_info(code)
                sector = info.get("sector", "기타")
            except Exception:
                sector = "기타"

            self.portfolio.update_position(code, sector, qty, price, side)
            # v7.6: Ghost fill 종목은 qty_confidence=LOW, needs_reconcile=True
            if side == "BUY" and code in self.portfolio.positions:
                pos = self.portfolio.positions[code]
                pos.order_no = order_no
                pos.stage = "GHOST"
                pos.mark_restricted("GHOST_FILL", confidence="LOW")
                # v7.8: Risk field 보장 (SL/TP=0 방지)
                self.portfolio.ensure_position_risk_fields(code)

            # v7.8: Fill Ledger 기록 + 상태 전환
            self.order_mgr.tracker.record_fill(
                order_no=order_no, side=side, code=code,
                exec_qty=qty, exec_price=price,
                cumulative_qty=qty, source="GHOST",
            )
            g["status"] = "APPLIED"

            self.state_mgr.save_portfolio(self.portfolio)
            reconciled += 1
            print(f"[GHOST_FILL] 포트폴리오 반영: {side} {code} {qty}주 @ {price:,.0f}원 "
                  f"order_no={order_no} → APPLIED (qty_confidence=LOW)")

        return reconciled

    def _try_reconcile_restricted(self) -> int:
        """
        v7.6: restricted 포지션(ghost/timeout/mismatch)에 대해 broker 재검증 시도.
        성공 시 qty_confidence=HIGH로 복원, 실패 시 유지.

        v7.8 개선:
          - broker_hold=0 연속 N회(RECONCILE_FORCE_CLOSE_COUNT) → FORCE_ORPHAN_CLOSED
          - 당일 CLOSED 목록에 있으면 즉시 제거
          - monitor-only 무한 유지 방지
        """
        if not hasattr(self.provider, 'query_sellable_qty'):
            return 0
        _force_close_n = getattr(self.config, 'RECONCILE_FORCE_CLOSE_COUNT', 3)
        resolved = 0
        _removed = []
        for code, pos in list(self.portfolio.positions.items()):
            if not getattr(pos, 'needs_reconcile', False):
                continue

            # v7.9: 당일 CLOSED 종목이면 즉시 제거 (메모리 + 영속 모두 확인)
            if code in self._closed_today or code in self.state_mgr.get_closed_today():
                print(f"[RECONCILE] {code} 당일 청산 확인 → FORCE_ORPHAN_CLOSED")
                del self.portfolio.positions[code]
                self._reconcile_miss_count.pop(code, None)
                _removed.append(code)
                continue

            try:
                info = self.provider.query_sellable_qty(code)
                if info.get("error"):
                    continue  # 조회 실패 → 다음 사이클 재시도
                broker_hold = info["hold_qty"]
                broker_sell = info["sellable_qty"]
                if broker_hold >= pos.quantity and broker_sell > 0:
                    # broker에서 확인 → 정합성 복원
                    old_conf = pos.qty_confidence
                    old_reason = pos.restricted_reason
                    # v7.9: pending sell 해제 (broker 확인 완료)
                    if hasattr(pos, 'clear_pending_sell'):
                        pos.clear_pending_sell()
                    self._reconcile_miss_count.pop(code, None)
                    # 수량 불일치 보정
                    if broker_hold != pos.quantity:
                        print(f"[RECONCILE] {code} 수량 보정: engine={pos.quantity} → broker={broker_hold}")
                        pos.quantity = broker_hold
                    pos.broker_confirmed_qty = broker_hold
                    pos.qty_sellable = broker_sell
                    # v7.9: RECON_BLOCK 해제 — broker 재조회 일치 확인 후에만
                    _can_release = (
                        pos.quantity >= 0
                        and broker_sell <= pos.quantity
                        and pos.qty_pending_sell == 0
                    )
                    if _can_release:
                        _snap_before = (f"hold={pos.quantity} sell={broker_sell} "
                                        f"conf={old_conf} reason={old_reason}")
                        pos.mark_reconciled(sellable_qty=broker_sell)
                        print(f"[RECONCILE] {code} 정합성 복원: "
                              f"before=({_snap_before}) → HIGH/OK "
                              f"(broker_hold={broker_hold}, sellable={broker_sell})")
                    else:
                        print(f"[RECONCILE] {code} broker 확인됨 but 해제 조건 미충족 "
                              f"(qty={pos.quantity} sell={broker_sell} "
                              f"pending={pos.qty_pending_sell}) — restricted 유지")
                    resolved += 1
                elif broker_hold == 0:
                    # v7.8: broker 미보유 카운트 증가
                    cnt = self._reconcile_miss_count.get(code, 0) + 1
                    self._reconcile_miss_count[code] = cnt
                    if cnt >= _force_close_n:
                        print(f"[RECONCILE] {code} broker 미보유 {cnt}회 연속 → FORCE_ORPHAN_CLOSED "
                              f"(engine_qty={pos.quantity}, reason={pos.restricted_reason})")
                        del self.portfolio.positions[code]
                        self._reconcile_miss_count.pop(code, None)
                        self._closed_today.add(code)
                        self.state_mgr.mark_closed_today([code])
                        _removed.append(code)
                    else:
                        print(f"[RECONCILE] {code} broker 미보유 "
                              f"({cnt}/{_force_close_n})")
                else:
                    # broker에 있지만 sellable=0 또는 수량 부족 (T+2 미결제 등)
                    # v7.9: 카운터 리셋하지 않음 — broker가 보유 중이므로 miss 아님, 단 누적도 안 함
                    pos.qty_sellable = broker_sell
                    pos.broker_confirmed_qty = broker_hold
                    print(f"[RECONCILE] {code} T+2 미결제: "
                          f"hold={broker_hold}, sellable={broker_sell} "
                          f"(miss_cnt={self._reconcile_miss_count.get(code, 0)}) — restricted 유지")
            except Exception as e:
                print(f"[RECONCILE] {code} 조회 실패: {e}")
        if resolved or _removed:
            self.state_mgr.save_portfolio(self.portfolio)
        return resolved

    # v7.9: stale refresh 상태
    _refresh_in_flight: bool = False
    _last_refresh_mono: float = 0.0
    _REFRESH_COOLDOWN_SEC: float = 300.0  # 5분 쿨다운

    def _force_price_refresh(self) -> None:
        """v7.9: stale 상태에서 강제 가격 재조회 (TR 폴링 폴백).
        쿨다운 300초, in-flight guard 포함."""
        import time as _time

        # in-flight guard
        if self._refresh_in_flight:
            return
        # cooldown guard
        now_mono = _time.monotonic()
        if now_mono - self._last_refresh_mono < self._REFRESH_COOLDOWN_SEC:
            return

        self._refresh_in_flight = True
        self._last_refresh_mono = now_mono
        self._last_stale_refresh_ts = self.portfolio.get_stale_age_sec()
        refreshed = 0
        try:
            # 1) 개별 종목 현재가 TR 재조회
            for code in list(self.portfolio.positions.keys()):
                try:
                    p = self.provider.get_current_price(code)
                    if p and p > 0:
                        self.portfolio.positions[code].current_price = float(p)
                        if float(p) > self.portfolio.positions[code].high_watermark:
                            self.portfolio.positions[code].high_watermark = float(p)
                        refreshed += 1
                except Exception as e:
                    print(f"[STALE:REFRESH] {code} 가격 조회 실패: {e}")
            # 2) account snapshot (현금 재확인)
            if hasattr(self.provider, 'query_account_summary'):
                try:
                    summary = self.provider.query_account_summary()
                    if not summary.get("error"):
                        kw_cash = summary.get("available_cash", 0)
                        if kw_cash > 0:
                            self.portfolio.cash = float(max(0, kw_cash))
                except Exception as e:
                    print(f"[STALE:REFRESH] account 조회 실패: {e}")
            if refreshed > 0:
                print(f"[STALE:REFRESH] {refreshed}종목 가격 갱신 완료 "
                      f"(equity={self.portfolio.get_current_equity():,.0f})")
        finally:
            self._refresh_in_flight = False

    def _record_sell_blocked(self, exit_results: list) -> None:
        """매도 거부된 종목을 당일 재시도 차단 + 포지션 mismatch 마킹.
        v7.6: REJECTED_BROKER → broker_position_mismatch 이벤트로 분류.
        영속화: runtime_state.json에 저장 → 재시작 시 복원."""
        if not exit_results:
            return
        new_blocked = []
        for r in exit_results:
            if not r.rejected:
                continue
            reason = r.reject_reason or ""
            # v7.6: broker rejection OR SELL_RESTRICTED 모두 차단 등록
            is_broker_reject = "order rejected:" in reason
            is_restricted = "SELL_RESTRICTED" in reason
            if is_broker_reject or is_restricted:
                self._sell_blocked.add(r.code)
                new_blocked.append(r.code)
                # v7.6: 포지션을 POSITION_MISMATCH로 마킹 (반복 청산 시도 방지)
                pos = self.portfolio.positions.get(r.code)
                if pos:
                    mismatch_reason = "SELL_REJECTED" if is_broker_reject else pos.restricted_reason
                    if pos.qty_confidence == "HIGH":
                        pos.mark_restricted(mismatch_reason, confidence="LOW")
                    print(f"[RuntimeEngine] 매도 차단 등록: {r.code} "
                          f"(qty_confidence={pos.qty_confidence}, "
                          f"reason={mismatch_reason}, "
                          f"사유: {reason[:80]})")
                else:
                    print(f"[RuntimeEngine] 매도 차단 등록: {r.code} (포지션 없음, 사유: {reason[:80]})")
        if new_blocked:
            self.state_mgr.mark_sell_blocked(new_blocked)
            self.state_mgr.save_portfolio(self.portfolio)
