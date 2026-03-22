"""
RiskManager (Core)
==================
포트폴리오 수준 리스크 관리자.
매 사이클 시작 시 evaluate() → NORMAL / SOFT_STOP / DAILY_KILL / HARD_STOP 결정.

상태 전이 (단방향 에스컬레이션, 하루 내 하향 불가):
  NORMAL → SOFT_STOP → DAILY_KILL → HARD_STOP
  - SOFT_STOP: 신규 진입 중단, 손실 최대 1종목 청산 (1회만)
  - DAILY_KILL: 신규 진입 완전 차단, 포지션 유지 (SL만 감시)
  - HARD_STOP: 전 포지션 강제 청산

※ 포지션 사이징(TP/SL 계산)은 strategy/stage_manager.py 에서 수행.
"""

from core.portfolio_manager import PortfolioManager

_MODE_RANK = {"NORMAL": 0, "SOFT_STOP": 1, "DAILY_KILL": 2, "HARD_STOP": 3}

# NORMAL 반복 시 주기 요약 로그 간격 (평가 횟수 기준, 60초 주기면 ~10분)
_NORMAL_SUMMARY_INTERVAL = 10


class RiskManager:

    def __init__(self, portfolio: PortfolioManager, executor):
        self.portfolio = portfolio
        self.executor  = executor
        self._peak_mode: str = "NORMAL"          # 당일 최고 에스컬레이션
        self._soft_stop_acted: bool = False       # SOFT_STOP 청산 1회 플래그
        self._last_logged_mode: str = ""          # 중복 로그 방지
        self._normal_eval_count: int = 0          # NORMAL 연속 평가 카운터
        self.risk_confidence: str = "HIGH"        # v7.7: HIGH | DEGRADED

    def reset_daily(self) -> None:
        """일일 초기화 (EOD 또는 날짜 변경 시 호출)."""
        self._peak_mode = "NORMAL"
        self._soft_stop_acted = False
        self._last_logged_mode = ""
        self._normal_eval_count = 0
        self.risk_confidence = "HIGH"

    def evaluate(self, snap_ts: str = "") -> str:
        """
        현재 리스크 모드를 평가하고 필요한 조치(청산 등)를 즉시 실행한다.
        반환: 'NORMAL' | 'SOFT_STOP' | 'DAILY_KILL' | 'HARD_STOP'

        단방향 에스컬레이션: 한번 올라간 모드는 당일 내 하향되지 않는다.
        snap_ts: 가격 스냅샷 타임스탬프 (디버깅용 로그 태그)
        """
        raw_mode = self.portfolio.risk_mode()
        _tag = f"src=live snap={snap_ts}" if snap_ts else "src=live"

        # v7.7: stale equity 감지
        is_stale = self.portfolio.check_stale_equity()
        self.risk_confidence = "DEGRADED" if is_stale else "HIGH"

        _eq = self.portfolio.get_current_equity()
        _base = self.portfolio.prev_close_equity
        _pnl = self.portfolio.get_daily_pnl_pct()

        # 로그 정책: 상태 전이 시 즉시, NORMAL 반복 시 ~10분 주기 요약
        _mode_changed = (raw_mode != self._last_logged_mode
                         or self._peak_mode != "NORMAL")
        if _mode_changed:
            self._normal_eval_count = 0
            print(f"[RiskEval] {_tag} equity={_eq:,.0f} prev_close={_base:,.0f} "
                  f"pnl={_pnl:.2%} raw={raw_mode} peak={self._peak_mode}")
        else:
            # NORMAL 반복 — 주기적 요약만
            self._normal_eval_count += 1
            if self._normal_eval_count % _NORMAL_SUMMARY_INTERVAL == 0:
                print(f"[RiskEval] NORMAL 유지 {self._normal_eval_count}회 "
                      f"(snap={snap_ts} equity={_eq:,.0f} pnl={_pnl:.2%})")

        # 단방향 래치: 실시간 모드와 기존 peak 중 상위 유지
        if _MODE_RANK.get(raw_mode, 0) > _MODE_RANK.get(self._peak_mode, 0):
            self._peak_mode = raw_mode
        mode = self._peak_mode
        # summary() 표시용 래치 모드 동기화
        self.portfolio._display_risk_mode = mode

        if mode == "HARD_STOP":
            print(f"[HARD_STOP] {_tag} 월 DD {self.portfolio.get_monthly_dd_pct():.2%} "
                  f"— 전 포지션 강제 청산")
            self._force_liquidate_all()

        elif mode == "DAILY_KILL":
            if self._last_logged_mode != "DAILY_KILL":
                print(f"[DAILY_KILL] {_tag} pnl={_pnl:.2%} "
                      f"(equity={_eq:,.0f} vs prev_close={_base:,.0f}) "
                      f"— 신규 진입 완전 차단 (포지션 유지, SL만 감시)")

        elif mode == "SOFT_STOP":
            if not self._soft_stop_acted:
                print(f"[SOFT_STOP] {_tag} pnl={_pnl:.2%} "
                      f"(equity={_eq:,.0f} vs prev_close={_base:,.0f}) "
                      f"— 신규 진입 중단 / 손실 최대 종목 1개 청산 (1회)")
                self._reduce_exposure()
                self._soft_stop_acted = True
            # 이미 1회 청산 완료 → 추가 청산 없이 진입만 차단

        self._last_logged_mode = mode
        return mode

    def _force_liquidate_all(self) -> None:
        from runtime.order_executor import Order
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            print("[HARD_STOP] 청산 대상 없음")
            return
        for code in targets:
            pos = self.portfolio.positions.get(code)
            if not pos:
                continue
            # v7.6: restricted 포지션은 자동 청산 금지
            if getattr(pos, 'is_restricted', False):
                print(f"  [HARD_STOP] {code} 자동청산 차단: "
                      f"qty_confidence={pos.qty_confidence}, reason={pos.restricted_reason}")
                continue
            sell_qty = getattr(pos, 'effective_sellable', pos.quantity)
            if sell_qty <= 0:
                print(f"  [HARD_STOP] {code} 매도 차단: qty_sellable=0 (broker restriction)")
                continue
            order = Order(code=code, sector=pos.sector, side="SELL",
                          quantity=sell_qty, price=pos.current_price)
            result = self.executor.execute(order)
            print(f"  → {result}")

    def _reduce_exposure(self) -> None:
        from runtime.order_executor import Order
        targets = self.portfolio.get_liquidation_targets()
        if not targets:
            return
        code = targets[0]
        pos  = self.portfolio.positions.get(code)
        if not pos:
            return
        # v7.6: restricted 포지션은 스킵 → 다음 후보
        if getattr(pos, 'is_restricted', False):
            print(f"[SOFT_STOP] {code} 자동청산 차단: "
                  f"qty_confidence={pos.qty_confidence} — 다음 후보 탐색")
            for alt_code in targets[1:]:
                alt_pos = self.portfolio.positions.get(alt_code)
                if alt_pos and not getattr(alt_pos, 'is_restricted', False):
                    code, pos = alt_code, alt_pos
                    break
            else:
                print("[SOFT_STOP] 청산 가능 종목 없음 (모두 restricted)")
                return
        sell_qty = getattr(pos, 'effective_sellable', pos.quantity)
        if sell_qty <= 0:
            print(f"[SOFT_STOP] {code} 매도 차단: qty_sellable=0 (broker restriction)")
            return
        order = Order(code=code, sector=pos.sector, side="SELL",
                      quantity=sell_qty, price=pos.current_price)
        result = self.executor.execute(order)
        print(f"[SOFT_STOP 축소] {result}")
