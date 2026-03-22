"""
SignalTracker (v7.4)
====================
종목별 진행 상태 실시간 추적.

상태 흐름:
  SIGNAL  -> 시그널 CSV에서 로드됨
  TICK    -> 체결강도 관측 완료 (STRONG/NEUTRAL/WEAK/DROP)
  READY   -> 필터 통과, 매수 대기 중
  HOLDING -> 보유 중 (실시간 PnL 추적)
  EXITED  -> 청산 완료 (SL/TP/GAP_DOWN/MAX_HOLD 등)
  BLOCKED -> 진입 차단됨 (block_code + 사유 기록)

Block Codes (통계 집계용):
  TICK_DROP    체결강도 DROP (< 80)
  ENTRY_DEV    signal entry 대비 현재가 괴리 > +-7%
  SL_BUFFER    현재가가 SL 이하 또는 버퍼 1% 미만
  OPEN_GAP     갭업 > 8% (StageA 갭 필터)
  SECTOR_CAP   섹터 한도 초과
  ATR_HIGH     ATR 변동성 순위 초과
  BEAR_RS      BEAR 모드 RS 미달
  SLOT_FULL    포지션 슬롯 가득참
  COOLDOWN     SL/GAP_DOWN 청산 당일 재진입 금지
  RE_ENTERED   당일 이미 진입
  TIME_CUTOFF  12:00 이후 진입 차단
  MARGIN       증거금/현금 부족
  LIQUIDITY    유동성 부족
  RISK_STOP    SOFT_STOP/DAILY_KILL/HARD_STOP
  ENTRY_FILTER 기타 필터
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from data.name_lookup import get_name


# ── Block Code 매핑 (filter_reason 문자열 → 코드 자동 추론) ─────────────

_REASON_TO_CODE = [
    ("tick DROP",        "TICK_DROP"),
    ("entry gap",        "ENTRY_DEV"),
    ("entry 괴리",       "ENTRY_DEV"),
    ("SL 버퍼",          "SL_BUFFER"),
    ("현재가",           "SL_BUFFER"),    # "현재가 <= SL"
    ("갭",               "OPEN_GAP"),
    ("섹터",             "SECTOR_CAP"),
    ("ATR",              "ATR_HIGH"),
    ("BEAR RS",          "BEAR_RS"),
    ("슬롯",             "SLOT_FULL"),
    ("cooldown",         "COOLDOWN"),
    ("already entered",  "RE_ENTERED"),
    ("시간대",           "TIME_CUTOFF"),
    ("cutoff",           "TIME_CUTOFF"),
    ("증거금",           "MARGIN"),
    ("현금 부족",        "MARGIN"),
    ("유동성",           "LIQUIDITY"),
    ("SOFT_STOP",        "RISK_STOP"),
    ("DAILY_KILL",       "RISK_STOP"),
    ("HARD_STOP",        "RISK_STOP"),
]


def _infer_block_code(reason: str) -> str:
    """filter_reason 문자열에서 block_code를 추론."""
    if not reason:
        return "ENTRY_FILTER"
    reason_lower = reason.lower()
    for keyword, code in _REASON_TO_CODE:
        if keyword.lower() in reason_lower:
            return code
    return "ENTRY_FILTER"


@dataclass
class StockStatus:
    code:          str
    name:          str = ""
    stage:         str = ""       # A / B
    sector:        str = ""
    signal_entry:  int = 0        # batch CSV entry price
    signal_tp:     int = 0
    signal_sl:     int = 0
    rs_composite:  float = 0.0

    # -- runtime tracking --
    phase:         str = "SIGNAL"  # SIGNAL / TICK / READY / HOLDING / EXITED / BLOCKED
    block_code:    str = ""        # TICK_DROP / ENTRY_DEV / SL_BUFFER / OPEN_GAP / ...
    tick_strength: float = 0.0
    tick_verdict:  str = ""        # STRONG / NEUTRAL / WEAK / DROP / ""
    filter_reason: str = ""        # 차단 상세 사유
    order_price:   float = 0.0     # 체결가
    order_qty:     int = 0
    order_rejected:bool = False
    reject_reason: str = ""

    # -- position tracking --
    current_price: float = 0.0
    pnl_pct:       float = 0.0
    live_tp:       int = 0
    live_sl:       int = 0
    sl_dist_pct:   float = 0.0    # SL까지 남은 거리 %
    tp_dist_pct:   float = 0.0    # TP까지 남은 거리 %

    # -- ready tracking (매수 대기 상태) --
    ready_gap_pct: float = 0.0    # signal entry 대비 현재가 괴리 %
    ready_sl_buf:  float = 0.0    # SL 버퍼 %

    # -- exit tracking --
    close_type:    str = ""        # SL / GAP_DOWN / TP / MAX_HOLD / RS_EXIT 등
    close_price:   float = 0.0
    close_pnl_pct: float = 0.0

    # -- cooldown --
    cooldown:      bool = False

    updated_at:    str = ""

    def set_blocked(self, reason: str, code: str = "") -> None:
        """BLOCKED 상태로 전환 + block_code 자동 추론."""
        self.phase = "BLOCKED"
        self.filter_reason = reason
        self.block_code = code if code else _infer_block_code(reason)
        self.updated_at = datetime.now().strftime("%H:%M:%S")


class SignalTracker:

    def __init__(self):
        self.stocks: Dict[str, StockStatus] = {}

    # ── Phase 0: 시그널 로드 ─────────────────────────────────────────────

    def init_signals(self, signals: List[Dict[str, Any]]) -> None:
        """시그널 CSV 로드 결과로 초기화."""
        self.stocks.clear()
        for sig in signals:
            code = sig["code"]
            self.stocks[code] = StockStatus(
                code=code,
                name=get_name(code),
                stage=sig.get("stage", "B"),
                sector=sig.get("sector", ""),
                signal_entry=int(sig.get("entry", 0)),
                signal_tp=int(sig.get("tp", 0)),
                signal_sl=int(sig.get("sl", 0)),
                rs_composite=float(sig.get("rs_composite", sig.get("qscore", 0))),
                phase="SIGNAL",
                updated_at=datetime.now().strftime("%H:%M:%S"),
            )

    # ── Phase 0.5: 체결강도 결과 ─────────────────────────────────────────

    def update_tick(self, code: str, avg_strength: float, verdict: str) -> None:
        s = self.stocks.get(code)
        if not s:
            return
        s.tick_strength = avg_strength
        s.tick_verdict = verdict
        if verdict == "DROP":
            s.set_blocked(f"tick DROP ({avg_strength:.0f})", "TICK_DROP")
        else:
            s.phase = "TICK"
        s.updated_at = datetime.now().strftime("%H:%M:%S")

    def update_tick_from_signals(self, pre_signals: List[Dict], post_signals: List[Dict]) -> None:
        """체결강도 필터 전후 시그널 비교로 DROP 종목 식별."""
        post_codes = {s["code"] for s in post_signals}
        for s in pre_signals:
            code = s["code"]
            if code in self.stocks:
                if code not in post_codes:
                    self.stocks[code].set_blocked("tick DROP", "TICK_DROP")
                    self.stocks[code].tick_verdict = "DROP"
                else:
                    if self.stocks[code].phase == "SIGNAL":
                        self.stocks[code].phase = "TICK"
                self.stocks[code].updated_at = datetime.now().strftime("%H:%M:%S")

    # ── Phase 3-4: 필터 결과 ─────────────────────────────────────────────

    def update_filter(self, code: str, passed: bool, reason: str = "",
                      block_code: str = "") -> None:
        s = self.stocks.get(code)
        if not s:
            return
        if passed:
            s.phase = "FILTER"
        else:
            s.set_blocked(reason, block_code)
        s.updated_at = datetime.now().strftime("%H:%M:%S")

    # ── Phase 3-4: 주문 결과 ─────────────────────────────────────────────

    def update_order(self, code: str, exec_price: float, quantity: int,
                     rejected: bool = False, reason: str = "") -> None:
        s = self.stocks.get(code)
        if not s:
            return
        if rejected:
            s.set_blocked(reason)
            s.order_rejected = True
            s.reject_reason = reason
        else:
            s.phase = "HOLDING"
            s.order_price = exec_price
            s.order_qty = quantity
            s.current_price = exec_price
        s.updated_at = datetime.now().strftime("%H:%M:%S")

    def update_order_results(self, results: list, positioned: list) -> None:
        """OrderManager 결과 일괄 반영."""
        pos_map = {p["code"]: p for p in positioned} if positioned else {}
        for r in results:
            plan = pos_map.get(r.code, {})
            if r.rejected:
                self.update_order(r.code, 0, 0, rejected=True, reason=r.reject_reason)
            else:
                self.update_order(r.code, r.exec_price, r.quantity)
                s = self.stocks.get(r.code)
                if s:
                    s.live_tp = int(plan.get("tp", s.signal_tp))
                    s.live_sl = int(plan.get("sl", s.signal_sl))

    # ── READY 상태: 필터 통과했지만 아직 미체결 ──────────────────────────

    def mark_ready(self, code: str, current_price: float, sl: int) -> None:
        """StageManager에서 _size_position 통과 후, 주문 전 READY 표시."""
        s = self.stocks.get(code)
        if not s:
            return
        s.phase = "READY"
        s.current_price = current_price
        if s.signal_entry > 0:
            s.ready_gap_pct = (current_price - s.signal_entry) / s.signal_entry
        if current_price > 0 and sl > 0:
            s.ready_sl_buf = (current_price - sl) / current_price
        s.updated_at = datetime.now().strftime("%H:%M:%S")

    # ── Monitor: 포지션 가격 업데이트 ─────────────────────────────────────

    def update_prices(self, portfolio) -> None:
        """PortfolioManager의 현재 포지션 정보로 가격/PnL 업데이트."""
        # v7.6: portfolio 참조 저장 (dashboard에서 sellable/confidence 표시용)
        self._portfolio_ref = portfolio
        now_str = datetime.now().strftime("%H:%M:%S")
        for code, pos in portfolio.positions.items():
            s = self.stocks.get(code)
            if not s:
                # 시그널에 없던 종목 (AccountSync 복원 등)
                s = StockStatus(
                    code=code, name=get_name(code),
                    phase="HOLDING",
                    order_price=pos.avg_price,
                    order_qty=pos.quantity,
                    sector=pos.sector,
                    stage=getattr(pos, "stage", ""),
                )
                self.stocks[code] = s

            if pos.quantity > 0:
                s.phase = "HOLDING"

            s.current_price = pos.current_price
            s.pnl_pct = pos.unrealized_pnl_pct
            s.live_tp = int(pos.tp) if pos.tp else s.live_tp
            s.live_sl = int(pos.sl) if pos.sl else s.live_sl
            # Position.stage와 동기화 (StageA/B 대시보드 표시 정확도)
            actual_stage = getattr(pos, "stage", "")
            if actual_stage:
                s.stage = actual_stage

            # SL/TP 거리 계산
            if s.live_sl > 0 and s.current_price > 0:
                s.sl_dist_pct = (s.current_price - s.live_sl) / s.current_price
            if s.live_tp > 0 and s.current_price > 0:
                s.tp_dist_pct = (s.live_tp - s.current_price) / s.current_price

            s.updated_at = now_str

    # ── Monitor: 청산 결과 ────────────────────────────────────────────────

    def update_exit(self, code: str, close_type: str, close_price: float = 0,
                    pnl_pct: float = 0) -> None:
        s = self.stocks.get(code)
        if not s:
            return
        s.phase = "EXITED"
        s.close_type = close_type
        s.close_price = close_price
        s.close_pnl_pct = pnl_pct
        s.updated_at = datetime.now().strftime("%H:%M:%S")

    def update_exit_results(self, exit_results: list) -> None:
        """ExitLogic 결과 일괄 반영."""
        for r in exit_results:
            if r.rejected:
                continue
            pnl_pct = getattr(r, "pnl_pct", 0)
            close_type = getattr(r, "close_type", "UNKNOWN")
            self.update_exit(r.code, close_type, r.exec_price, pnl_pct)

    # ── Cooldown 반영 ─────────────────────────────────────────────────────

    def mark_cooldown(self, codes: set) -> None:
        for code in codes:
            s = self.stocks.get(code)
            if s:
                s.cooldown = True
                if s.phase not in ("HOLDING", "EXITED"):
                    s.set_blocked("SL cooldown", "COOLDOWN")

    def mark_already_entered(self, codes: set) -> None:
        for code in codes:
            s = self.stocks.get(code)
            if s and s.phase in ("SIGNAL", "TICK"):
                s.set_blocked("already entered today", "RE_ENTERED")

    # ── 잔여 미처리 종목 일괄 BLOCKED ─────────────────────────────────────

    def finalize_entries(self) -> None:
        """run_entries() 완료 후 -- SIGNAL/TICK/FILTER/READY 중 HOLDING 안 된 종목 BLOCKED."""
        for s in self.stocks.values():
            if s.phase in ("SIGNAL", "TICK", "FILTER", "READY"):
                if not s.filter_reason:
                    s.set_blocked("entry filter", "ENTRY_FILTER")
                else:
                    # filter_reason 이미 있으면 block_code만 보충
                    s.phase = "BLOCKED"
                    if not s.block_code:
                        s.block_code = _infer_block_code(s.filter_reason)
                s.updated_at = datetime.now().strftime("%H:%M:%S")

    # ── 대시보드 출력 ─────────────────────────────────────────────────────

    def print_dashboard(self, compact: bool = False) -> None:
        """종목별 상태 대시보드 출력."""
        if not self.stocks:
            return

        now_str = datetime.now().strftime("%H:%M:%S")
        phase_order = {"HOLDING": 0, "READY": 1, "EXITED": 2,
                       "TICK": 3, "FILTER": 4, "SIGNAL": 5, "BLOCKED": 6}

        sorted_stocks = sorted(
            self.stocks.values(),
            key=lambda s: (phase_order.get(s.phase, 9), -s.rs_composite)
        )

        # 카운트 (v7.8: SL=0 HOLDING → ZOMBIE로 분리)
        counts = {}
        _zombie_n = 0
        for s in sorted_stocks:
            if s.phase == "HOLDING" and s.live_sl <= 0:
                _zombie_n += 1
            else:
                counts[s.phase] = counts.get(s.phase, 0) + 1
        if _zombie_n:
            counts["ZOMBIE"] = _zombie_n
        count_str = " | ".join(f"{k}={v}" for k, v in sorted(counts.items()))

        print(f"\n{'='*78}")
        print(f"  Signal Dashboard  [{now_str}]  {count_str}")
        print(f"{'='*78}")

        if compact:
            self._print_compact(sorted_stocks)
        else:
            self._print_full(sorted_stocks)

        print(f"{'='*78}\n")

    def _print_full(self, stocks: list) -> None:
        """Full dashboard with details."""
        # HOLDING (v7.6: hold/sellable/confidence 분리 표시)
        # v7.8: SL=0 포지션은 ZOMBIE로 분리 표시
        _all_hold = [s for s in stocks if s.phase == "HOLDING"]
        holdings = [s for s in _all_hold if s.live_sl > 0]
        zombies = [s for s in _all_hold if s.live_sl <= 0]
        if zombies:
            print(f"\n  -- ZOMBIE ({len(zombies)}) -- SL 미설정, 거래 차단 --")
            for s in zombies:
                label = f"{s.name}({s.code})"
                if len(label) > 15:
                    label = label[:15]
                print(f"  {label:<16} {s.stage:>5} avg={s.order_price:>9,.0f} "
                      f"cur={s.current_price:>9,.0f} SL=0 TP={s.live_tp:>8,}")
        if holdings:
            print(f"\n  -- HOLDING ({len(holdings)}) --")
            print(f"  {'종목':<16} {'Stage':>5} {'체결가':>9} {'현재가':>9} "
                  f"{'PnL':>7} {'SL거리':>6} {'TP거리':>6} {'SL':>8} {'TP':>8} {'수량':>12} {'상태'}")
            for s in holdings:
                pnl_sign = "+" if s.pnl_pct >= 0 else ""
                sl_warn = " (!)" if 0 < s.sl_dist_pct < 0.02 else ""
                label = f"{s.name}({s.code})"
                if len(label) > 15:
                    label = label[:15]
                # v7.6: portfolio에서 sellable/confidence 가져오기
                qty_str = ""
                conf_str = ""
                if hasattr(self, '_portfolio_ref') and self._portfolio_ref:
                    pos = self._portfolio_ref.positions.get(s.code)
                    if pos:
                        sell_s = f"{pos.qty_sellable}" if pos.qty_sellable >= 0 else "?"
                        qty_str = f"{pos.quantity}/{sell_s}"
                        conf = getattr(pos, 'qty_confidence', 'HIGH')
                        if conf != "HIGH":
                            conf_str = f"[{conf}]"
                            reason = getattr(pos, 'restricted_reason', '')
                            if reason:
                                conf_str += f"({reason})"
                if not qty_str:
                    qty_str = f"{s.order_qty}"
                print(f"  {label:<16} {s.stage:>5} {s.order_price:>9,.0f} {s.current_price:>9,.0f} "
                      f"{pnl_sign}{s.pnl_pct:>6.1%} {s.sl_dist_pct:>5.1%}{sl_warn} "
                      f"{s.tp_dist_pct:>5.1%} {s.live_sl:>8,} {s.live_tp:>8,} {qty_str:>12} {conf_str}")

        # READY
        ready = [s for s in stocks if s.phase == "READY"]
        if ready:
            print(f"\n  -- READY ({len(ready)}) --")
            print(f"  {'종목':<16} {'Stage':>5} {'Signal':>8} {'현재가':>9} "
                  f"{'Gap':>7} {'SLbuf':>6}")
            for s in ready:
                label = f"{s.name}({s.code})"
                if len(label) > 15:
                    label = label[:15]
                gap_sign = "+" if s.ready_gap_pct >= 0 else ""
                print(f"  {label:<16} {s.stage:>5} {s.signal_entry:>8,} {s.current_price:>9,.0f} "
                      f"{gap_sign}{s.ready_gap_pct:>6.1%} {s.ready_sl_buf:>5.1%}")

        # EXITED
        exited = [s for s in stocks if s.phase == "EXITED"]
        if exited:
            print(f"\n  -- EXITED ({len(exited)}) --")
            print(f"  {'종목':<16} {'사유':<10} {'체결가':>9} {'청산가':>9} {'PnL':>7}")
            for s in exited:
                pnl_sign = "+" if s.close_pnl_pct >= 0 else ""
                label = f"{s.name}({s.code})"
                if len(label) > 15:
                    label = label[:15]
                print(f"  {label:<16} {s.close_type:<10} {s.order_price:>9,.0f} "
                      f"{s.close_price:>9,.0f} {pnl_sign}{s.close_pnl_pct:>6.1%}")

        # BLOCKED
        blocked = [s for s in stocks if s.phase == "BLOCKED"]
        if blocked:
            print(f"\n  -- BLOCKED ({len(blocked)}) --")
            print(f"  {'종목':<16} {'Code':<13} {'Stage':>5} {'Signal':>8} {'RS':>5} {'사유'}")
            for s in blocked:
                label = f"{s.name}({s.code})"
                if len(label) > 15:
                    label = label[:15]
                reason = s.filter_reason[:30] if s.filter_reason else ""
                bc = f"[{s.block_code}]" if s.block_code else "[?]"
                print(f"  {label:<16} {bc:<13} {s.stage:>5} {s.signal_entry:>8,} "
                      f"{s.rs_composite:>5.2f} {reason}")

        # SIGNAL/TICK (not yet processed)
        pending = [s for s in stocks if s.phase in ("SIGNAL", "TICK", "FILTER")]
        if pending:
            print(f"\n  -- PENDING ({len(pending)}) --")
            for s in pending:
                label = f"{s.name}({s.code})"
                tick_info = f"tick={s.tick_strength:.0f}({s.tick_verdict})" if s.tick_verdict else ""
                print(f"  {label:<16} {s.phase:<8} {s.stage:>3} {tick_info}")

    def _print_compact(self, stocks: list) -> None:
        """One-line per stock compact view."""
        PHASE_ICON = {
            "HOLDING": "[H]", "READY": "[R]", "EXITED": "[X]", "BLOCKED": "[B]",
            "SIGNAL": "[S]", "TICK": "[T]", "FILTER": "[F]",
        }
        for s in stocks:
            icon = PHASE_ICON.get(s.phase, "[?]")
            label = f"{s.name}({s.code})"
            if len(label) > 16:
                label = label[:16]

            if s.phase == "HOLDING":
                # v7.8: SL=0 zombie 표시
                if s.live_sl <= 0:
                    icon = "[Z]"
                    detail = f"@ {s.current_price:,.0f} SL=0 ZOMBIE"
                else:
                    pnl = f"{'+' if s.pnl_pct >= 0 else ''}{s.pnl_pct:.1%}"
                    sl_warn = " (!)" if 0 < s.sl_dist_pct < 0.02 else ""
                    detail = f"@ {s.current_price:,.0f} {pnl} SL:{s.sl_dist_pct:.1%}{sl_warn}"
            elif s.phase == "READY":
                gap = f"{'+' if s.ready_gap_pct >= 0 else ''}{s.ready_gap_pct:.1%}"
                detail = f"gap {gap}  SLbuf {s.ready_sl_buf:.1%}"
            elif s.phase == "EXITED":
                pnl = f"{'+' if s.close_pnl_pct >= 0 else ''}{s.close_pnl_pct:.1%}"
                detail = f"{s.close_type} {pnl}"
            elif s.phase == "BLOCKED":
                bc = f"[{s.block_code}]" if s.block_code else ""
                detail = f"{bc} {s.filter_reason[:28]}"
            else:
                detail = f"RS={s.rs_composite:.2f}"

            print(f"  {icon} {label:<17} {detail}")

    # ── 요약 통계 (EOD 집계용) ────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        counts = {}
        for s in self.stocks.values():
            counts[s.phase] = counts.get(s.phase, 0) + 1

        # Block code 통계
        block_stats: Dict[str, int] = {}
        for s in self.stocks.values():
            if s.phase == "BLOCKED" and s.block_code:
                block_stats[s.block_code] = block_stats.get(s.block_code, 0) + 1

        holding_pnl = [s.pnl_pct for s in self.stocks.values() if s.phase == "HOLDING"]
        exited_pnl  = [s.close_pnl_pct for s in self.stocks.values() if s.phase == "EXITED"]

        return {
            "total_signals": len(self.stocks),
            "phase_counts": counts,
            "block_stats": block_stats,
            "holding_avg_pnl": sum(holding_pnl) / len(holding_pnl) if holding_pnl else 0,
            "exited_avg_pnl": sum(exited_pnl) / len(exited_pnl) if exited_pnl else 0,
            "exited_win_rate": sum(1 for p in exited_pnl if p > 0) / len(exited_pnl) if exited_pnl else 0,
        }

    def print_block_summary(self) -> None:
        """EOD에 차단 사유별 통계 출력."""
        stats = self.summary()
        block_stats = stats.get("block_stats", {})
        if not block_stats:
            return
        print(f"\n[BlockStats] 차단 사유별 집계:")
        for code, cnt in sorted(block_stats.items(), key=lambda x: -x[1]):
            print(f"  {code:<15} {cnt}건")
