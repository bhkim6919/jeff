"""
PortfolioManager
================
현금 + 보유 포지션 관리. Gen3에서 position_tracker.Position을 사용한다.

6중 게이트 (can_enter):
  1. 일일 손실 한도
  2. 월간 DD 한도
  3. 최대 보유 종목 수
  4. 종목당 최대 비중
  5. 섹터 노출 한도
  6. 총 노출도 한도
"""

from datetime import date, datetime
from typing import Dict, List, Tuple

from config import Gen3Config
from core.position_tracker import Position


class PortfolioManager:

    def __init__(self, config: Gen3Config):
        self.config = config
        self.positions: Dict[str, Position] = {}
        self.cash: float = config.initial_cash

        self.prev_close_equity: float = config.initial_cash
        self.peak_equity:       float = config.initial_cash
        self._peak_month:       int   = date.today().month
        # v7.8: sync 당일 broker 기준 equity (DAILY_KILL 기준 정책적 선택용)
        self.synced_broker_equity: float = 0.0

        # 리스크 한도
        self.daily_loss_limit  = config.daily_loss_limit
        self.daily_kill_limit  = getattr(config, 'daily_kill_limit', -0.04)
        self.monthly_dd_limit  = config.monthly_dd_limit
        self.max_exposure      = config.max_exposure
        self.max_per_stock     = config.max_per_stock
        # BUG-2 FIX: BULL/BEAR 별도 관리
        self.max_pos_bull      = getattr(config, 'MAX_POS_BULL', config.max_positions)
        self.max_pos_bear      = getattr(config, 'MAX_POS_BEAR', 8)
        self.max_positions     = self.max_pos_bull   # 기본값
        self.max_sector_exp    = config.max_sector_exp
        self._current_regime: str = "BULL"

        # 부대비용 누적 추적
        self._cumul_fee_buy:  float = 0.0
        self._cumul_fee_sell: float = 0.0
        self._cumul_tax:      float = 0.0

        # v7.7: stale equity 감지
        self._last_equity_change_ts: datetime = datetime.now()
        self._last_equity_value: float = 0.0
        self._stale_warned: bool = False

    def set_regime_limits(self, regime_value: str) -> None:
        """레짐에 따라 max_positions 동적 조정. BULL=20, BEAR/SIDEWAYS=8.
        소액 계좌면 자본 기반으로 추가 축소."""
        self._current_regime = regime_value
        if regime_value == "BULL":
            base = self.max_pos_bull
        else:
            base = self.max_pos_bear

        # 소액 계좌: 자본 기반 포지션 수 축소
        equity = self.get_current_equity()
        cap = self._equity_based_max_pos(equity, base)
        self.max_positions = cap

        if cap < base:
            print(f"[PortfolioManager] max_positions={cap} "
                  f"(regime={regime_value}, 소액 조정: 자본={equity:,.0f}원)")
        else:
            print(f"[PortfolioManager] max_positions={cap} (regime={regime_value})")

    @staticmethod
    def _equity_based_max_pos(equity: float, config_max: int) -> int:
        """자본 규모에 따른 최대 포지션 수 결정.
        종목당 최소 20만원 이상 투자 가능하도록 조정."""
        MIN_PER_POS = 200_000   # 종목당 최소 투자금 20만원
        equity_based = max(1, int(equity / MIN_PER_POS))
        return min(config_max, equity_based)

    # ── 평가 ─────────────────────────────────────────────────────────────────

    def get_current_equity(self) -> float:
        return self.cash + sum(p.market_value for p in self.positions.values())

    def get_daily_pnl_pct(self) -> float:
        """일간 PnL. v7.8: synced_broker_equity 우선 사용 (sync 당일 정확도 보장)."""
        base = self.synced_broker_equity if self.synced_broker_equity > 0 else self.prev_close_equity
        if base == 0:
            return 0.0
        return (self.get_current_equity() - base) / base

    def get_daily_pnl_vs_prev_close(self) -> float:
        """엔진 prev_close 기준 PnL (리포트 비교용)."""
        if self.prev_close_equity == 0:
            return 0.0
        return (self.get_current_equity() - self.prev_close_equity) / self.prev_close_equity

    def get_monthly_dd_pct(self) -> float:
        today = date.today()
        if today.month != self._peak_month:
            self._peak_month = today.month
            self.peak_equity = self.get_current_equity()
        equity = self.get_current_equity()
        self.peak_equity = max(self.peak_equity, equity)
        if self.peak_equity == 0:
            return 0.0
        return (equity - self.peak_equity) / self.peak_equity

    def get_exposure_pct(self) -> float:
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        return (equity - self.cash) / equity

    # v7.9: stale equity 감지 (강화)
    def check_stale_equity(self, stale_threshold_sec: int = 600) -> bool:
        """10분 이상 equity 변동 없으면 True + 경고."""
        equity = self.get_current_equity()
        now = datetime.now()
        if abs(equity - self._last_equity_value) > 1.0:
            self._last_equity_value = equity
            self._last_equity_change_ts = now
            self._stale_warned = False
            return False
        elapsed = (now - self._last_equity_change_ts).total_seconds()
        if elapsed > stale_threshold_sec and not self._stale_warned:
            print(f"[WARN:STALE_PRICE] equity={equity:,.0f} "
                  f"변동없음 {elapsed/60:.0f}분 stale_age_sec={elapsed:.0f}")
            self._stale_warned = True
            return True
        return elapsed > stale_threshold_sec

    def get_stale_age_sec(self) -> float:
        """v7.9: equity 마지막 변동 이후 경과 시간 (초)."""
        return (datetime.now() - self._last_equity_change_ts).total_seconds()

    def _sector_exposure(self, sector: str) -> float:
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        sec_val = sum(p.market_value for p in self.positions.values() if p.sector == sector)
        return sec_val / equity

    def get_sector_exposures(self) -> Dict[str, float]:
        """섹터별 노출도 반환 {sector: pct}, 노출도 내림차순."""
        equity = self.get_current_equity()
        if equity == 0:
            return {}
        sector_vals: Dict[str, float] = {}
        for pos in self.positions.values():
            sector_vals[pos.sector] = sector_vals.get(pos.sector, 0.0) + pos.market_value
        return dict(sorted(
            {s: v / equity for s, v in sector_vals.items()}.items(),
            key=lambda x: -x[1]
        ))

    def sector_capacity_remaining(self, sector: str) -> float:
        """해당 섹터에 추가 투자 가능한 금액 (max_sector_exp 기준)."""
        equity = self.get_current_equity()
        if equity == 0:
            return 0.0
        used = self._sector_exposure(sector)
        remaining_pct = max(0.0, self.max_sector_exp - used)
        return remaining_pct * equity

    # ── Risk Mode ────────────────────────────────────────────────────────────

    def risk_mode(self) -> str:
        """HARD_STOP | DAILY_KILL | SOFT_STOP | NORMAL"""
        if self.get_monthly_dd_pct() < self.monthly_dd_limit:
            return "HARD_STOP"
        daily_pnl = self.get_daily_pnl_pct()
        if daily_pnl < self.daily_kill_limit:
            return "DAILY_KILL"
        if daily_pnl < self.daily_loss_limit:
            return "SOFT_STOP"
        return "NORMAL"

    def get_liquidation_targets(self) -> List[str]:
        """HARD_STOP 시 청산 대상 (손실 큰 순)."""
        return sorted(self.positions.keys(),
                      key=lambda c: self.positions[c].unrealized_pnl)

    def set_regime(self, regime_value: str) -> None:
        """레짐 변경 시 max_positions 동기화. StageManager에서 호출."""
        self._current_regime = regime_value
        self.max_positions = self.max_pos_bear if regime_value == "BEAR" else self.max_pos_bull

    # ── 6중 게이트 + 현금 체크 ─────────────────────────────────────────────────

    def can_enter(self, code: str, amount: float, sector: str) -> Tuple[bool, str]:
        equity = self.get_current_equity()

        # v7.6 Gate -1: restricted 포지션이 있는 종목은 추가매수 금지
        if code in self.positions:
            pos = self.positions[code]
            if getattr(pos, 'is_restricted', False):
                return False, (f"추가매수 금지 — qty_confidence={pos.qty_confidence}, "
                               f"reason={pos.restricted_reason}")

        # Gate 0: 현금 부족 체크 (미수금 방지)
        if amount > self.cash:
            return False, f"현금 부족 (필요={amount:,.0f}, 가용={self.cash:,.0f}) — 미수 방지"

        if self.get_daily_pnl_pct() < self.daily_loss_limit:
            return False, f"일일 손실 한도 초과 ({self.get_daily_pnl_pct():.2%})"

        if self.get_monthly_dd_pct() < self.monthly_dd_limit:
            return False, f"월간 DD 한도 초과 ({self.get_monthly_dd_pct():.2%})"

        if code not in self.positions and len(self.positions) >= self.max_positions:
            return False, f"최대 보유 종목 수 초과 ({self.max_positions}개)"

        cur_val = self.positions[code].market_value if code in self.positions else 0.0
        if equity > 0 and (cur_val + amount) / equity > self.max_per_stock:
            return False, f"종목당 최대 비중 초과 ({self.max_per_stock:.0%})"

        if self._sector_exposure(sector) + (amount / equity if equity > 0 else 0) > self.max_sector_exp:
            return False, f"섹터 노출 한도 초과 ({self.max_sector_exp:.0%})"

        if self.get_exposure_pct() + (amount / equity if equity > 0 else 0) > self.max_exposure:
            return False, f"총 노출도 한도 초과 ({self.max_exposure:.0%})"

        return True, "OK"

    # ── 포지션 업데이트 ───────────────────────────────────────────────────────

    def update_position(self, code: str, sector: str,
                        quantity: int, price: float, side: str) -> None:
        amount = price * quantity

        if side == "BUY":
            # v7.4 FIX: 매수 수수료를 현금에서 함께 차감
            fee = amount * self.config.FEE
            self._cumul_fee_buy += fee

            if code in self.positions:
                pos = self.positions[code]
                total_qty  = pos.quantity + quantity
                total_cost = pos.avg_price * pos.quantity + price * quantity
                pos.avg_price     = total_cost / total_qty
                pos.quantity      = total_qty
                pos.current_price = price
                # v7.8: 보조 수량 갱신
                pos.filled_buy_qty += quantity
            else:
                self.positions[code] = Position(
                    code=code, sector=sector,
                    quantity=quantity, avg_price=price, current_price=price,
                    prev_close=price,  # v7.4: 당일 진입 → prev_close=진입가 (GAP_DOWN 오판 방지)
                    high_watermark=price,  # v7.5: trailing stop 기준
                    qty_sellable=-1,   # BUG-3 FIX: unqueried sentinel → effective_sellable=quantity 허용
                    # v7.8: 보조 수량 초기화
                    requested_qty=quantity,
                    filled_buy_qty=quantity,
                )
                print(f"[POS] ENTERING → OPEN {code} qty={quantity} price={price:,.0f}")
            self.cash -= (amount + fee)

        elif side == "SELL":
            if code in self.positions:
                pos = self.positions[code]
                # v7.9: qty guard — final_sell_qty 기준 clamp
                actual_sold = min(quantity, pos.quantity)
                if actual_sold != quantity:
                    # v7.9: 통합 qty guard 로그 (모든 수량 필드 한 줄 출력)
                    print(f"[POS] WARN qty guard: {code} "
                          f"broker_hold={pos.broker_confirmed_qty} "
                          f"broker_sellable={pos.qty_sellable} "
                          f"engine_hold={pos.quantity} engine_net={pos.net_qty} "
                          f"final_sell_qty={quantity} chejan_fill_qty={quantity} "
                          f"close_applied_qty={actual_sold}")
                sell_amount = price * actual_sold
                fee = sell_amount * self.config.FEE
                tax = sell_amount * self.config.TAX
                self._cumul_fee_sell += fee
                self._cumul_tax += tax

                pos.quantity -= actual_sold
                # v7.9: 보조 수량 갱신
                pos.filled_sell_qty += actual_sold
                self.cash    += (sell_amount - fee - tax)
                if pos.quantity <= 0:
                    print(f"[POS] OPEN → CLOSED {code} qty={actual_sold}")
                    del self.positions[code]
                else:
                    print(f"[POS] PARTIAL_CLOSED {code} sold={actual_sold} "
                          f"remaining={pos.quantity}")

    def update_prices(self, price_map: Dict[str, float]) -> None:
        for code, price in price_map.items():
            if code in self.positions:
                self.positions[code].current_price = price
                # v7.5: trailing stop high watermark 갱신 (상승만)
                if price > self.positions[code].high_watermark:
                    self.positions[code].high_watermark = price

    def register_plan(self, code: str, tp: float, sl: float,
                      q_score: float = 0.0, rr_ratio: float = 0.0) -> None:
        pos = self.positions.get(code)
        if pos and pos.tp == 0.0 and pos.sl == 0.0:
            pos.tp       = tp
            pos.sl       = sl
            pos.q_score  = q_score
            pos.rr_ratio = rr_ratio

    def force_update_sl(self, code: str, sl: float) -> None:
        """v7.5: SL을 올리기만 함 (절대 내리지 않음). Trailing/Decay용."""
        pos = self.positions.get(code)
        if pos and sl > pos.sl:
            pos.sl = sl

    def has_position(self, code: str) -> bool:
        pos = self.positions.get(code)
        return bool(pos and pos.quantity > 0)

    def ensure_position_risk_fields(self, code: str) -> bool:
        """v7.8: OPEN 전환 후 SL/TP/avg_price 유효성 검증.
        미설정 시 RISK_UNINITIALIZED로 격리. 반환: True=유효, False=격리됨."""
        pos = self.positions.get(code)
        if not pos:
            return False
        missing = []
        if pos.sl <= 0:
            missing.append("SL")
        if pos.tp <= 0:
            missing.append("TP")
        if pos.avg_price <= 0:
            missing.append("avg_price")
        if missing:
            pos.mark_restricted("RISK_UNINITIALIZED", confidence="LOW")
            print(f"[RISK_UNINITIALIZED] {code} 필수 필드 미설정: {missing} → 거래 차단")
            return False
        return True

    # ── Broker Truth Source 강제 ───────────────────────────────────────────────

    def reconcile_with_broker(self, broker_positions: Dict[str, dict]) -> Dict[str, list]:
        """
        Broker holdings = truth. Engine portfolio를 broker 기준으로 overwrite.

        v7.9: 2단계 검증 구조
          1단계 (POSITION): broker TR 보유내역 기준 현재 포지션 스냅샷 정합성
          2단계 (EXECUTION): trades.csv ↔ 체결/주문 내역 대조 (별도 단계, 여기서 미수행)

        broker_positions: {code: {"qty": int, "avg_price": float, "cur_price": float,
                                  "name": str, "sector": str}}
        반환: {"added": [...], "removed": [...], "qty_fixed": [...], "position_matches": [...]}
        """
        result = {"added": [], "removed": [], "qty_fixed": [], "position_matches": []}
        engine_codes = set(self.positions.keys())
        broker_codes = set(broker_positions.keys())

        # Broker에만 있는 종목 → 내부에 추가 (전체 수량 필드 일관 설정)
        for code in broker_codes - engine_codes:
            bp = broker_positions[code]
            _bq = bp["qty"]
            self.positions[code] = Position(
                code=code,
                sector=bp.get("sector", "기타"),
                quantity=_bq,
                avg_price=float(bp["avg_price"]),
                current_price=float(bp["cur_price"]),
                high_watermark=max(float(bp["avg_price"]), float(bp["cur_price"])),
                qty_sellable=_bq,
                broker_confirmed_qty=_bq,
                requested_qty=_bq,
                filled_buy_qty=_bq,
                filled_sell_qty=0,
            )
            result["added"].append(code)
            print(f"[RECON] broker position adopted {code} "
                  f"qty={_bq} avg={bp['avg_price']:,.0f}")

        # Engine에만 있는 종목 → 제거 (phantom)
        # NOTE: cash는 건드리지 않음.
        #   _sync_with_kiwoom()가 먼저 broker cash로 overwrite하므로
        #   여기서 market_value를 더하면 이중 반영(cash 과대계상).
        #   broker가 truth → broker cash가 이미 정확한 값.
        for code in engine_codes - broker_codes:
            pos = self.positions[code]
            print(f"[RECON] phantom removed {code} "
                  f"engine_qty={pos.quantity} mv={pos.market_value:,.0f} (broker에 없음)")
            del self.positions[code]
            result["removed"].append(code)

        # 수량 불일치 → broker 기준으로 덮어쓰기 + 전체 수량 필드 재정렬
        _diff_entries = []
        for code in broker_codes & engine_codes:
            bp = broker_positions[code]
            pos = self.positions[code]
            broker_qty = bp["qty"]
            engine_qty = pos.quantity

            if engine_qty != broker_qty:
                # v7.7: LARGE_RECON 경고
                diff_pct = abs(engine_qty - broker_qty) / max(engine_qty, 1)
                if diff_pct > 0.10:
                    print(f"[WARN:LARGE_RECON] {code} engine={engine_qty} "
                          f"broker={broker_qty} diff={diff_pct:.0%}")
                # v7.9: per-position before/after 로그
                print(f"[RECON] qty mismatch {code} "
                      f"engine={engine_qty} broker={broker_qty} "
                      f"(before: hold={pos.quantity} net={pos.net_qty} "
                      f"filled_buy={pos.filled_buy_qty} filled_sell={pos.filled_sell_qty})")
                _diff_entries.append({
                    "code": code, "engine_qty": engine_qty,
                    "broker_qty": broker_qty, "diff_pct": round(diff_pct, 3) if engine_qty > 0 else 1.0,
                    "before": {"hold": engine_qty, "net": pos.net_qty,
                               "filled_buy": pos.filled_buy_qty, "filled_sell": pos.filled_sell_qty},
                })
                pos.avg_price = float(bp["avg_price"])
                result["qty_fixed"].append(code)

            # v7.9: 포지션 수량 필드 재정렬
            # --- 현재 상태 필드: broker 기준 즉시 덮어쓰기 ---
            pos.quantity = broker_qty
            pos.current_price = float(bp["cur_price"])
            pos.qty_sellable = broker_qty
            pos.broker_confirmed_qty = broker_qty
            pos.qty_pending_sell = 0  # reconcile 시 pending 초기화

            # --- 이력 필드: 직접 덮어쓰지 않고 불일치 시 repair flag ---
            _expected_net = pos.filled_buy_qty - pos.filled_sell_qty
            if _expected_net != broker_qty:
                # 이력 기준 net과 broker qty 불일치 — 이력 보정
                # filled_sell_qty는 실제 매도 이벤트 누적이므로 보존
                # filled_buy_qty만 역산으로 보정 (의미: "broker가 확인한 잔고 = buy - sell")
                _old_buy = pos.filled_buy_qty
                pos.filled_buy_qty = broker_qty + pos.filled_sell_qty
                pos.requested_qty = max(pos.requested_qty, broker_qty)
                print(f"[RECON:HISTORY_REPAIR] {code} filled_buy "
                      f"{_old_buy}→{pos.filled_buy_qty} "
                      f"(broker={broker_qty}, sold={pos.filled_sell_qty}, "
                      f"old_net={_expected_net}→new_net={pos.net_qty})")

            # --- RECON_BLOCK 판정은 validation 단계에서 수행 (여기서 해제 안 함) ---

            # v7.9: per-position 정밀 비교 (position reconcile 결과)
            _avg_tol = 0.02  # 평균단가 허용 오차 2%
            _avg_match = True
            if pos.avg_price > 0 and float(bp["avg_price"]) > 0:
                _avg_diff = abs(pos.avg_price - float(bp["avg_price"])) / pos.avg_price
                _avg_match = _avg_diff <= _avg_tol
            result["position_matches"].append({
                "code": code,
                "broker_hold_qty": broker_qty,
                "broker_avg_price": float(bp["avg_price"]),
                "engine_quantity": pos.quantity,
                "engine_qty_sellable": pos.qty_sellable,
                "engine_avg_price": pos.avg_price,
                "qty_match": pos.quantity == broker_qty,
                "avg_price_match": _avg_match,
                "status": "MATCH" if (pos.quantity == broker_qty and _avg_match) else "POSITION_MISMATCH",
            })

        # v7.9: reconciliation 후 전체 포지션 유효성 검증
        _recon_blocked = []
        for code, pos in list(self.positions.items()):
            _violations = []
            if pos.net_qty < 0:
                _violations.append(f"net_qty={pos.net_qty}<0")
            if pos.quantity < pos.net_qty:
                _violations.append(f"hold={pos.quantity}<net={pos.net_qty}")
            if pos.qty_sellable > pos.quantity and pos.qty_sellable >= 0:
                _violations.append(f"sellable={pos.qty_sellable}>hold={pos.quantity}")
            if pos.quantity > 0 and pos.quantity != pos.net_qty and not pos.restricted_reason:
                _violations.append(f"hold({pos.quantity})!=net({pos.net_qty}) no_pending_reason")
            if _violations:
                pos.mark_restricted("RECON_BLOCK", confidence="LOW")
                _recon_blocked.append(code)
                print(f"[RECON:BLOCK] {code} 정합성 위반: {', '.join(_violations)} → RECON_BLOCK")

        n_fixed = len(result["qty_fixed"])
        _n_match = sum(1 for m in result["position_matches"] if m["status"] == "MATCH")
        _n_mismatch = sum(1 for m in result["position_matches"] if m["status"] != "MATCH")
        print(f"[RECON] 결과: added={len(result['added'])}, "
              f"removed={len(result['removed'])}, qty_fixed={n_fixed}"
              f"{f', recon_blocked={len(_recon_blocked)}' if _recon_blocked else ''}")
        print(f"[RECON:POSITION] 현재 포지션 검증: {_n_match}건 일치, {_n_mismatch}건 불일치 "
              f"(증권사 TR 기준)")
        if _n_mismatch == 0 and not _recon_blocked:
            print(f"[RECON:POSITION] 현재 포지션은 증권사 TR 기준 일치")
        print(f"[RECON:EXECUTION] 체결 원장 일치는 별도 주문/체결 내역 조회 필요 "
              f"(trades.csv 검증 미완료)")

        # v7.9: diff log 첨부 — per-position 비교 결과 포함
        if _diff_entries or result["added"] or result["removed"] or result["position_matches"]:
            # 현재 포지션 스냅샷 (after)
            _pos_after = {}
            for code in self.positions:
                p = self.positions[code]
                _pos_after[code] = {
                    "hold": p.quantity, "net": p.net_qty,
                    "sellable": p.qty_sellable, "broker_confirmed": p.broker_confirmed_qty,
                    "filled_buy": p.filled_buy_qty, "filled_sell": p.filled_sell_qty,
                }
            result["_diff_log"] = {
                "timestamp": datetime.now().isoformat(),
                "verification_level": "POSITION_ONLY",
                "verification_note": "현재 포지션은 증권사 TR 기준 검증 완료. "
                                     "체결 원장(trades.csv) 일치는 별도 주문/체결 내역 조회 필요.",
                "added": result["added"],
                "removed": result["removed"],
                "qty_diffs": _diff_entries,
                "position_matches": result["position_matches"],
                "cash_after": self.cash,
                "positions_after": _pos_after,
            }

        return result

    # ── 장 종료 ───────────────────────────────────────────────────────────────

    def end_of_day_update(self) -> None:
        self.prev_close_equity = self.get_current_equity()
        # v7.4: 전일 종가 스냅샷 (GAP_DOWN 판정 기준)
        for pos in self.positions.values():
            pos.prev_close = pos.current_price

    # ── 요약 ─────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        equity = self.get_current_equity()
        daily_pnl_pct = self.get_daily_pnl_pct()
        # v7.8: PnL 기준 선택 (synced 우선)
        _base = self.synced_broker_equity if self.synced_broker_equity > 0 else self.prev_close_equity
        daily_pnl_won = equity - _base
        sec_exp = self.get_sector_exposures()
        # 한도(30%) 초과 위험 섹터 강조
        sec_info = {
            s: f"{v:.1%}{'(!!)' if v >= self.max_sector_exp * 0.8 else ''}"
            for s, v in list(sec_exp.items())[:5]
        }
        total_cost = self._cumul_fee_buy + self._cumul_fee_sell + self._cumul_tax
        result = {
            "총평가금액": f"{equity:,.0f}원",
            "현금":       f"{self.cash:,.0f}원",
            "보유종목수": len(self.positions),
            "총노출도":   f"{self.get_exposure_pct():.1%}",
            "일간손익":   f"{daily_pnl_pct:+.2%} ({daily_pnl_won:+,.0f}원)",
            "월간DD":     f"{self.get_monthly_dd_pct():.2%}",
            "리스크모드": getattr(self, '_display_risk_mode', None) or self.risk_mode(),
            "섹터노출도": sec_info,
            "누적부대비용": f"{total_cost:,.0f}원 (수수료 {self._cumul_fee_buy + self._cumul_fee_sell:,.0f} + 세금 {self._cumul_tax:,.0f})",
        }
        # v7.8: 이중 PnL 기준 출력 (sync 당일에만 둘 다 표시)
        if self.synced_broker_equity > 0 and self.prev_close_equity != self.synced_broker_equity:
            pnl_vs_prev = self.get_daily_pnl_vs_prev_close()
            result["pnl_vs_prev_close"] = f"{pnl_vs_prev:+.2%}"
            result["pnl_vs_synced"] = f"{daily_pnl_pct:+.2%}"
            result["pnl_기준"] = f"synced={self.synced_broker_equity:,.0f}"
        return result
