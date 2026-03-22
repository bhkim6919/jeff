"""
ExitLogic (v7.5)
================
청산 우선순위:
  0. GAP_DOWN   — 전일종가 대비 현재가 ≤ -5% 강제 청산 (테일 컷)
                  ※ 장중 매 사이클(60초) 판정. 시가뿐 아니라 장중 급락도 포착.
  1. SL         — 손실 제한 (ATR 기반 + trailing/decay 동적 조정)
     1a. MAX_LOSS_CAP — 진입가 대비 -8% 절대 한도 (ATR/trailing 무관 최후 방어)
     1b. ATR SL      — trailing stop / time-decay로 상향만 가능
  2. RAL CRASH  — CRASH 모드 + rs_composite < 0.45 강제청산
  3. RS 청산    — 월초 rs_composite < 0.40 (RS_EXIT_THRESH)
  4. MAX_HOLD   — 최대 보유일 초과 (60일)

변경 (v7.5b):
  - GAP_DOWN 정의 명확화: "시가 갭다운" → "전일종가 대비 현재가 급락" (장중 계속 판정)
  - SL 상향 사유 분리 기록: sl_adjust_reason (TRAILING / TIME_DECAY / REPAIR)
  - Orphan Repair 실패 시 MAX_LOSS_CAP 폴백 + stage=SYNC 태그

변경 (v7.5):
  - Trailing Stop: 수익 +5% 이상 시 SL 자동 상향
  - Time-Decay: 보유 20일 이후 SL 점진 강화
  - Orphan Repair: SL=0 포지션 자동 ATR SL 설정 + prev_close 백필

변경 (v7.2):
  - GAP_DOWN 테일 컷 룰 추가

변경 (v7):
  - MA20 청산 제거 (RS 청산으로 대체)
  - TP 청산 제거 (추세추종 → 충분히 버티는 설계)
  - RAL CRASH 강제청산 추가
  - 월초 RS 청산 추가
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.portfolio_manager import PortfolioManager
from core.position_tracker import Position
from core.exceptions import PriceFetchError
from config import Gen3Config


class ExitLogic:

    def __init__(self, provider, executor, portfolio: PortfolioManager, config: Gen3Config,
                 trade_logger=None, ral_mode: str = "NORMAL"):
        self.provider     = provider
        self.executor     = executor
        self.portfolio    = portfolio
        self.config       = config
        self.trade_logger = trade_logger
        self.ral_mode     = ral_mode   # "CRASH" | "SURGE" | "NORMAL"

        # v7.5: ATR 캐시 (LIVE TR 콜 절약, 30분 유효)
        self._atr_cache: Dict[str, Tuple[float, datetime]] = {}

    # ── ATR 계산 (캐시 포함) ──────────────────────────────────────────────────

    def _calc_atr(self, code: str) -> float:
        """Wilder ATR(20) 계산. 30분 캐시."""
        now = datetime.now()
        if code in self._atr_cache:
            val, ts = self._atr_cache[code]
            if (now - ts).total_seconds() < 1800:
                return val
        atr = self._compute_atr(code)
        self._atr_cache[code] = (atr, now)
        return atr

    def _compute_atr(self, code: str) -> float:
        """provider.get_stock_ohlcv로 Wilder ATR(20) 계산."""
        try:
            df = self.provider.get_stock_ohlcv(code, days=30)
            if df is None or df.empty or len(df) < 21:
                return 0.0
            high  = df["high"].astype(float).values
            low   = df["low"].astype(float).values
            close = df["close"].astype(float).values
            tr = np.maximum(
                high[1:] - low[1:],
                np.maximum(np.abs(high[1:] - close[:-1]),
                           np.abs(low[1:]  - close[:-1]))
            )
            period = 20
            if len(tr) < period:
                return float(tr.mean())
            atr = float(tr[:period].mean())
            k = 1.0 / period
            for v in tr[period:]:
                atr = atr * (1 - k) + v * k
            return atr
        except Exception:
            return 0.0

    # ── Orphan Repair (시작 시 1회) ──────────────────────────────────────────

    def repair_orphan_positions(self, regime: str = "BULL") -> int:
        """
        v7.5: SL=0 포지션에 ATR 기반 SL 자동 설정.
        prev_close, high_watermark 미설정 시 백필.
        stage가 비어있으면 "SYNC" 태그 부여 (전략 진입 vs 계좌동기화 구분).
        반환: 수정된 포지션 수.
        """
        repaired = 0
        repair_ok = 0
        repair_fail = 0

        for code, pos in self.portfolio.positions.items():
            changed = False

            # stage 미설정 → SYNC 태그 (Kiwoom 동기화로 들어온 포지션)
            if not getattr(pos, 'stage', ''):
                pos.stage = "SYNC"
                changed = True

            # prev_close 백필
            if pos.prev_close <= 0:
                try:
                    df = self.provider.get_stock_ohlcv(code, days=5)
                    if df is not None and len(df) >= 2:
                        pos.prev_close = float(df["close"].iloc[-2])
                        changed = True
                        print(f"  [SL:REPAIR] {code} prev_close 백필: {pos.prev_close:,.0f}")
                    else:
                        print(f"  [SL:REPAIR] {code} prev_close 백필 실패 (OHLCV 부족)")
                except Exception as e:
                    print(f"  [SL:REPAIR] {code} prev_close 백필 실패: {e}")

            # high_watermark 초기화
            hwm = getattr(pos, 'high_watermark', 0)
            if hwm <= 0:
                pos.high_watermark = max(pos.avg_price, pos.current_price)
                changed = True

            # SL=0 → ATR 기반 SL 자동 설정
            if pos.sl <= 0:
                atr = self._calc_atr(code)
                if atr > 0:
                    mult = self.config.ATR_MULT_BEAR if regime == "BEAR" else self.config.ATR_MULT_BULL
                    new_sl = pos.avg_price - mult * atr
                    cap_sl = pos.avg_price * (1 + self.config.MAX_LOSS_CAP)
                    pos.sl = int(max(new_sl, cap_sl))
                    changed = True
                    repair_ok += 1
                    print(f"  [SL:REPAIR] {code} SL=0 -> {pos.sl:,.0f} "
                          f"(avg={pos.avg_price:,.0f}, ATR={atr:,.0f}, mult={mult}) [OK]")
                else:
                    # ATR 계산 실패 → MAX_LOSS_CAP 보수적 폴백
                    pos.sl = int(pos.avg_price * (1 + self.config.MAX_LOSS_CAP))
                    changed = True
                    repair_fail += 1
                    print(f"  [SL:REPAIR] {code} SL=0 -> {pos.sl:,.0f} "
                          f"(ATR 실패 → MAX_LOSS_CAP={self.config.MAX_LOSS_CAP:.0%} 폴백) [FALLBACK]")

            # SL 역전 체크: SL >= avg_price → ATR 기반 재계산
            # 단, 수익 포지션(current_price > avg_price)이면 trailing이 정상 작동 중이므로 skip
            if pos.sl >= pos.avg_price and pos.avg_price > 0:
                cur = getattr(pos, 'current_price', 0) or 0
                if cur > pos.avg_price:
                    pass  # trailing 활성 — SL > avg 정상
                else:
                    old_sl = pos.sl
                    atr = self._calc_atr(code)
                    if atr > 0:
                        mult = self.config.ATR_MULT_BEAR if regime == "BEAR" else self.config.ATR_MULT_BULL
                        new_sl = pos.avg_price - mult * atr
                        cap_sl = pos.avg_price * (1 + self.config.MAX_LOSS_CAP)
                        pos.sl = int(max(new_sl, cap_sl))
                    else:
                        pos.sl = int(pos.avg_price * (1 + self.config.MAX_LOSS_CAP))
                    changed = True
                    print(f"  [SL:INVERT] {code} SL({old_sl:,.0f}) >= avg({pos.avg_price:,.0f}) "
                          f"cur={cur:,.0f} (손실) → SL={pos.sl:,.0f}")

            # TP=0 또는 TP <= avg_price → SL 대칭 TP 자동 설정 (RR 2:1)
            if pos.sl > 0 and (getattr(pos, 'tp', 0) <= 0 or pos.tp <= pos.avg_price):
                old_tp = getattr(pos, 'tp', 0)
                pos.tp = int(pos.avg_price + (pos.avg_price - pos.sl) * 2.0)
                changed = True
                print(f"  [TP:REPAIR] {code} TP={old_tp:,.0f} -> {pos.tp:,.0f} "
                      f"(avg={pos.avg_price:,.0f}, SL={pos.sl:,.0f}, RR=2:1)")

            if changed:
                repaired += 1

        if repaired:
            print(f"[OrphanFix] {repaired}개 포지션 수정 완료 "
                  f"(ATR성공={repair_ok}, ATR실패→폴백={repair_fail})")
        return repaired

    # ── Trailing Stop + Time-Decay ───────────────────────────────────────────

    def apply_trailing_and_decay(self, regime: str = "BULL") -> int:
        """
        v7.5: 매 모니터 사이클 호출.
        1) Time-Decay: 보유일 경과에 따라 ATR mult 축소 → SL 상향
        2) Trailing Stop: 수익률 TRAIL_ACTIVATION_PCT 이상 시 SL 상향
        SL은 절대 내리지 않음 (max 연산).
        반환: 조정된 포지션 수.
        """
        cfg = self.config
        adjusted = 0

        for code, pos in self.portfolio.positions.items():
            if pos.avg_price <= 0:
                continue
            old_sl = pos.sl
            sl_reason = None       # 어떤 메커니즘이 SL을 올렸는지 추적
            sl_detail = {}         # 디버깅용 메타데이터

            # ── Time-Decay SL Tightening ──
            if getattr(cfg, 'DECAY_ENABLED', False) and pos.held_days >= cfg.DECAY_START_DAY:
                atr = self._calc_atr(code)
                if atr > 0:
                    orig_mult = cfg.ATR_MULT_BEAR if regime == "BEAR" else cfg.ATR_MULT_BULL
                    if pos.held_days >= cfg.DECAY_END_DAY:
                        eff_mult = cfg.DECAY_ATR_MULT_MIN
                    else:
                        t = (pos.held_days - cfg.DECAY_START_DAY) / (cfg.DECAY_END_DAY - cfg.DECAY_START_DAY)
                        eff_mult = orig_mult - t * (orig_mult - cfg.DECAY_ATR_MULT_MIN)
                    decay_sl = pos.avg_price - eff_mult * atr
                    if decay_sl > pos.sl:
                        pos.sl = decay_sl
                        sl_reason = "TIME_DECAY"
                        sl_detail = {"atr": atr, "eff_mult": eff_mult, "held_days": pos.held_days}

            # ── Trailing Stop ──
            hwm = getattr(pos, 'high_watermark', 0)
            cur_price = getattr(pos, 'current_price', 0) or 0
            # 현재가 기준 실제 pnl 직접 계산 (stale pnl_pct 방지)
            actual_pnl = (cur_price - pos.avg_price) / pos.avg_price if pos.avg_price > 0 and cur_price > 0 else 0
            if (getattr(cfg, 'TRAIL_ENABLED', False)
                    and hwm > 0
                    and actual_pnl > 0  # 현재가 기준 수익 중일 때만
                    and actual_pnl >= cfg.TRAIL_ACTIVATION_PCT):
                # hwm 정합성: 현재가보다 과도하게 높은 hwm 클램프
                eff_hwm = min(hwm, cur_price * 1.15)  # hwm은 현재가 +15% 이내
                atr = self._calc_atr(code)
                if atr > 0:
                    trail_sl = eff_hwm - cfg.TRAIL_ATR_MULT * atr
                    trail_floor = pos.avg_price * (1 + cfg.TRAIL_MIN_LOCK_PCT)
                    trail_sl = max(trail_sl, trail_floor)
                    # 현재가 이상으로 SL 설정 방지 (즉시 청산 트리거 방지)
                    if trail_sl >= cur_price:
                        trail_sl = int(cur_price * 0.98)  # 현재가 -2% 마진
                    if trail_sl > pos.sl:
                        pos.sl = int(trail_sl)
                        sl_reason = "TRAILING"
                        sl_detail = {"atr": atr, "hwm": hwm, "eff_hwm": eff_hwm,
                                     "trail_sl": trail_sl, "trail_floor": trail_floor}

            if pos.sl > old_sl:
                adjusted += 1
                from data.name_lookup import get_name
                label = f"{get_name(code)}({code})"
                atr_val = sl_detail.get("atr", 0)
                if sl_reason == "TRAILING":
                    _eff_hwm = sl_detail.get("eff_hwm", hwm)
                    _actual = (cur_price - pos.avg_price) / pos.avg_price if pos.avg_price > 0 else 0
                    print(f"  [SL:{sl_reason}] {label} SL {old_sl:,.0f} -> {pos.sl:,.0f} "
                          f"(hwm={hwm:,.0f}→eff={_eff_hwm:,.0f}, ATR={atr_val:,.0f}, "
                          f"cur={cur_price:,.0f}, pnl={_actual:+.1%})")
                elif sl_reason == "TIME_DECAY":
                    eff_m = sl_detail.get("eff_mult", 0)
                    print(f"  [SL:{sl_reason}] {label} SL {old_sl:,.0f} -> {pos.sl:,.0f} "
                          f"(d{pos.held_days}, ATR_mult={eff_m:.2f}, ATR={atr_val:,.0f})")

        return adjusted

    # ── 메인 청산 로직 ─────────────────────────────────────────────────────────

    def check_and_exit(self, signals_today: list = None, regime: str = "BULL",
                       skip_codes: set = None,
                       price_snapshot: dict = None) -> list:
        """
        보유 전 포지션 순회 → 청산 조건 체크 → 해당 시 청산.
        signals_today: EntrySignal.load_today() 결과 (rs_composite 조회용)
        regime: "BULL" | "BEAR" (v7.5: trailing/decay에 전달)
        skip_codes: 매도 실패로 당일 재시도 차단된 종목 코드 set
        price_snapshot: 외부에서 미리 조회한 가격 맵 (제공 시 재조회 생략)
        """
        if not self.portfolio.positions:
            return []

        if price_snapshot:
            price_map = price_snapshot
        else:
            price_map = self._fetch_latest_prices()
        self.portfolio.update_prices(price_map)

        # v7.5: Trailing Stop + Time-Decay (SL 상향 후 exit 조건 체크)
        self.apply_trailing_and_decay(regime=regime)

        # 당일 RS 맵 (code → rs_composite)
        rs_map = {}
        if signals_today:
            for s in signals_today:
                rs_map[s["code"]] = float(s.get("rs_composite", s.get("qscore", 1.0)))

        is_month_start = self._is_month_start()

        _skip = skip_codes or set()
        exits = []  # (code, pos, current_price, close_type)
        for code, pos in list(self.portfolio.positions.items()):
            if code in _skip:
                continue
            current    = price_map.get(code, pos.current_price)
            close_type = self._eval_exit(code, pos, current, rs_map, is_month_start)
            if close_type:
                exits.append((code, pos, current, close_type))

        if not exits:
            return []

        # 우선순위: GAP_DOWN → SL → RAL_CRASH → RS_EXIT → MAX_HOLD
        priority = {"GAP_DOWN": 0, "SL": 1, "RAL_CRASH": 2, "RS_EXIT": 3, "MAX_HOLD": 4}
        exits.sort(key=lambda x: priority.get(x[3], 9))

        print(f"[ExitLogic] 청산 대상 {len(exits)}개")
        results = []
        for code, pos, current, close_type in exits:
            result = self._execute_exit(code, pos, current, close_type)
            results.append(result)

        return results

    # ── 내부 ─────────────────────────────────────────────────────────────────

    def _eval_exit(
        self,
        code: str,
        pos: Position,
        current: float,
        rs_map: dict,
        is_month_start: bool,
    ) -> Optional[str]:
        """청산 조건 판별. 우선순위 순으로 확인."""

        # 0. GAP_DOWN 테일 컷 — 전일종가 대비 현재가 ≤ -5% (장중 매 사이클 판정)
        #    ※ "시가 갭다운"이 아닌 "전일종가 대비 급락" 기준.
        #       시가뿐 아니라 장중 급락(=intraday breakdown)도 포착한다.
        prev_close = pos.prev_close
        if prev_close > 0 and current > 0:
            gap_pct = (current - prev_close) / prev_close
            if gap_pct <= self.config.GAP_DOWN_EXIT:
                print(f"  [ExitLogic] {code} GAP_DOWN {gap_pct:+.2%} "
                      f"(전일종가={prev_close:,.0f} → 현재={current:,.0f})")
                return "GAP_DOWN"

        # 1a. MAX_LOSS_CAP — 진입가 대비 -8% 강제 청산 (ATR SL 무관)
        max_loss_cap = getattr(self.config, 'MAX_LOSS_CAP', -0.08)
        if pos.avg_price > 0 and current > 0:
            loss_pct = (current - pos.avg_price) / pos.avg_price
            if loss_pct <= max_loss_cap:
                print(f"  [ExitLogic] {code} MAX_LOSS_CAP {loss_pct:+.2%} "
                      f"(진입={pos.avg_price:,.0f} → 현재={current:,.0f}, cap={max_loss_cap:.0%})")
                return "SL"

        # 1b. SL (ATR 기반 + trailing/decay 반영)
        if pos.sl > 0 and current <= pos.sl:
            return "SL"

        # 2. RAL CRASH 강제청산 (rs_composite < 0.45)
        if self.ral_mode == "CRASH":
            rs = rs_map.get(code, 1.0)  # signals에 없으면 청산 안 함
            if rs < self.config.RAL_CRASH_CLOSE_RS:
                return "RAL_CRASH"

        # 3. 월초 RS 청산 (rs_composite < 0.40)
        if is_month_start:
            rs = rs_map.get(code, 1.0)
            if rs < self.config.RS_EXIT_THRESH:
                return "RS_EXIT"

        # 4. 최대 보유일 초과
        if pos.held_days >= self.config.MAX_HOLD_DAYS:
            return "MAX_HOLD"

        return None

    def _is_month_start(self) -> bool:
        """오늘이 월초(1~5 거래일)인지 확인."""
        today = date.today()
        # 간단 구현: 1일부터 7일 이내 (영업일 기준 1~5일째 커버)
        return today.day <= 7

    def _resolve_sellable_qty(self, code: str, pos: Position) -> int:
        """
        v7.9: 매도 수량 기준값 단일화 (final_sell_qty).
        우선순위: broker_sellable → broker_hold → engine net_qty.
        반환값이 곧 final_sell_qty — 이후 모든 매도 로직은 이 값만 사용.
        """
        broker_sell = -1
        broker_hold = -1

        # 브로커 조회 시도 (LIVE 모드만)
        _sellable_source = "engine_fallback"
        if hasattr(self.provider, 'query_sellable_qty'):
            try:
                info = self.provider.query_sellable_qty(code)
                if not info.get("error"):
                    broker_hold = info["hold_qty"]
                    broker_sell = info["sellable_qty"]
                    _sellable_source = info.get("sellable_source", "opw00018")
                    pos.broker_confirmed_qty = broker_hold
                    if broker_sell >= 0:
                        pos.qty_sellable = broker_sell
                else:
                    print(f"  [SellCheck] {code} broker 조회 실패: {info['error']} — 폴백")
            except Exception as e:
                print(f"  [SellCheck] {code} broker 조회 예외: {e} — 폴백")

        # v7.9: 우선순위 — real_sellable → broker_hold → engine
        # sellable=-1(UNKNOWN_SELLABLE)이면 broker_hold로 보수적 폴백
        if broker_sell >= 0:
            final_sell_qty = broker_sell
            source = "broker_sellable"
        elif broker_hold >= 0:
            # sellable 미확인 — hold를 사용하되 명시적 폴백 표시
            final_sell_qty = broker_hold
            source = f"broker_hold(sellable={_sellable_source})"
        else:
            final_sell_qty = getattr(pos, 'effective_sellable', pos.quantity)
            source = "engine_fallback"

        # 통합 로그
        print(f"  [SellCheck] {code} broker_hold={broker_hold} broker_sellable={broker_sell} "
              f"engine_hold={pos.quantity} engine_net={pos.net_qty} "
              f"final_sell_qty={final_sell_qty} source={source}")

        # v7.9: sellable 미확인 경고
        if _sellable_source == "UNKNOWN_SELLABLE":
            print(f"  [SellCheck] WARN {code} sellable 미확인 — "
                  f"broker_hold={broker_hold}을 fallback 사용 (T+2/제약 미반영)")

        if broker_sell >= 0 and broker_sell < pos.quantity:
            print(f"  [SellCheck] WARN {code} sellable({broker_sell}) "
                  f"< hold({pos.quantity}) — SELL_RESTRICTED")
            pos.mark_restricted("POSITION_MISMATCH", confidence="LOW")

        return final_sell_qty

    def _fetch_latest_prices(self) -> dict:
        price_map = {}
        n_total = len(self.portfolio.positions)
        for code in self.portfolio.positions:
            try:
                p = self.provider.get_current_price(code)
                if p and p > 0:
                    price_map[code] = float(p)
            except (PriceFetchError, ValueError, KeyError, AttributeError, OSError) as e:
                print(f"  [ExitLogic] {code} 가격 조회 실패: {e}")
        n_fail = n_total - len(price_map)
        if n_fail > 0:
            print(f"  [ExitLogic] 가격 갱신: {len(price_map)}/{n_total} "
                  f"({n_fail}개 stale — 이전 가격 사용)")
        return price_map

    def _execute_exit(self, code: str, pos: Position, current: float, close_type: str):
        from runtime.order_executor import Order, TradeResult

        # v7.9: pending sell 존재 시 중복 SELL 차단
        if getattr(pos, 'has_pending_sell', False):
            print(f"  [ExitLogic] {code} SELL 차단: pending_sell 진행 중 "
                  f"(order={pos.pending_sell_order_no}, "
                  f"remaining={pos.pending_sell_remaining})")
            return TradeResult(
                code=code, side="SELL", quantity=0, exec_price=0,
                slippage_pct=0, timestamp=datetime.now(),
                rejected=True, reject_reason=f"PENDING_SELL: remaining={pos.pending_sell_remaining}",
            )

        # v7.6: qty_confidence 체크 — restricted 포지션은 자동 청산 금지
        if getattr(pos, 'is_restricted', False):
            reason = (f"SELL_RESTRICTED — qty_confidence={pos.qty_confidence}, "
                      f"reason={pos.restricted_reason}, needs_reconcile={pos.needs_reconcile}")
            print(f"  [ExitLogic] {code} {close_type} 자동청산 차단 (RESTRICTED): {reason}")
            return TradeResult(
                code=code, side="SELL", quantity=0, exec_price=0,
                slippage_pct=0, timestamp=datetime.now(),
                rejected=True, reject_reason=f"SELL_RESTRICTED: {reason}",
            )

        # v7.6: sellable_qty 사전검증 — broker에서 매도가능수량 조회
        sell_qty = self._resolve_sellable_qty(code, pos)
        if sell_qty <= 0:
            reason = (f"EXIT_BLOCKED_BROKER_RULE — "
                      f"hold_qty={pos.quantity}, qty_sellable={pos.qty_sellable}, "
                      f"resolved_sell_qty={sell_qty}")
            print(f"  [ExitLogic] {code} {close_type} 매도 차단: {reason}")
            # 매도 불가 → SELL_RESTRICTED로 마킹
            pos.mark_restricted("SELLABLE_ZERO", confidence="LOW")
            return TradeResult(
                code=code, side="SELL", quantity=0, exec_price=0,
                slippage_pct=0, timestamp=datetime.now(),
                rejected=True, reject_reason=reason,
            )

        order = Order(
            code=code, sector=pos.sector, side="SELL",
            quantity=sell_qty, price=current,
        )
        result = self.executor.execute(order)

        # v7.9: chejan 체결수량과 final_sell_qty 비교 검증
        chejan_fill_qty = result.quantity if not result.rejected else 0
        if not result.rejected:
            if chejan_fill_qty < sell_qty:
                # partial fill — CLOSED 금지, PARTIAL 유지
                print(f"  [SELL:PARTIAL] {code} chejan_fill={chejan_fill_qty} < "
                      f"final_sell_qty={sell_qty} — PARTIAL_FILL 처리")
            elif chejan_fill_qty > sell_qty:
                # chejan이 더 많이 보고 — 오류 기록, 자동 보정 금지
                print(f"  [SELL:OVERFILL_WARN] {code} chejan_fill={chejan_fill_qty} > "
                      f"final_sell_qty={sell_qty} — 브로커 체결 초과, 수동 확인 필요")

        tag = {
            "GAP_DOWN":  "[G]",
            "SL":        "[-]",
            "RAL_CRASH": "[R]",
            "RS_EXIT":   "[~]",
            "MAX_HOLD":  "[T]",
        }.get(close_type, "[?]")
        # v7.9: [SELL] 요약 로그에 공식 청산 수량(final_sell_qty)만 출력
        if not result.rejected:
            from data.name_lookup import get_name
            label = f"{get_name(code)}({code})"
            _slip = result.slippage_pct
            _fp = getattr(result, 'fill_price', 0) or result.exec_price
            print(f"  {tag} [{close_type}] [SELL] {label} "
                  f"{sell_qty}주 @ {_fp:,.0f}원 (슬리피지 {_slip:.3%})")
        else:
            print(f"  {tag} [{close_type}] {result}")

        # BUG-3 FIX: 브로커 거절 시 상세 로그 (내부 버그 vs 브로커 제약 분리)
        if result.rejected:
            print(f"  [ExitLogic] SELL REJECTED {code} "
                  f"close_type={close_type} "
                  f"final_sell_qty={sell_qty} "
                  f"hold_qty={pos.quantity} "
                  f"sellable={getattr(pos, 'qty_sellable', -1)} "
                  f"reason={result.reject_reason}")

        if not result.rejected:
            # v7.7: fill_price 우선, fallback → exec_price → current
            close_price = (getattr(result, 'fill_price', 0) or
                           result.exec_price or current)
            # v7.9: 공식 청산 수량 = final_sell_qty (chejan 초과 보고 무시)
            close_qty   = min(chejan_fill_qty, sell_qty) if chejan_fill_qty > 0 else sell_qty
            pnl_gross = (close_price - pos.avg_price) * close_qty
            pnl_pct   = (close_price / pos.avg_price - 1.0) if pos.avg_price > 0 else 0.0

            # 부대비용 계산 (수수료 + 세금)
            buy_amount  = pos.avg_price * close_qty
            sell_amount = close_price * close_qty
            fee_buy   = buy_amount  * self.config.FEE        # 매수 수수료
            fee_sell  = sell_amount * self.config.FEE         # 매도 수수료
            tax       = sell_amount * self.config.TAX         # 매도 세금
            total_cost = fee_buy + fee_sell + tax
            pnl_net    = pnl_gross - total_cost

            # v7.9: PARTIAL_CLOSED 감지 + pending state 설정
            pos_after = self.portfolio.positions.get(code)
            _close_status = "CLOSED"
            if pos_after and pos_after.quantity > 0:
                _close_status = "PARTIAL_CLOSED"
                _remaining = sell_qty - close_qty
                if _remaining > 0 and hasattr(pos_after, 'set_pending_sell'):
                    pos_after.set_pending_sell(
                        order_no=getattr(result, 'order_no', ''),
                        qty=_remaining,
                    )
                    print(f"  [PARTIAL] {code} 잔량 {pos_after.quantity}주 — "
                          f"pending_sell remaining={_remaining} "
                          f"(broker 재조회 전 재SELL 차단)")
                else:
                    # close_qty == sell_qty지만 포지션 잔량 있음 (이전 매수분)
                    if hasattr(pos_after, 'clear_pending_sell'):
                        pos_after.clear_pending_sell()
                    print(f"  [PARTIAL] {code} 잔량 {pos_after.quantity}주 — 다음 사이클 재시도")
            else:
                # 완전 청산 — pending 해제
                # pos_after가 None이면 이미 del됨
                pass

            # 청산 메타 주입
            result.close_type  = close_type
            result.close_status = _close_status
            result.entry_price = pos.avg_price
            result.pnl         = pnl_net
            result.pnl_gross   = pnl_gross
            result.pnl_pct     = pnl_pct
            result.hold_days   = pos.held_days
            # v7.9: raw vs applied 분리
            result.raw_fill_qty = chejan_fill_qty
            result.applied_qty  = close_qty
            result.fee_buy     = fee_buy
            result.fee_sell    = fee_sell
            result.tax         = tax
            result.total_cost  = total_cost

            if self.trade_logger:
                try:
                    self.trade_logger.log_close(
                        code        = code,
                        close_type  = close_type,
                        entry_price = pos.avg_price,
                        close_price = close_price,
                        quantity    = close_qty,
                        pnl_gross   = pnl_gross,
                        fee_buy     = fee_buy,
                        fee_sell    = fee_sell,
                        tax         = tax,
                        total_cost  = total_cost,
                        pnl_net     = pnl_net,
                        decision_price = getattr(result, 'decision_price', current),
                        fill_price     = close_price,
                        qty_before     = getattr(result, 'qty_before', 0),
                        qty_after      = getattr(result, 'qty_after', 0),
                    )
                except (OSError, ValueError, TypeError) as e:
                    print(f"  [ExitLogic] close_log 기록 실패 (비치명): {e}")

        return result
