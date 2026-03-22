"""
OrderExecutor
=============
개별 주문 실행 엔진.
paper_trading=True → 시뮬레이션 / False → Kiwoom 실거래 (안정화 후 구현)

6중 게이트(PortfolioManager.can_enter)를 통과한 주문만 체결 처리.
"""

import logging
import time as _time
from dataclasses import dataclass
from datetime import datetime

from core.exceptions import PriceFetchError
from data.name_lookup import get_name

_log = logging.getLogger("OrderExecutor")


MIN_DAILY_VOLUME    = 2_000_000_000  # 일 거래대금 20억 미만 → 진입 금지
MAX_SLIPPAGE_CAP    = 0.03           # 슬리피지 경고 임계값 3% (LIVE 실거래 시)
MAX_ADVERSE_PRECHECK = 0.05          # 사전 adverse 슬리피지 차단 임계값 5% (주문 전)
ORDER_INTERVAL      = 0.3            # 실주문 간 최소 간격 (초) — Kiwoom 초당 제한 대응
LIMIT_ORDER_THRESH  = 5_000_000_000  # 50억 미만 → 지정가 주문 (슬리피지 방어)


@dataclass
class Order:
    code:     str
    sector:   str
    side:     str      # "BUY" | "SELL"
    quantity: int
    price:    float


@dataclass
class TradeResult:
    code:         str
    side:         str
    quantity:     int
    exec_price:   float
    slippage_pct: float
    timestamp:    datetime
    rejected:     bool = False
    reject_reason: str = ""
    order_no:     str  = ""     # Kiwoom 주문번호 (LIVE 시)
    # v7.7: price source 분리
    decision_price: float = 0.0   # 엔진 판단 호가 (SL/TP/시장가)
    fill_price:     float = 0.0   # 브로커 실체결가
    qty_ordered:    int   = 0     # 주문 요청 수량
    # v7.7: qty 추적
    qty_before:     int   = 0     # 체결 전 보유수량
    qty_after:      int   = 0     # 체결 후 보유수량
    # v7.9: 원장 정합성 — raw vs applied 분리
    raw_fill_qty:   int   = 0     # chejan 원본 체결 수량 (broker 보고)
    applied_qty:    int   = 0     # 실제 포지션에 적용된 수량 (min clamp 후)

    def __str__(self):
        label = f"{get_name(self.code)}({self.code})"
        if self.rejected:
            return f"[REJECTED] {label} — {self.reject_reason}"
        return (
            f"[{self.side}] {label} "
            f"{self.quantity}주 @ {self.exec_price:,.0f}원 "
            f"(슬리피지 {self.slippage_pct:.3%})"
        )


class OrderExecutor:

    def __init__(self, provider, portfolio, paper_trading: bool = True):
        self.provider      = provider
        self.portfolio     = portfolio
        self.paper_trading = paper_trading
        self._last_order_ts: float = 0.0   # 마지막 실주문 시각 (monotonic)

    def execute(self, order: Order) -> TradeResult:
        # ── v7.9: 주문 차단 가드 ──────────────────────────────────────
        block_reason = self._check_order_guard(order)
        if block_reason:
            return self._rejected(order, f"REJECTED_GUARD: {block_reason}")

        # ── Risk Mode 체크 ──────────────────────────────────────────────
        mode = self.portfolio.risk_mode()

        if mode == "HARD_STOP" and order.side == "BUY":
            return self._rejected(order, "REJECTED_VALIDATION: HARD_STOP — 월 DD 한도 초과, BUY 전면 금지")

        if mode == "DAILY_KILL" and order.side == "BUY":
            return self._rejected(order, "REJECTED_VALIDATION: DAILY_KILL — 일 DD -4% 초과, 신규 진입 완전 차단")

        if mode == "SOFT_STOP" and order.side == "BUY":
            return self._rejected(order, "REJECTED_VALIDATION: SOFT_STOP — 일 손실 한도 초과, 신규 진입 금지")

        # ── 유동성 체크 (BUY만 — SELL은 반드시 실행해야 함) ─────────────
        if order.side == "BUY":
            try:
                avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
                if avg_vol < MIN_DAILY_VOLUME:
                    return self._rejected(order, f"REJECTED_VALIDATION: 유동성 부족 (5일 평균 {avg_vol:,.0f}원)")
            except (PriceFetchError, ValueError, KeyError, AttributeError) as e:
                _log.debug("[OrderExecutor] %s 유동성 조회 실패 → 통과 허용: %s", order.code, e)

        # ── 6중 게이트 (BUY) ────────────────────────────────────────────
        if order.side == "BUY":
            amount = order.price * order.quantity
            ok, reason = self.portfolio.can_enter(order.code, amount, order.sector)
            if not ok:
                return self._rejected(order, f"REJECTED_VALIDATION: {reason}")

        # ── Dry-run 로그 (실주문 전 기록) ──────────────────────────────
        order_amount = order.price * order.quantity
        print(f"[ORD] EXECUTE {order.side} {order.code} "
              f"qty={order.quantity} price={order.price:,.0f} "
              f"(paper={self.paper_trading}, mode={mode})")

        # ── 실행 ────────────────────────────────────────────────────────
        if self.paper_trading:
            avg_vol = 0
            try:
                avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
            except (PriceFetchError, ValueError, KeyError, AttributeError) as e:
                _log.debug("[OrderExecutor] %s 슬리피지용 거래대금 조회 실패 → 0 사용: %s", order.code, e)
            return self._simulate(order, avg_vol)
        else:
            # ── v7.4: 사전 adverse 슬리피지 차단 (BUY) ──────────────
            # signal 가격(order.price)이 stale할 수 있으므로
            # 주문 직전 현재가 기준으로 adverse 괴리 검증
            if order.side == "BUY" and order.price > 0:
                try:
                    live_price = self.provider.get_current_price(order.code)
                    if live_price > 0:
                        adverse_slip = (live_price - order.price) / order.price
                        if adverse_slip > MAX_ADVERSE_PRECHECK:
                            _log.warning(
                                "[ADVERSE_PREBLOCK] %s 현재가=%,.0f vs 시그널=%,.0f "
                                "(+%.1f%% > cap %.1f%%) — 주문 사전 차단",
                                order.code, live_price, order.price,
                                adverse_slip * 100, MAX_ADVERSE_PRECHECK * 100,
                            )
                            return self._rejected(
                                order,
                                f"REJECTED_VALIDATION: ADVERSE_PREBLOCK 현재가 {live_price:,.0f} vs "
                                f"시그널 {order.price:,.0f} ({adverse_slip:+.1%})"
                            )
                except Exception as e:
                    _log.debug("[OrderExecutor] %s 사전 가격 조회 실패 → 통과: %s", order.code, e)

            # ── 주문 간 최소 간격 보장 (Kiwoom 초당 제한 대응) ────────
            elapsed = _time.monotonic() - self._last_order_ts
            if elapsed < ORDER_INTERVAL:
                _time.sleep(ORDER_INTERVAL - elapsed)

            # ── 주문 실패 시 재시도 (최대 2회) ────────────────────────
            # TIMEOUT_PENDING은 재주문 금지 (ghost order 중복 리스크)
            result = self._send_to_kiwoom(order)
            for retry in range(2):
                if not result.rejected:
                    break
                if "Kiwoom:" not in result.reject_reason:
                    break   # 리스크 게이트 거부는 재시도 불필요
                if "TIMEOUT_UNCERTAIN" in (result.reject_reason or "") or "TIMEOUT_PENDING" in (result.reject_reason or ""):
                    _log.warning("[OrderExecutor] %s %s TIMEOUT_UNCERTAIN — 재주문 금지 (ghost 방지)",
                                 order.side, order.code)
                    break
                if "order rejected:" in (result.reject_reason or ""):
                    _log.warning("[OrderExecutor] %s %s 서버 거부 — 재시도 불필요",
                                 order.side, order.code)
                    break
                _log.warning("[OrderExecutor] %s %s 주문 실패 → 재시도 %d/2",
                             order.side, order.code, retry + 1)
                _time.sleep(ORDER_INTERVAL * 2)
                result = self._send_to_kiwoom(order)

            self._last_order_ts = _time.monotonic()
            # ── 슬리피지 검증 (LIVE BUY, 체결 후 기록) ─────────────────
            if not result.rejected and order.side == "BUY" and order.price > 0:
                # strategy_ref = signal 기준가, order_ref = 주문 직전 현재가
                strategy_slip = (result.exec_price - order.price) / order.price
                s_abs = abs(strategy_slip)
                s_dir = "ADVERSE" if strategy_slip > 0 else "FAVORABLE"
                if s_abs > MAX_SLIPPAGE_CAP:
                    _log.warning(
                        "[SLIPPAGE_%s] %s %.2f%% > cap %.1f%% "
                        "(시그널=%.0f원, 체결=%.0f원) — 이미 체결됨, 모니터링 필요",
                        s_dir, order.code, s_abs * 100, MAX_SLIPPAGE_CAP * 100,
                        order.price, result.exec_price,
                    )
            return result

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _calc_slippage(self, order: Order, avg_daily_volume: float) -> float:
        """
        거래대금 기반 슬리피지 모델:
          대형주 (거래대금 200억+): 0.3~0.7%
          중형주 (거래대금 50~200억): 0.7~1.5%
          소형주 (거래대금 20~50억): 1.5~3.0%
        """
        order_amount    = order.price * order.quantity
        liquidity_ratio = order_amount / avg_daily_volume if avg_daily_volume > 0 else 1.0

        if avg_daily_volume >= 20_000_000_000:      # 200억+ 대형주
            base = 0.003
            cap  = 0.007
        elif avg_daily_volume >= 5_000_000_000:      # 50~200억 중형주
            base = 0.007
            cap  = 0.015
        else:                                        # 50억 미만 소형주
            base = 0.015
            cap  = 0.030

        liq_penalty = liquidity_ratio * 0.10
        return min(base + liq_penalty, cap)

    def _simulate(self, order: Order, avg_daily_volume: float) -> TradeResult:
        slippage   = self._calc_slippage(order, avg_daily_volume)
        exec_price = (
            order.price * (1 + slippage) if order.side == "BUY"
            else order.price * (1 - slippage)
        )
        # v7.7: qty_before 캡처
        pos = self.portfolio.positions.get(order.code)
        qty_before = pos.quantity if pos else 0

        self.portfolio.update_position(
            order.code, order.sector, order.quantity, exec_price, order.side
        )

        # v7.7: qty_after 캡처
        pos_after = self.portfolio.positions.get(order.code)
        qty_after = pos_after.quantity if pos_after else 0

        print(f"[ORD] FILLED {order.side} {order.code} "
              f"qty={order.quantity} price={exec_price:,.0f} slip={slippage:.3%}")
        return TradeResult(
            code=order.code, side=order.side,
            quantity=order.quantity, exec_price=exec_price,
            slippage_pct=slippage, timestamp=datetime.now(),
            decision_price=order.price, fill_price=exec_price,
            qty_ordered=order.quantity, qty_before=qty_before, qty_after=qty_after,
        )

    MAX_PARTIAL_RETRY = 2   # 부분체결 잔량 재주문 최대 횟수

    def _send_to_kiwoom(self, order: Order) -> TradeResult:
        """Kiwoom API 실주문. 소형주는 지정가, 대형주는 시장가. 부분체결 시 잔량 최대 2회 재주문."""
        if not hasattr(self.provider, 'send_order'):
            return self._rejected(order, "REJECTED_PROVIDER_DEAD: KiwoomProvider가 아님")

        # 주문 방식 결정: 소형주 BUY → 지정가 (슬리피지 방어)
        use_limit = False
        if order.side == "BUY" and order.price > 0:
            try:
                avg_vol = self.provider.get_avg_daily_volume(order.code, days=5)
                if avg_vol < LIMIT_ORDER_THRESH:
                    use_limit = True
                    _log.info("[OrderExecutor] %s 소형주 (거래대금 %.0f억) → 지정가 주문",
                              order.code, avg_vol / 1e8)
            except (PriceFetchError, ValueError, KeyError, AttributeError):
                pass

        hoga_type = "00" if use_limit else "03"       # 00=지정가, 03=시장가
        limit_price = int(order.price) if use_limit else 0

        remaining   = order.quantity
        total_qty   = 0
        total_cost  = 0.0
        last_order_no = ""

        # v7.7: qty_before 캡처
        _pos_before = self.portfolio.positions.get(order.code)
        _qty_before = _pos_before.quantity if _pos_before else 0

        for attempt in range(1 + self.MAX_PARTIAL_RETRY):
            result = self.provider.send_order(
                code=order.code,
                side=order.side,
                quantity=remaining,
                price=limit_price,
                hoga_type=hoga_type,
            )

            if result["error"]:
                if total_qty > 0:
                    # 이전 시도에서 일부 체결됨 — 부분 결과 반환
                    _log.warning("[Kiwoom] %s %s 재주문 실패 (기체결 %d주): %s",
                                order.side, order.code, total_qty, result["error"])
                    break
                _log.error("[Kiwoom] %s %s 실패: %s", order.side, order.code, result["error"])
                return self._rejected(order, f"REJECTED_BROKER: Kiwoom: {result['error']}")

            exec_qty   = result["exec_qty"]
            exec_price = result["exec_price"]
            total_qty  += exec_qty
            total_cost += exec_price * exec_qty
            last_order_no = result.get("order_no", "")

            self.portfolio.update_position(
                order.code, order.sector, exec_qty, exec_price, order.side,
            )

            remaining -= exec_qty
            if remaining <= 0:
                break

            # 부분체결 — 잔량 재주문
            print(f"[ORD] PARTIAL_FILLED {order.side} {order.code} "
                  f"qty={exec_qty}/{order.quantity} remaining={remaining}")
            _log.warning("[Kiwoom] %s %s 부분체결 %d/%d주 → 잔량 %d주 재주문 (%d/%d)",
                         order.side, order.code, exec_qty, order.quantity,
                         remaining, attempt + 1, self.MAX_PARTIAL_RETRY)
            _time.sleep(ORDER_INTERVAL)

        avg_price = total_cost / total_qty if total_qty > 0 else 0.0
        slippage  = abs(avg_price - order.price) / order.price if order.price > 0 else 0.0

        # v7.9: 구조화 체결 로그 — 모든 수량 필드 한 블록 출력
        _pos_cur = self.portfolio.positions.get(order.code)
        _net = _pos_cur.quantity if _pos_cur else 0
        _bh = _pos_cur.broker_confirmed_qty if _pos_cur and hasattr(_pos_cur, 'broker_confirmed_qty') else -1
        _sell = _pos_cur.effective_sellable if _pos_cur else 0
        print(f"[ORD] FILLED {order.side} {order.code} order_no={last_order_no}\n"
              f"  broker_hold={_bh} broker_sellable={_sell}"
              f" engine_hold={_net} engine_net={_pos_cur.net_qty if _pos_cur else 0}"
              f" final_sell_qty={order.quantity} chejan_fill_qty={total_qty}"
              f" close_applied_qty={min(total_qty, order.quantity)}"
              f" price={avg_price:,.0f} slip={slippage:.2%}")
        _log.info(
            "[Kiwoom] %s %s %d주 @ %.0f원 (슬리피지 %.2f%%)",
            order.side, order.code, total_qty, avg_price, slippage * 100,
        )

        if remaining > 0:
            _log.warning("[Kiwoom] %s %s 미체결 잔량 %d주 (총 %d/%d주 체결)",
                         order.side, order.code, remaining, total_qty, order.quantity)
            # v7.6: TIMEOUT_UNCERTAIN 잔량 있으면 포지션을 LOW confidence로 마킹
            pos = self.portfolio.positions.get(order.code)
            if pos and order.side == "BUY":
                pos.mark_restricted("TIMEOUT_UNCERTAIN", confidence="LOW")
                _log.warning("[Kiwoom] %s 포지션 qty_confidence=LOW (미체결 잔량 %d주)",
                             order.code, remaining)

        # v7.7: qty_after 캡처
        _pos_after = self.portfolio.positions.get(order.code)
        _qty_after = _pos_after.quantity if _pos_after else 0

        return TradeResult(
            code=order.code, side=order.side,
            quantity=total_qty, exec_price=avg_price,
            slippage_pct=slippage, timestamp=datetime.now(),
            order_no=last_order_no,
            decision_price=order.price, fill_price=avg_price,
            qty_ordered=order.quantity, qty_before=_qty_before, qty_after=_qty_after,
        )

    def _check_order_guard(self, order: Order) -> str:
        """v7.9: 주문 차단 가드. 차단 사유 반환, 통과 시 빈 문자열."""
        # 1. stale 상태에서 BUY 차단
        #    1차 기준: feed timestamp (실시간 시세 수신 시각)
        #    2차 참고: equity stale (보조 지표)
        if order.side == "BUY":
            # feed timestamp 기반 (provider가 지원하는 경우)
            _provider = self.provider if hasattr(self, 'provider') else None
            if _provider and hasattr(_provider, 'check_real_feed_health'):
                stale_feeds = _provider.check_real_feed_health(stale_sec=300)
                if stale_feeds and len(stale_feeds) >= max(1, len(self.portfolio.positions) // 2):
                    return (f"STALE_FEED — {len(stale_feeds)}종목 시세 미수신 5분+ "
                            f"(전체 {len(self.portfolio.positions)}종목)")
            # equity 기반은 보조 (무포지션/횡보 오탐 방지: 포지션 3개 이상일 때만)
            if len(self.portfolio.positions) >= 3 and self.portfolio.check_stale_equity():
                return f"STALE_EQUITY — stale_age={self.portfolio.get_stale_age_sec():.0f}s (보조)"

        # 2. broker_hold < 0 또는 engine quantity < 0
        pos = self.portfolio.positions.get(order.code)
        if pos:
            if pos.quantity < 0:
                return f"NEGATIVE_QTY — engine_qty={pos.quantity}"
            if pos.broker_confirmed_qty < 0 and pos.broker_confirmed_qty != -1:
                return f"NEGATIVE_BROKER — broker_hold={pos.broker_confirmed_qty}"

        # 3. unresolved qty mismatch (RECON_BLOCK)
        if pos and order.side == "SELL":
            if getattr(pos, 'restricted_reason', '') == "RECON_BLOCK":
                return f"RECON_BLOCK — {order.code} qty 정합성 미해결"

        # 4. 동일 종목 pending sell 존재 시 추가 sell 차단
        if order.side == "SELL" and pos:
            if getattr(pos, 'qty_pending_sell', 0) > 0:
                return f"PENDING_SELL — {order.code} pending_sell={pos.qty_pending_sell}"

        return ""

    @staticmethod
    def _rejected(order: Order, reason: str) -> TradeResult:
        print(f"[ORD] REJECTED {order.side} {order.code} — {reason}")
        return TradeResult(
            code=order.code, side=order.side,
            quantity=0, exec_price=0, slippage_pct=0,
            timestamp=datetime.now(),
            rejected=True, reject_reason=reason,
        )
