"""
OrderManager
============
진입 주문 목록을 순서대로 OrderExecutor에 위임하고 결과를 수집한다.

MarginState: 증거금 부족 상태를 Stage 간 전파하기 위한 상태 객체.
  - exhausted: 현재 사이클 내 증거금 부족 여부
  - reason: 원인 코드 (RC4025, 800033, ...)
  - scope: "cycle" (현재 런타임 사이클 한정) / "account" (계좌 전체)
  - detected_at: 최초 감지 시각
"""

from datetime import datetime
from typing import List, Dict, Any

from core.order_tracker import OrderTracker


class OrderManager:

    def __init__(self, executor, provider):
        self.executor = executor
        self.provider = provider
        self.tracker  = OrderTracker()

        # ── MarginState: Stage 간 전파 가능한 증거금 상태 ──────────────
        self.margin_state: Dict[str, Any] = self._make_margin_state()

        # BUG-2: TIMEOUT_UNCERTAIN 예약 차감 추적 {code: reserved_amount}
        self.timeout_reserved: Dict[str, float] = {}

    @staticmethod
    def _make_margin_state() -> Dict[str, Any]:
        return {
            "exhausted":   False,
            "reason":      "",
            "scope":       "",          # "cycle" | "account"
            "detected_at": None,
        }

    def reset_margin_state(self) -> None:
        """새 런타임 사이클 시작 시 호출. 예수금 재조회 후 해제."""
        self.margin_state = self._make_margin_state()

    def execute_all(self, positioned: List[Dict[str, Any]]) -> list:
        """
        positioned : RiskManager 반환값 (entry_price, shares, tp, sl … 포함)
        반환: TradeResult 리스트

        margin_state.exhausted가 이미 True이면 전체 스킵 (Stage 간 전파).
        """
        if not positioned:
            print("[OrderManager] 진입 후보 없음")
            return []

        # 이전 Stage에서 증거금 부족이 이미 감지된 경우
        if self.margin_state["exhausted"]:
            print(f"[OrderManager] 증거금 부족 상태 유지 — {len(positioned)}건 전체 스킵 "
                  f"(사유: {self.margin_state['reason']}, "
                  f"감지: {self.margin_state['detected_at']})")
            return []

        results = []

        for item in positioned:
            if self.margin_state["exhausted"]:
                print(f"  [OrderManager] {item['code']} 스킵: 증거금 부족으로 잔여 주문 중단")
                continue

            from runtime.order_executor import Order
            order = Order(
                code     = item["code"],
                sector   = item.get("sector", "기타"),
                side     = "BUY",
                quantity = item["shares"],
                price    = item["entry_price"],
            )

            # ── BUG-1 FIX: OrderTracker 등록 및 상태 추적 ──────────────────
            rec = self.tracker.register(
                code     = item["code"],
                sector   = item.get("sector", "기타"),
                side     = "BUY",
                quantity = item["shares"],
                price    = item["entry_price"],
            )
            self.tracker.mark_submitted(rec.order_id)

            # BUG-5 FIX: execute 예외 시 tracker 정합성 보장
            _already_tracked = False
            try:
                result = self.executor.execute(order)
            except Exception as exc:
                from runtime.order_executor import TradeResult
                result = TradeResult(
                    code=item["code"], side="BUY", quantity=0,
                    exec_price=0, slippage_pct=0, timestamp=datetime.now(),
                    rejected=True, reject_reason=f"REJECTED_PROVIDER_DEAD: {type(exc).__name__}: {exc}",
                )
                self.tracker.mark_rejected(rec.order_id, result.reject_reason)
                _already_tracked = True
                print(f"  [OrderManager] {item['code']} EXEC_ERROR: {exc}")
            results.append(result)

            if _already_tracked:
                pass  # 예외 블록에서 이미 tracker 처리 완료
            elif result.rejected:
                reason = result.reject_reason or ""
                # TIMEOUT_UNCERTAIN 분리 처리
                if "TIMEOUT_UNCERTAIN" in reason:
                    self.tracker.mark_timeout_uncertain(rec.order_id)
                    # BUG-2 FIX: ghost fill 대비 cash 예약 차감 (과잉 매수 방지)
                    reserve_amount = item["entry_price"] * item["shares"]
                    self.executor.portfolio.cash -= reserve_amount
                    self.timeout_reserved[item["code"]] = reserve_amount
                    print(f"  [OrderManager] TIMEOUT_UNCERTAIN {item['code']} — "
                          f"cash 예약 차감 {reserve_amount:,.0f}원 "
                          f"(잔여 cash={self.executor.portfolio.cash:,.0f}원)")
                else:
                    self.tracker.mark_rejected(rec.order_id, reason)
                # 증거금/매수가능금액 부족 → MarginState 활성화 (BUY만)
                reason = result.reject_reason or ""
                if order.side == "BUY" and ("RC4025" in reason or "증거금" in reason or "매수" in reason):
                    self.margin_state = {
                        "exhausted":   True,
                        "reason":      self._extract_error_code(reason),
                        "scope":       "cycle",
                        "detected_at": datetime.now().strftime("%H:%M:%S"),
                    }
                    print(f"  [OrderManager] *** 증거금 부족 — MarginState 활성화 "
                          f"(scope=cycle, 잔여 주문+이후 Stage 스킵) ***")
            else:
                _exec_price = getattr(result, "exec_price", item["entry_price"])
                _exec_qty   = getattr(result, "quantity", item["shares"])
                _order_no   = getattr(result, "order_no", "")
                self.tracker.mark_filled(
                    rec.order_id,
                    exec_price = _exec_price,
                    exec_qty   = _exec_qty,
                )
                # v7.8: Fill Ledger 기록 (중복 반영 방지)
                self.tracker.record_fill(
                    order_no=_order_no, side="BUY", code=item["code"],
                    exec_qty=_exec_qty, exec_price=_exec_price,
                    cumulative_qty=_exec_qty, source="CHEJAN",
                )
                # v7.1: Position 메타데이터 설정 (stage, order_no, source_signal_date)
                pos = self.executor.portfolio.positions.get(item["code"])
                if pos:
                    pos.stage = item.get("stage", "")
                    pos.source_signal_date = item.get("date", "")
                    pos.order_no = _order_no
                # v7.8: 구조화 체결 로그
                pos = self.executor.portfolio.positions.get(item["code"])
                _net = pos.quantity if pos else 0
                print(f"  [OrderManager] [{result.side}] {result.code} "
                      f"order_no={_order_no} requested={item['shares']} "
                      f"filled={_exec_qty} net_qty={_net} "
                      f"price={_exec_price:,.0f} slip={result.slippage_pct:.3%}")

        accepted = [r for r in results if not r.rejected]
        print(f"[OrderManager] 체결: {len(accepted)}/{len(positioned)}건")
        return results

    def restore_timeout_reserve(self, code: str) -> float:
        """TIMEOUT_UNCERTAIN 미체결 확정 시 예약 차감 복원. 반환: 복원 금액."""
        amount = self.timeout_reserved.pop(code, 0.0)
        if amount > 0:
            self.executor.portfolio.cash += amount
            print(f"[OrderManager] TIMEOUT 미체결 확정 {code} — "
                  f"cash 복원 {amount:,.0f}원 "
                  f"(잔여 cash={self.executor.portfolio.cash:,.0f}원)")
        return amount

    @staticmethod
    def _extract_error_code(reason: str) -> str:
        """거부 사유에서 에러 코드 추출. 예: '[RC4025] ...' → 'RC4025'"""
        import re
        m = re.search(r'\[([A-Z0-9]+)\]', reason)
        return m.group(1) if m else reason[:30]
