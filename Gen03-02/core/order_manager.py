"""
OrderManager
============
진입 주문 목록을 순서대로 OrderExecutor에 위임하고 결과를 수집한다.
"""

from typing import List, Dict, Any

from core.order_tracker import OrderTracker


class OrderManager:

    def __init__(self, executor, provider):
        self.executor = executor
        self.provider = provider
        self.tracker  = OrderTracker()

    def execute_all(self, positioned: List[Dict[str, Any]]) -> list:
        """
        positioned : RiskManager 반환값 (entry_price, shares, tp, sl … 포함)
        반환: TradeResult 리스트
        """
        if not positioned:
            print("[OrderManager] 진입 후보 없음")
            return []

        results = []
        for item in positioned:
            from runtime.order_executor import Order
            order = Order(
                code     = item["code"],
                sector   = item.get("sector", "기타"),
                side     = "BUY",
                quantity = item["shares"],
                price    = item["entry_price"],
            )
            result = self.executor.execute(order)
            results.append(result)

            if result.rejected:
                print(f"  [OrderManager] {result.code} 거부: {result.reject_reason}")
            else:
                print(f"  [OrderManager] {result}")

        accepted = [r for r in results if not r.rejected]
        print(f"[OrderManager] 체결: {len(accepted)}/{len(positioned)}건")
        return results
