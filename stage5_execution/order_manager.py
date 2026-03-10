"""
OrderManager
============
positioned 리스트(Stage4 출력)를 받아
ExecutionEngine에 넘길 Order 객체를 생성하고 일괄 실행.

역할:
  - positioned → Order 변환
  - ExecutionEngine.execute() 호출
  - TradeResult 수집 및 반환
"""

from stage5_execution.execution_engine import ExecutionEngine, Order, TradeResult
from core.data_provider import DataProvider


class OrderManager:

    def __init__(self, engine: ExecutionEngine, provider: DataProvider):
        self.engine   = engine
        self.provider = provider

    def execute_all(self, positioned: list[dict]) -> list[TradeResult]:
        """
        positioned : RiskManager.apply() 반환값
        반환       : TradeResult 리스트
        """
        results = []

        for item in positioned:
            code  = item["code"]
            info  = self.provider.get_stock_info(code)
            sector = info.get("sector", "기타")

            order = Order(
                code     = code,
                sector   = sector,
                side     = "BUY",
                quantity = item["shares"],
                price    = item["entry_price"],
            )

            result = self.engine.execute(order)
            results.append(result)
            print(f"  {result}")

        accepted = [r for r in results if not r.rejected]
        rejected = [r for r in results if r.rejected]
        print(f"[Stage5] 체결: {len(accepted)}건 / 거부: {len(rejected)}건")
        return results
