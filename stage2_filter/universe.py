"""
Universe
========
KOSPI + KOSDAQ 전체 종목에서 기본 필터링을 거쳐
거래 가능한 종목 풀(universe)을 반환한다.

제외 조건:
  - 관리종목 / 거래정지 (종목명 키워드)
  - 주가 1,000원 미만 (동전주)
"""

import re
from core.data_provider import DataProvider


# 관리종목/거래정지 종목명 패턴
_ADMIN_KEYWORDS = ["관리", "거래정지", "정리매매"]

# 우선주 종목코드 패턴 (끝자리 5~9)
_PREFERRED_CODE_PATTERN = re.compile(r"\d{5}[5-9]$")


class Universe:
    """
    KOSPI + KOSDAQ 유니버스 생성.

    Parameters
    ----------
    provider  : DataProvider
    min_price : int — 최소 주가 (기본 1,000원)
    """

    def __init__(
        self,
        provider: DataProvider,
        min_price: int = 1_000,
    ):
        self.provider  = provider
        self.min_price = min_price

    def get_universe(self) -> list[str]:
        """필터링 완료된 종목 코드 리스트 반환."""
        codes = []
        for market in ["KOSPI", "KOSDAQ"]:
            codes.extend(self.provider.get_stock_list(market))

        result = []
        for code in codes:
            if self._is_excluded_by_code(code):
                continue
            info  = self.provider.get_stock_info(code)
            price = self.provider.get_current_price(code)
            if self._is_excluded_by_info(info, price):
                continue
            result.append(code)

        print(f"[Universe] 전체: {len(codes)}개 → 필터 후: {len(result)}개")
        return result

    # ── 내부 필터 ────────────────────────────────────────────────────────────

    def _is_excluded_by_code(self, code: str) -> bool:
        """종목코드만으로 빠르게 제외 (API 호출 없음)."""
        if _PREFERRED_CODE_PATTERN.match(code):
            return True
        return False

    def _is_excluded_by_info(self, info: dict, price: float) -> bool:
        """관리종목/거래정지, 동전주 제외."""
        name = info.get("name", "")

        for kw in _ADMIN_KEYWORDS:
            if kw in name:
                return True

        if price < self.min_price:
            return True

        return False
