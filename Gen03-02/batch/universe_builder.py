"""
UniverseBuilder
===============
KOSPI + KOSDAQ 전체 종목에서 거래 가능한 유니버스를 구성한다.

제외 조건:
  - 우선주 (종목코드 끝자리 5~9)
  - 관리종목 / 거래정지 (종목명 키워드)
  - 주가 min_price 미만 (동전주)
  - 시총 min_market_cap 미만
  - 5일 평균 거래대금 min_daily_volume 미만
"""

import re
from typing import List, Dict

from config import Gen3Config


def _progress_bar(current: int, total: int, width: int = 40) -> str:
    filled = int(width * current / total) if total else 0
    bar    = "█" * filled + "░" * (width - filled)
    pct    = current / total * 100 if total else 0
    return f"\r[{bar}] {pct:5.1f}%  {current:4d}/{total}개"

_ADMIN_KEYWORDS      = ["관리", "거래정지", "정리매매"]
_PREFERRED_CODE_PAT  = re.compile(r"\d{5}[5-9]$")


class UniverseBuilder:

    def __init__(self, provider, config: Gen3Config):
        self.provider        = provider
        self.min_price       = config.min_price
        self.min_market_cap  = config.min_market_cap
        self.min_daily_vol   = config.min_daily_volume

    def build(self, max_stocks: int = 0) -> List[str]:
        """필터링 완료된 종목 코드 리스트 반환. max_stocks>0 이면 샘플 제한."""
        codes = []
        for market in ("KOSPI", "KOSDAQ"):
            codes.extend(self.provider.get_stock_list(market))
        print(f"[UniverseBuilder] 전체 종목: {len(codes)}개")
        if max_stocks > 0:
            codes = codes[:max_stocks]
            print(f"[UniverseBuilder] 샘플 제한: {max_stocks}개")

        result: List[str] = []
        fail_count: int   = 0
        fail_reasons: Dict[str, int] = {}
        total = len(codes)

        for i, code in enumerate(codes, 1):
            print(_progress_bar(i, total), end="", flush=True)
            if _PREFERRED_CODE_PAT.match(code):
                continue
            try:
                info  = self.provider.get_stock_info(code)
                price = self.provider.get_current_price(code)
                if not self._passes_filter(info, price, code):
                    continue
                result.append(code)
            except Exception as e:
                fail_count += 1
                key = type(e).__name__
                fail_reasons[key] = fail_reasons.get(key, 0) + 1

        print()  # 완료 후 줄바꿈
        if fail_count:
            print(f"[UniverseBuilder] 오류 제외: {fail_count}개 — {fail_reasons}")
        print(f"[UniverseBuilder] 필터 후: {len(result)}개")
        return result

    def _passes_filter(self, info: dict, price: float, code: str) -> bool:
        name = info.get("name", "")
        for kw in _ADMIN_KEYWORDS:
            if kw in name:
                return False

        if price < self.min_price:
            return False

        market_cap = info.get("market_cap", 0)
        if market_cap and market_cap < self.min_market_cap:
            return False

        try:
            avg_vol = self.provider.get_avg_daily_volume(code, days=5)
            if avg_vol < self.min_daily_vol:
                return False
        except Exception as e:
            import logging
            logging.getLogger("UniverseBuilder").debug(
                "[UniverseBuilder] %s 거래대금 조회 실패: %s", code, e
            )

        return True
