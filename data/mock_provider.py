import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.data_provider import DataProvider


class MockProvider(DataProvider):
    """
    API 없이 로컬 테스트용 가짜 데이터 제공.
    회사 노트북 개발 환경에서 사용.
    """

    def __init__(self, seed: int = 42):
        np.random.seed(seed)
        self._stock_list = {
            "KOSPI":  ["005930", "000660", "207940", "005380", "051910"],
            "KOSDAQ": ["035720", "247540", "086520", "196170", "091990"],
        }
        self._sectors = {
            "005930": "반도체",  "000660": "반도체",
            "207940": "바이오",  "005380": "자동차",
            "051910": "화학",    "035720": "IT",
            "247540": "바이오",  "086520": "바이오",
            "196170": "바이오",  "091990": "바이오",
        }

    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """가짜 지수 OHLCV — 완만한 상승 추세"""
        base = 2500 if code == "KOSPI" else 800
        return self._make_ohlcv(base, days, trend=0.005)

    def get_stock_list(self, market: str) -> list[str]:
        return self._stock_list.get(market, [])

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """가짜 종목 OHLCV"""
        base = np.random.randint(10_000, 100_000)
        return self._make_ohlcv(base, days, trend=0.0003)

    def get_stock_info(self, code: str) -> dict:
        return {
            "name":          f"종목_{code}",
            "sector":        self._sectors.get(code, "기타"),
            "market_cap":    np.random.randint(1_000, 100_000) * 1_000_000,
            "listed_shares": np.random.randint(10_000_000, 500_000_000),
        }

    def get_foreign_institution_data(self, code: str, days: int) -> pd.DataFrame:
        dates = [datetime.today() - timedelta(days=i) for i in range(days)][::-1]
        return pd.DataFrame({
            "date":            dates,
            "foreign_net":     np.random.randint(-100_000, 100_000, days),
            "institution_net": np.random.randint(-50_000, 50_000, days),
        })

    def get_avg_daily_volume(self, code: str, days: int) -> float:
        """5억~50억 사이 랜덤 거래대금 (유동성 필터 통과 수준)"""
        return np.random.uniform(500_000_000, 5_000_000_000)

    def get_current_price(self, code: str) -> float:
        return float(np.random.randint(10_000, 100_000))

    # ── 헬퍼 ─────────────────────────────────────────────

    @staticmethod
    def _make_ohlcv(base: float, days: int, trend: float) -> pd.DataFrame:
        dates  = [datetime.today() - timedelta(days=i) for i in range(days)][::-1]
        closes = [base]
        for _ in range(days - 1):
            change = np.random.normal(trend, 0.015)
            closes.append(closes[-1] * (1 + change))

        closes = np.array(closes)
        highs  = closes * np.random.uniform(1.001, 1.02, days)
        lows   = closes * np.random.uniform(0.98, 0.999, days)
        opens  = closes * np.random.uniform(0.99, 1.01, days)
        vols   = np.random.randint(100_000, 10_000_000, days)

        return pd.DataFrame({
            "date":   dates,
            "open":   opens.astype(int),
            "high":   highs.astype(int),
            "low":    lows.astype(int),
            "close":  closes.astype(int),
            "volume": vols,
        })
