"""
DataProvider (Abstract Base)
=============================
Kiwoom / PykrxProvider / MockProvider 공통 인터페이스.
모든 Provider는 이 클래스를 상속해야 한다.
"""

from abc import ABC, abstractmethod
import pandas as pd


class DataProvider(ABC):

    @abstractmethod
    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """지수 OHLCV 반환 (code: "KOSPI" | "KOSDAQ")."""
        ...

    @abstractmethod
    def get_stock_list(self, market: str) -> list:
        """시장별 종목코드 리스트 (market: "KOSPI" | "KOSDAQ")."""
        ...

    @abstractmethod
    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """종목 OHLCV 반환 (컬럼: date, open, high, low, close, volume)."""
        ...

    @abstractmethod
    def get_stock_info(self, code: str) -> dict:
        """종목 기본정보 반환 (keys: name, sector, market_cap, listed_shares)."""
        ...

    @abstractmethod
    def get_avg_daily_volume(self, code: str, days: int) -> float:
        """N일 평균 거래대금(원) 반환."""
        ...

    @abstractmethod
    def get_current_price(self, code: str) -> float:
        """종목 현재가 반환."""
        ...

    def get_investor_trend(self, code: str, days: int) -> dict:
        """
        외인/기관 순매수 트렌드 반환.
        반환 키: foreign_net, institute_net, total_volume
        미구현 Provider는 빈 dict 반환.
        """
        return {}
