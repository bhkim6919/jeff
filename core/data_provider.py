from abc import ABC, abstractmethod
import pandas as pd


class DataProvider(ABC):
    """
    Kiwoom(실거래) / Mock(테스트) 공통 인터페이스.
    모든 Provider는 이 클래스를 상속해야 한다.
    """

    @abstractmethod
    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        지수 OHLCV 반환.
        code: "KOSPI" | "KOSDAQ"
        반환 컬럼: date, open, high, low, close, volume
        """
        ...

    @abstractmethod
    def get_stock_list(self, market: str) -> list[str]:
        """
        시장별 종목 코드 리스트 반환.
        market: "KOSPI" | "KOSDAQ"
        """
        ...

    @abstractmethod
    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        종목 OHLCV 반환.
        반환 컬럼: date, open, high, low, close, volume
        """
        ...

    @abstractmethod
    def get_stock_info(self, code: str) -> dict:
        """
        종목 기본 정보 반환.
        반환 키: name, sector, market_cap, listed_shares
        """
        ...

    @abstractmethod
    def get_foreign_institution_data(self, code: str, days: int) -> pd.DataFrame:
        """
        외국인/기관 순매수 데이터 반환.
        반환 컬럼: date, foreign_net, institution_net
        """
        ...

    @abstractmethod
    def get_avg_daily_volume(self, code: str, days: int) -> float:
        """
        N일 평균 거래대금(원) 반환.
        유동성 필터 및 슬리피지 계산에 사용.
        """
        ...

    @abstractmethod
    def get_current_price(self, code: str) -> float:
        """종목 현재가 반환."""
        ...
