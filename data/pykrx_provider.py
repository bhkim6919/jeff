"""
PykrxProvider
=============
DataProvider 인터페이스의 pykrx 기반 실데이터 구현체.
Python 3.9 호환. pykrx v1.0.51 실측 컬럼 기준.

실측 컬럼:
  get_market_ohlcv_by_date  → 인덱스:날짜 / 컬럼:시가 고가 저가 종가 거래량 등락률
  get_market_cap_by_ticker  → 인덱스:티커 / 컬럼:시가총액 거래량 거래대금 상장주수
  get_market_ohlcv_by_ticker→ 장마감일만 유효, 휴장일 오류 → 최근영업일 종가로 대체
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    raise ImportError("pip install pykrx")

from core.data_provider import DataProvider

_API_DELAY = 0.3
_INDEX_TICKER = {"KOSPI": "1001", "KOSDAQ": "2001"}


def _today() -> str:
    return datetime.today().strftime("%Y%m%d")

def _n_days_ago(n: int) -> str:
    return (datetime.today() - timedelta(days=n)).strftime("%Y%m%d")

def _last_business_day() -> str:
    """
    가장 최근 영업일 반환 (KRX 서버 호출 없이 요일 계산만 사용).
    - 장외 시간 / 주말에도 안전하게 동작
    - 공휴일은 처리하지 않음 (pykrx가 빈 DataFrame 반환 → 자연 처리)
    """
    d = datetime.today()
    # 오늘이 장 마감 전(15:30 이전)이면 오늘 포함, 이후면 오늘 제외
    if d.hour < 15 or (d.hour == 15 and d.minute < 30):
        # 오늘이 평일이면 오늘 반환
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    # 가장 최근 평일(금요일 방향으로 탐색)
    for i in range(1, 8):
        prev = d - timedelta(days=i)
        if prev.weekday() < 5:
            return prev.strftime("%Y%m%d")
    return d.strftime("%Y%m%d")


class PykrxProvider(DataProvider):

    def __init__(self):
        self._info_cache: Dict[str, dict] = {}
        self._info_cache_date: str = ""
        self._list_cache: Dict[str, List[str]] = {}
        self._list_cache_date: str = ""
        self._price_cache: Dict[str, float] = {}
        self._price_cache_date: str = ""

    # ── DataProvider 인터페이스 ──────────────────────────────────────────────

    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        ticker   = _INDEX_TICKER.get(code, "1001")
        fromdate = _n_days_ago(days + 60)
        todate   = _last_business_day()
        try:
            df = krx.get_index_ohlcv_by_date(fromdate, todate, ticker)
            time.sleep(_API_DELAY)
            if df is not None and not df.empty:
                return self._parse_ohlcv(df, days)
        except Exception as e:
            print(f"[PykrxProvider] 지수 OHLCV 실패 ({code}): {e}")
        return self._fallback_index(code, days)

    def get_stock_list(self, market: str) -> List[str]:
        today = _today()
        if self._list_cache_date == today and market in self._list_cache:
            return self._list_cache[market]
        bday = _last_business_day()
        try:
            codes = krx.get_market_ticker_list(bday, market=market)
            time.sleep(_API_DELAY)
        except Exception as e:
            print(f"[PykrxProvider] 종목 리스트 실패 ({market}): {e}")
            codes = []
        self._list_cache[market] = list(codes)
        self._list_cache_date = today
        return self._list_cache[market]

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        fromdate = _n_days_ago(days + 60)
        todate   = _last_business_day()
        try:
            df = krx.get_market_ohlcv_by_date(fromdate, todate, code)
            time.sleep(_API_DELAY)
        except Exception as e:
            print(f"[PykrxProvider] OHLCV 실패 ({code}): {e}")
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])
        if df is None or df.empty:
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])
        return self._parse_ohlcv(df, days)

    def get_stock_info(self, code: str) -> dict:
        self._ensure_info_cache()
        return self._info_cache.get(code, {
            "name": f"종목_{code}", "sector": "기타",
            "market_cap": 0, "listed_shares": 0,
        })

    def get_foreign_institution_data(self, code: str, days: int) -> pd.DataFrame:
        fromdate = _n_days_ago(days + 30)
        todate   = _last_business_day()
        empty    = pd.DataFrame(columns=["date","foreign_net","institution_net"])
        try:
            df = krx.get_market_trading_volume_by_date(fromdate, todate, code)
            time.sleep(_API_DELAY)
        except Exception as e:
            print(f"[PykrxProvider] 외인/기관 실패 ({code}): {e}")
            return empty
        if df is None or df.empty:
            return empty
        return self._parse_foreign_data(df, days)

    def get_avg_daily_volume(self, code: str, days: int) -> float:
        df = self.get_stock_ohlcv(code, days)
        if df.empty:
            return 0.0
        df["amount"] = df["close"] * df["volume"]
        return float(df["amount"].mean())

    def get_current_price(self, code: str) -> float:
        today = _today()
        if self._price_cache_date == today and code in self._price_cache:
            return self._price_cache[code]
        if self._price_cache_date != today:
            self._bulk_refresh_prices()
        return self._price_cache.get(code, 0.0)

    # ── 캐시 ────────────────────────────────────────────────────────────────

    def _ensure_info_cache(self):
        today = _today()
        if self._info_cache_date == today:
            return
        print("[PykrxProvider] 종목 기본정보 일괄 조회 중... (최초 1회, 약 10~20초)")
        bday     = _last_business_day()
        info_map: Dict[str, dict] = {}
        for market in ["KOSPI", "KOSDAQ"]:
            try:
                df_cap = krx.get_market_cap_by_ticker(bday, market=market)
                time.sleep(_API_DELAY)
                try:
                    df_sec = krx.get_market_sector_classifications(bday, market=market)
                    time.sleep(_API_DELAY)
                except Exception:
                    df_sec = None
                for code in df_cap.index:
                    try:
                        name = krx.get_market_ticker_name(code)
                    except Exception:
                        name = f"종목_{code}"
                    sector = "기타"
                    if df_sec is not None and not df_sec.empty and code in df_sec.index:
                        try:
                            sector = str(df_sec.loc[code].iloc[0])
                        except Exception:
                            pass
                    row    = df_cap.loc[code]
                    cap    = int(row.get("시가총액", 0)) if "시가총액" in df_cap.columns else int(row.iloc[0])
                    shares = int(row.get("상장주수", 0)) if "상장주수" in df_cap.columns else 0
                    info_map[code] = {
                        "name": name or f"종목_{code}",
                        "sector": sector,
                        "market_cap": cap,
                        "listed_shares": shares,
                    }
            except Exception as e:
                print(f"[PykrxProvider] {market} 기본정보 실패: {e}")
        self._info_cache      = info_map
        self._info_cache_date = today
        print(f"[PykrxProvider] 기본정보 캐시 완료: {len(info_map)}개 종목")

    def _bulk_refresh_prices(self):
        print("[PykrxProvider] 현재가 일괄 조회 중...")
        bday      = _last_business_day()
        price_map: Dict[str, float] = {}
        for market in ["KOSPI", "KOSDAQ"]:
            try:
                df = krx.get_market_ohlcv_by_ticker(bday, market=market)
                time.sleep(_API_DELAY)
                if df is not None and not df.empty:
                    # 종가 컬럼 탐색
                    close_col = None
                    for c in ["종가", "close"]:
                        if c in df.columns:
                            close_col = c
                            break
                    if close_col is None and len(df.columns) >= 4:
                        close_col = df.columns[3]
                    if close_col:
                        for code in df.index:
                            try:
                                price_map[code] = float(df.loc[code, close_col])
                            except Exception:
                                pass
            except Exception as e:
                print(f"[PykrxProvider] {market} 현재가 일괄 실패: {e} → 개별 폴백")
                self._fallback_prices_individual(market, bday, price_map)
        self._price_cache      = price_map
        self._price_cache_date = _today()
        print(f"[PykrxProvider] 현재가 캐시 완료: {len(price_map)}개 종목")

    def _fallback_prices_individual(self, market: str, bday: str,
                                    price_map: Dict[str, float]):
        codes = self.get_stock_list(market)
        for code in codes[:100]:
            try:
                df = krx.get_market_ohlcv_by_date(_n_days_ago(5), bday, code)
                if df is not None and not df.empty and "종가" in df.columns:
                    price_map[code] = float(df["종가"].iloc[-1])
                time.sleep(_API_DELAY)
            except Exception:
                continue

    # ── 파싱 헬퍼 ────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_ohlcv(df: pd.DataFrame, days: int) -> pd.DataFrame:
        """pykrx OHLCV → 표준 포맷. 실측 컬럼: 시가/고가/저가/종가/거래량"""
        df = df.reset_index()
        df = df.rename(columns={df.columns[0]: "date"})
        col_map = {"시가":"open","고가":"high","저가":"low","종가":"close","거래량":"volume"}
        df = df.rename(columns=col_map)
        keep = [c for c in ["date","open","high","low","close","volume"] if c in df.columns]
        df   = df[keep].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for col in ["open","high","low","close","volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df.tail(days).reset_index(drop=True)

    @staticmethod
    def _parse_foreign_data(df: pd.DataFrame, days: int) -> pd.DataFrame:
        df = df.reset_index()
        df = df.rename(columns={df.columns[0]: "date"})
        result = pd.DataFrame()
        result["date"] = pd.to_datetime(df["date"], errors="coerce")
        for cand in ["외국인","외국인순매수","외국인_순매수","외국인 합계"]:
            if cand in df.columns:
                result["foreign_net"] = pd.to_numeric(df[cand], errors="coerce").fillna(0)
                break
        else:
            result["foreign_net"] = 0
        for cand in ["기관합계","기관순매수","기관합계_순매수","기관 합계"]:
            if cand in df.columns:
                result["institution_net"] = pd.to_numeric(df[cand], errors="coerce").fillna(0)
                break
        else:
            result["institution_net"] = 0
        result = result.sort_values("date").reset_index(drop=True)
        return result.tail(days).reset_index(drop=True)

    def _fallback_index(self, code: str, days: int) -> pd.DataFrame:
        proxy = {"KOSPI": "005930", "KOSDAQ": "068270"}.get(code, "005930")
        print(f"[PykrxProvider] 지수 폴백: {code} → {proxy}")
        return self.get_stock_ohlcv(proxy, days)
