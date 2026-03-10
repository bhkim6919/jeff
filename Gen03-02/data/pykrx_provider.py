"""
PykrxProvider
=============
DataProvider 인터페이스의 pykrx 기반 실데이터 구현체.
Python 3.9 호환. pykrx v1.0.51 실측 컬럼 기준.

실측 컬럼:
  get_market_ohlcv_by_date  → 인덱스:날짜 / 컬럼:시가 고가 저가 종가 거래량 등락률
  get_market_cap_by_ticker  → 인덱스:티커 / 컬럼:시가총액 거래량 거래대금 상장주수
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    raise ImportError("pip install pykrx")

from data.data_provider import DataProvider

_API_DELAY    = 0.3
_INDEX_TICKER = {"KOSPI": "1001", "KOSDAQ": "2001"}


def _today() -> str:
    return datetime.today().strftime("%Y%m%d")

def _n_days_ago(n: int) -> str:
    return (datetime.today() - timedelta(days=n)).strftime("%Y%m%d")

def _last_business_day() -> str:
    """OHLCV todate용 — 장중(09:00~15:30)에는 오늘 포함, 이외엔 직전 영업일"""
    d = datetime.today()
    if d.hour < 15 or (d.hour == 15 and d.minute < 30):
        if d.weekday() < 5:
            return d.strftime("%Y%m%d")
    for i in range(1, 8):
        prev = d - timedelta(days=i)
        if prev.weekday() < 5:
            return prev.strftime("%Y%m%d")
    return d.strftime("%Y%m%d")

def _prev_completed_bday() -> str:
    """종가 기준 가격 조회용 — 항상 직전 완료된 영업일(오늘 제외)"""
    d = datetime.today()
    for i in range(1, 8):
        prev = d - timedelta(days=i)
        if prev.weekday() < 5:
            return prev.strftime("%Y%m%d")
    return d.strftime("%Y%m%d")


class PykrxProvider(DataProvider):

    def __init__(self):
        self._info_cache:       Dict[str, dict]  = {}
        self._info_cache_date:  str              = ""
        self._list_cache:       Dict[str, List]  = {}
        self._list_cache_date:  str              = ""
        self._price_cache:      Dict[str, float] = {}
        self._price_cache_date: str              = ""

    # ── DataProvider 인터페이스 ──────────────────────────────────────────────

    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        ticker   = _INDEX_TICKER.get(code, "1001")
        fromdate = _n_days_ago(days + 60)
        todate   = _last_business_day()
        try:
            df = krx.get_index_ohlcv_by_date(fromdate, todate, ticker, name_display=False)
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
        self._list_cache[market]  = list(codes)
        self._list_cache_date     = today
        return self._list_cache[market]

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        fromdate = _n_days_ago(days + 60)
        todate   = _last_business_day()
        empty    = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        try:
            df = krx.get_market_ohlcv_by_date(fromdate, todate, code)
            time.sleep(_API_DELAY)
        except Exception as e:
            print(f"[PykrxProvider] OHLCV 실패 ({code}): {e}")
            return empty
        if df is None or df.empty:
            return empty
        return self._parse_ohlcv(df, days)

    def get_stock_info(self, code: str) -> dict:
        self._ensure_info_cache()
        return self._info_cache.get(code, {
            "name": f"종목_{code}", "sector": "기타",
            "market_cap": 0, "listed_shares": 0,
        })

    def get_avg_daily_volume(self, code: str, days: int) -> float:
        df = self.get_stock_ohlcv(code, days)
        if df.empty:
            return 0.0
        df["amount"] = df["close"].astype("int64") * df["volume"].astype("int64")
        return max(0.0, float(df["amount"].mean()))

    def get_current_price(self, code: str) -> float:
        today = _today()
        # 캐시 히트
        if self._price_cache_date == today and code in self._price_cache:
            return self._price_cache[code]
        # 날짜 변경 → bulk 재시도 (bulk가 작동하면 빠름)
        if self._price_cache_date != today:
            self._bulk_refresh_prices()
        # bulk 실패 또는 해당 종목 누락 → 개별 OHLCV 폴백
        if code not in self._price_cache:
            try:
                df = self.get_stock_ohlcv(code, days=3)
                if not df.empty and "close" in df.columns:
                    price = float(df["close"].iloc[-1])
                    if price > 0:
                        self._price_cache[code] = price
            except Exception:
                pass
        return self._price_cache.get(code, 0.0)

    def get_investor_trend(self, code: str, days: int) -> dict:
        fromdate = _n_days_ago(days + 30)
        todate   = _last_business_day()
        try:
            df = krx.get_market_trading_volume_by_date(fromdate, todate, code)
            time.sleep(_API_DELAY)
            if df is None or df.empty:
                return {}
            foreign   = 0
            institute = 0
            for cand in ["외국인", "외국인순매수", "외국인 합계"]:
                if cand in df.columns:
                    foreign = int(df[cand].tail(days).sum())
                    break
            for cand in ["기관합계", "기관순매수", "기관 합계"]:
                if cand in df.columns:
                    institute = int(df[cand].tail(days).sum())
                    break
            total_vol = int(df.get("거래량", pd.Series([1])).tail(days).sum()) or 1
            return {"foreign_net": foreign, "institute_net": institute, "total_volume": total_vol}
        except Exception:
            return {}

    # ── 캐시 ─────────────────────────────────────────────────────────────────

    def _ensure_info_cache(self):
        today = _today()
        if self._info_cache_date == today:
            return
        print("[PykrxProvider] 종목 기본정보 일괄 조회 중 (최초 1회)...")
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
        print(f"[PykrxProvider] 기본정보 캐시 완료: {len(info_map)}개")

    def _bulk_refresh_prices(self):
        print("[PykrxProvider] 현재가 일괄 조회 중...")
        bday      = _prev_completed_bday()   # 항상 직전 완료 영업일 사용 (오늘 장중 에러 방지)
        price_map: Dict[str, float] = {}
        for market in ["KOSPI", "KOSDAQ"]:
            try:
                df = krx.get_market_ohlcv_by_ticker(bday, market=market)
                time.sleep(_API_DELAY)
                if df is None or df.empty:
                    continue
                # 종가 컬럼 탐색: 이름 우선, 없으면 4번째 컬럼(시가·고가·저가·종가 순)
                close_col = next((c for c in ["종가", "close"] if c in df.columns), None)
                if close_col is None and len(df.columns) >= 4:
                    close_col = df.columns[3]
                if close_col is None:
                    print(f"[PykrxProvider] {market} 종가 컬럼 탐색 실패 (컬럼: {list(df.columns)})")
                    continue
                for code in df.index:
                    try:
                        val = float(df.loc[code, close_col])
                        if val > 0:
                            price_map[code] = val
                    except Exception:
                        pass
            except Exception as e:
                print(f"[PykrxProvider] {market} 현재가 일괄 실패: {e}")
        self._price_cache      = price_map
        self._price_cache_date = _today()
        print(f"[PykrxProvider] 현재가 캐시 완료: {len(price_map)}개 (기준일: {bday})")

    # ── 파싱 헬퍼 ────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_ohlcv(df: pd.DataFrame, days: int) -> pd.DataFrame:
        df  = df.reset_index()
        df  = df.rename(columns={df.columns[0]: "date"})
        col = {"시가": "open", "고가": "high", "저가": "low", "종가": "close", "거래량": "volume"}
        df  = df.rename(columns=col)
        keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
        df  = df[keep].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
        df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df.tail(days).reset_index(drop=True)

    def _fallback_index(self, code: str, days: int) -> pd.DataFrame:
        proxy = {"KOSPI": "005930", "KOSDAQ": "068270"}.get(code, "005930")
        return self.get_stock_ohlcv(proxy, days)
