"""
csv_provider.py
===============
백테스트용 DataProvider 구현체.

핵심 원칙 — Look-Ahead Bias 차단:
  - 모든 데이터 요청은 "현재 날짜(cursor) 이전" 데이터만 반환
  - cursor는 bt_engine.py가 날짜 루프를 돌며 set_date()로 주입

데이터 소스:
  backtest/data/YYYYMMDD.csv      ← 날짜별 전 종목 OHLCV
  backtest/data/index/KOSPI.csv   ← KOSPI 지수 일봉
  backtest/data/index/KOSDAQ.csv  ← KOSDAQ 지수 일봉

사용:
  provider = CsvProvider(data_dir="backtest/data")
  provider.set_date("20240315")   # 엔진이 날짜마다 호출
  df = provider.get_stock_ohlcv("005930", days=60)
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd

from core.data_provider import DataProvider


class CsvProvider(DataProvider):
    """
    날짜별 CSV 파일을 읽어 DataProvider 인터페이스를 구현하는 백테스트용 Provider.

    메모리 전략:
      - 지수 CSV는 시작 시 전체 로드 (크기 작음)
      - 종목 CSV는 날짜 변경 시 해당 날짜 파일만 로드 + 이전 N일 캐시 유지
    """

    def __init__(self, data_dir: str = "backtest/data"):
        self._data_dir  = Path(data_dir)
        self._index_dir = self._data_dir / "index"

        # 날짜 커서 (엔진이 set_date로 주입)
        self._cursor: str = ""

        # 지수 캐시: {name: DataFrame(date,open,high,low,close,volume)}
        self._index_cache: Dict[str, pd.DataFrame] = {}

        # 종목 일별 캐시: {date_str: DataFrame(code,open,...)}
        self._day_cache: Dict[str, pd.DataFrame] = {}

        # 사전 로드
        self._preload_index()

    # ── 커서 제어 ─────────────────────────────────────────────────────────────

    def set_date(self, date_str: str):
        """
        백테스트 엔진이 매 루프마다 호출.
        date_str: 'YYYYMMDD' 형식
        """
        self._cursor = date_str

    def get_cursor(self) -> str:
        return self._cursor

    # ── DataProvider 인터페이스 구현 ─────────────────────────────────────────

    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        지수 일봉 반환. cursor 이전 데이터만 포함 (look-ahead 차단).
        code: 'KOSPI' | 'KOSDAQ'
        """
        df = self._index_cache.get(code)
        if df is None or df.empty:
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])

        cutoff = pd.to_datetime(self._cursor, format="%Y%m%d")
        df = df[df["date"] < cutoff].copy()
        return df.tail(days).reset_index(drop=True)

    def get_stock_list(self, market: str) -> List[str]:
        """
        cursor 날짜의 CSV에서 전 종목 코드 반환.
        market 필터는 CSV에 market 컬럼이 없으면 전체 반환.
        """
        df = self._load_day(self._cursor)
        if df.empty:
            return []
        if "market" in df.columns:
            df = df[df["market"] == market]
        return df["code"].astype(str).tolist()

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        종목 일봉 반환 — cursor 이전 days일.
        cursor 포함 날짜까지 필요한 CSV 파일들을 조합.
        """
        rows = self._collect_stock_rows(code, days)
        if not rows:
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.sort_values("date").dropna(subset=["date"])
        return df.tail(days).reset_index(drop=True)

    def get_stock_info(self, code: str) -> dict:
        """
        cursor 날짜 CSV에서 종목 기본정보 반환.
        (CSV에 market_cap 컬럼이 있는 경우 활용)
        """
        df = self._load_day(self._cursor)
        if df.empty or code not in df["code"].values:
            return {"name": code, "sector": "기타", "market_cap": 0, "listed_shares": 0}

        row = df[df["code"] == code].iloc[0]
        return {
            "name":          code,
            "sector":        "기타",
            "market_cap":    int(row.get("market_cap", 0)),
            "listed_shares": 0,
        }

    def get_foreign_institution_data(self, code: str, days: int) -> pd.DataFrame:
        """
        백테스트에서는 외인/기관 데이터 미지원 → 빈 DataFrame 반환.
        demand_score 에서 0점 처리됨.
        """
        return pd.DataFrame(columns=["date", "foreign_net", "institution_net"])

    def get_avg_daily_volume(self, code: str, days: int) -> float:
        """N일 평균 거래대금 반환."""
        rows = self._collect_stock_rows(code, days)
        if not rows:
            return 0.0
        amounts = []
        for r in rows:
            amt = r.get("amount", 0)
            if amt and amt > 0:
                amounts.append(float(amt))
            else:
                # amount 없으면 close * volume 추정
                amounts.append(float(r.get("close", 0)) * float(r.get("volume", 0)))
        return float(sum(amounts) / len(amounts)) if amounts else 0.0

    def get_current_price(self, code: str) -> float:
        """cursor 날짜의 종가를 현재가로 반환."""
        df = self._load_day(self._cursor)
        if df.empty or code not in df["code"].values:
            return 0.0
        row = df[df["code"] == code].iloc[0]
        return float(row.get("close", 0))

    # ── 내부 로더 ─────────────────────────────────────────────────────────────

    def _preload_index(self):
        """지수 CSV 전체 로드 (초기 1회)."""
        for name in ["KOSPI", "KOSDAQ"]:
            path = self._index_dir / f"{name}.csv"
            if not path.exists():
                print(f"[CsvProvider] 지수 파일 없음: {path}")
                self._index_cache[name] = pd.DataFrame()
                continue
            try:
                df = pd.read_csv(path, dtype=str)
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
                for col in ["open","high","low","close","volume"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                self._index_cache[name] = df
                print(f"[CsvProvider] 지수 로드: {name} {len(df)}일")
            except Exception as e:
                print(f"[CsvProvider] 지수 로드 실패 ({name}): {e}")
                self._index_cache[name] = pd.DataFrame()

    def _load_day(self, date_str: str) -> pd.DataFrame:
        """날짜별 CSV 로드 (캐시 활용). 없으면 빈 DataFrame."""
        if date_str in self._day_cache:
            return self._day_cache[date_str]

        path = self._data_dir / f"{date_str}.csv"
        if not path.exists():
            self._day_cache[date_str] = pd.DataFrame()
            return self._day_cache[date_str]

        try:
            df = pd.read_csv(path, dtype={"code": str})
            # 숫자 컬럼 변환
            for col in ["open","high","low","close","volume","amount","market_cap"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            self._day_cache[date_str] = df
        except Exception as e:
            print(f"[CsvProvider] CSV 로드 실패 ({date_str}): {e}")
            self._day_cache[date_str] = pd.DataFrame()

        # 캐시 크기 제한 (메모리 절약: 최근 90일치만 유지)
        if len(self._day_cache) > 90:
            oldest = sorted(self._day_cache.keys())[0]
            del self._day_cache[oldest]

        return self._day_cache[date_str]

    def _collect_stock_rows(self, code: str, days: int) -> list:
        """
        cursor 이전 days일치 CSV를 역순으로 읽어 해당 종목 행만 수집.
        look-ahead bias: cursor 날짜 포함 이전 데이터만 사용.
        """
        # cursor 이전 날짜 파일 목록 (역순)
        cursor_dt = datetime.strptime(self._cursor, "%Y%m%d")
        candidates = sorted(self._data_dir.glob("????????.csv"), reverse=True)

        rows = []
        for path in candidates:
            try:
                d = datetime.strptime(path.stem, "%Y%m%d")
            except ValueError:
                continue
            if d > cursor_dt:
                continue  # look-ahead 차단

            df = self._load_day(path.stem)
            if df.empty or "code" not in df.columns:
                continue

            match = df[df["code"].astype(str) == str(code)]
            if match.empty:
                continue

            row = match.iloc[0].to_dict()
            row["date"] = path.stem
            rows.append(row)

            if len(rows) >= days:
                break

        return list(reversed(rows))  # 오름차순(과거→최근)
