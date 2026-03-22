# -*- coding: utf-8 -*-
"""
HistoricalProvider + BacktestRegimeDetector
============================================
CSV 데이터를 날짜별로 슬라이싱하여 백테스트에 제공.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


class HistoricalProvider:
    """
    ohlcv_kospi_daily/ CSV 파일들을 메모리에 로드하여
    특정 날짜 기준 데이터를 제공.
    """

    def __init__(self, ohlcv_dir: str, index_file: str,
                 universe_file: str, sector_map_path: str,
                 max_tickers: int = 0):
        self.ohlcv_dir = Path(ohlcv_dir)
        self.index_file = Path(index_file)

        # 지수 로드
        self.index_df = pd.read_csv(index_file, parse_dates=["date"])
        self.index_df = self.index_df.sort_values("date").reset_index(drop=True)

        # 유니버스 로드
        self.tickers = pd.read_csv(universe_file)["ticker"].astype(str).tolist()
        if max_tickers > 0:
            self.tickers = self.tickers[:max_tickers]

        # 섹터맵
        self.sector_map: Dict[str, str] = {}
        try:
            with open(sector_map_path, encoding="utf-8") as f:
                self.sector_map = json.load(f)
        except Exception:
            pass

        # OHLCV 캐시 (ticker → DataFrame)
        self._cache: Dict[str, pd.DataFrame] = {}

        print(f"[HistoricalProvider] index: {len(self.index_df)} rows, "
              f"universe: {len(self.tickers)} tickers")

    def load_all(self, min_rows: int = 130) -> None:
        """전 종목 OHLCV 선로드."""
        loaded = 0
        for ticker in self.tickers:
            df = self._load_ticker(ticker, min_rows)
            if df is not None:
                loaded += 1
        print(f"[HistoricalProvider] {loaded}/{len(self.tickers)} tickers loaded")

    def _load_ticker(self, ticker: str, min_rows: int = 30) -> Optional[pd.DataFrame]:
        if ticker in self._cache:
            return self._cache[ticker]
        path = self.ohlcv_dir / f"{ticker}.csv"
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path, parse_dates=["date"])
            df = df.sort_values("date").reset_index(drop=True)
            for c in ["open", "high", "low", "close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            if len(df) < min_rows:
                return None
            self._cache[ticker] = df
            return df
        except Exception:
            return None

    def get_universe_at(self, eval_date: pd.Timestamp) -> Dict[str, pd.DataFrame]:
        """eval_date 이전 데이터만 포함된 종목별 DataFrame dict."""
        result = {}
        for ticker in self.tickers:
            df = self._load_ticker(ticker)
            if df is None:
                continue
            cut = df[df["date"] <= eval_date]
            if len(cut) >= 30:
                result[ticker] = cut
        return result

    def get_index_at(self, eval_date: pd.Timestamp) -> pd.DataFrame:
        return self.index_df[self.index_df["date"] <= eval_date]

    def get_bar(self, ticker: str, bar_date: pd.Timestamp) -> Optional[dict]:
        """특정 종목의 특정 날짜 bar (OHLCV dict)."""
        df = self._cache.get(ticker)
        if df is None:
            df = self._load_ticker(ticker)
        if df is None:
            return None
        match = df[df["date"] == bar_date]
        if match.empty:
            return None
        row = match.iloc[0]
        return {
            "date": bar_date, "open": float(row["open"]),
            "high": float(row["high"]), "low": float(row["low"]),
            "close": float(row["close"]), "volume": float(row["volume"]),
        }

    def get_trade_dates(self, start: str, end: str) -> List[pd.Timestamp]:
        """지수 기준 거래일 리스트."""
        mask = (self.index_df["date"] >= pd.Timestamp(start)) & \
               (self.index_df["date"] <= pd.Timestamp(end))
        return self.index_df.loc[mask, "date"].tolist()


class BacktestRegimeDetector:
    """
    백테스트용 레짐 판정.
    MA200 + Breadth → BULL / SIDEWAYS / BEAR 3단계.

    mode:
      "original"  — Case 0: 기존 (BEAR = MA200 below OR breadth low)
      "symmetric" — Case A: 대칭화 (BEAR = MA200 below AND breadth low)
      "relaxed"   — Case B: 임계값 완화 (기존 구조, breadth_bull=0.45, breadth_bear=0.30)
      "ma_first"  — Case C1: MA200 우선 (MA200 위→BULL/SIDEWAYS, 아래→BEAR/SIDEWAYS)
      "ma_only"   — Case C2: 추세 우선 단순형 (MA200만으로 BULL/BEAR)
    """

    def __init__(self, ma_period: int = 200,
                 breadth_bull: float = 0.55, breadth_bear: float = 0.35,
                 mode: str = "original"):
        self.ma_period = ma_period
        self.breadth_bull = breadth_bull
        self.breadth_bear = breadth_bear
        self.mode = mode

    def detect(self, index_df: pd.DataFrame,
               universe: Dict[str, pd.DataFrame]) -> str:
        if len(index_df) < self.ma_period:
            return "SIDEWAYS"

        close = index_df["close"].astype(float)
        ma200 = float(close.rolling(self.ma_period).mean().iloc[-1])
        last = float(close.iloc[-1])
        above_ma = last > ma200

        # C2: MA200만 사용, breadth 계산 불필요
        if self.mode == "ma_only":
            return "BULL" if above_ma else "BEAR"

        # Breadth: MA20 상회 비율
        above_count = 0
        total_count = 0
        for ticker, df in universe.items():
            if len(df) < 20:
                continue
            c = df["close"].astype(float)
            ma20 = float(c.rolling(20).mean().iloc[-1])
            total_count += 1
            if float(c.iloc[-1]) > ma20:
                above_count += 1

        breadth = above_count / total_count if total_count > 0 else 0.5

        if self.mode == "original":
            # Case 0: 기존 — BEAR = below OR low breadth (비대칭)
            if above_ma and breadth >= self.breadth_bull:
                return "BULL"
            elif (not above_ma) or breadth <= self.breadth_bear:
                return "BEAR"
            else:
                return "SIDEWAYS"

        elif self.mode == "symmetric":
            # Case A: 대칭화 — BEAR = below AND low breadth
            if above_ma and breadth >= self.breadth_bull:
                return "BULL"
            elif (not above_ma) and breadth <= self.breadth_bear:
                return "BEAR"
            else:
                return "SIDEWAYS"

        elif self.mode == "relaxed":
            # Case B: 임계값 완화 — 기존 구조 유지, 문턱만 조정
            if above_ma and breadth >= self.breadth_bull:
                return "BULL"
            elif (not above_ma) or breadth <= self.breadth_bear:
                return "BEAR"
            else:
                return "SIDEWAYS"

        elif self.mode == "ma_first":
            # Case C1: MA200 우선 — MA200이 1차 분류, breadth가 강도 조절
            if above_ma:
                return "BULL" if breadth >= self.breadth_bull else "SIDEWAYS"
            else:
                return "BEAR" if breadth <= self.breadth_bear else "SIDEWAYS"

        # fallback
        return "SIDEWAYS"
