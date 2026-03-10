"""
EarlySignalDetector
===================
섹터별 Early Entry 신호를 감지하는 모듈 (P1 구현).

동작 원리:
  테마 급등 전 T-4 ~ T-6일에 나타나는 가격/거래량 선행 패턴을 포착하여
  기존 Gen2 신호보다 평균 6.1일 먼저 진입.

신호 발생 조건 (3개 중 2개 이상 충족 → Early Entry 발동):
  ① Breadth Jump  : 섹터 내 MA20 상회 종목 비율이 전일 대비 ≥ 20%p 급등
  ② Volume Surge  : 섹터 평균 거래량이 20일 평균 대비 ≥ 1.3배
  ③ New High Ratio: 섹터 내 252일 신고가 달성 종목 비율 ≥ 7%

확정 파라미터 (2019~2022 학습 / 2023~2026 OOS 검증):
  BREADTH_JUMP   = 0.20
  VOL_RATIO_MIN  = 1.30
  NEW_HIGH_MIN   = 0.07
  MIN_STOCKS     = 3
  COND_THRESHOLD = 2   (3개 중 2개 이상)

실행 시점: 장 마감 후 (15:30 이후), Kiwoom API 일봉 데이터 확정 후
출력: 당일 Early Entry 활성 섹터 목록 (JSON 파일 + dict 반환)
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# ── 확정 파라미터 ─────────────────────────────────────────────────────────────
BREADTH_JUMP   = 0.20   # Breadth Jump 임계값 (20%p)
VOL_RATIO_MIN  = 1.30   # Volume Surge 임계값 (130%)
NEW_HIGH_MIN   = 0.07   # New High Ratio 임계값 (7%)
MIN_STOCKS     = 3      # 섹터 신호 계산 최소 종목 수
COND_THRESHOLD = 2      # 발동 조건 개수 (3 중 2)
LOOKBACK_VOL   = 20     # 거래량 이동평균 기간 (일)
LOOKBACK_HIGH  = 252    # 신고가 기준 기간 (거래일)
MA_PERIOD      = 20     # MA20 기간


@dataclass
class SectorSignal:
    """섹터 단위 Early Entry 신호 결과."""
    sector:        str
    signal:        bool           # 최종 발동 여부
    cond_count:    int            # 충족 조건 수
    breadth_jump:  float          # 전일 대비 breadth 변화량
    vol_ratio:     float          # 섹터 평균 거래량 배율
    new_high_ratio: float         # 신고가 달성 비율
    stock_count:   int            # 섹터 내 종목 수
    active_codes:  List[str] = field(default_factory=list)  # 신호 충족 종목들

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


@dataclass
class EarlySignalResult:
    """전체 Early Entry 실행 결과."""
    date:             str
    active_sectors:   List[str]
    sector_signals:   List[SectorSignal]
    total_sectors:    int
    execution_time:   str

    def to_dict(self) -> dict:
        return {
            "date":           self.date,
            "active_sectors": self.active_sectors,
            "sector_signals": [s.to_dict() for s in self.sector_signals],
            "total_sectors":  self.total_sectors,
            "execution_time": self.execution_time,
        }


class EarlySignalDetector:
    """
    Early Entry 신호 감지기.

    사용법:
        detector = EarlySignalDetector(provider, sector_map, config)
        result   = detector.detect()
        # result.active_sectors → 오늘 Early 신호 발생 섹터 목록
    """

    def __init__(
        self,
        provider,
        sector_map: Dict[str, str],     # {ticker: sector_name}
        output_dir: str = "data/early_signals",
        db_path:    str = "data/early_signals.db",
        params:     Optional[dict] = None,
    ):
        self.provider   = provider
        self.sector_map = sector_map     # ticker → sector

        # 섹터 → ticker 역매핑
        self._sector_tickers: Dict[str, List[str]] = {}
        for ticker, sector in sector_map.items():
            self._sector_tickers.setdefault(sector, []).append(ticker)

        # 출력 설정
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

        # 파라미터 (기본값 = 확정 파라미터)
        p = params or {}
        self.breadth_jump    = p.get("BREADTH_JUMP",   BREADTH_JUMP)
        self.vol_ratio_min   = p.get("VOL_RATIO_MIN",  VOL_RATIO_MIN)
        self.new_high_min    = p.get("NEW_HIGH_MIN",   NEW_HIGH_MIN)
        self.min_stocks      = p.get("MIN_STOCKS",     MIN_STOCKS)
        self.cond_threshold  = p.get("COND_THRESHOLD", COND_THRESHOLD)

        # SQLite 초기화
        self._init_db()

    # ── Public API ────────────────────────────────────────────────────────────

    def detect(self, target_date: Optional[date] = None) -> EarlySignalResult:
        """
        전 섹터 Early Entry 신호 감지.

        Args:
            target_date: 분석 기준일 (None이면 오늘)

        Returns:
            EarlySignalResult: 활성 섹터 목록 + 섹터별 상세 신호
        """
        today     = target_date or date.today()
        today_str = today.strftime("%Y-%m-%d")
        start_ts  = datetime.now()

        print(f"\n[EarlySignal] {today_str} Early Entry 신호 감지 시작")
        print(f"[EarlySignal] 총 {len(self._sector_tickers)}개 섹터 분석")

        sector_signals: List[SectorSignal] = []

        for sector, tickers in self._sector_tickers.items():
            if len(tickers) < self.min_stocks:
                continue
            sig = self._analyze_sector(sector, tickers)
            sector_signals.append(sig)

        active   = [s.sector for s in sector_signals if s.signal]
        exec_sec = (datetime.now() - start_ts).total_seconds()

        result = EarlySignalResult(
            date=today_str,
            active_sectors=active,
            sector_signals=sector_signals,
            total_sectors=len(sector_signals),
            execution_time=f"{exec_sec:.2f}s",
        )

        # 결과 저장
        self._save_json(result, today_str)
        self._save_db(result, today_str)

        print(f"[EarlySignal] 완료: {len(active)}/{len(sector_signals)}개 섹터 활성 ({exec_sec:.2f}s)")
        if active:
            print(f"[EarlySignal] 활성 섹터: {active}")

        return result

    def get_active_sectors(self, target_date: Optional[date] = None) -> List[str]:
        """활성 섹터 목록만 빠르게 반환 (파이프라인 연동용)."""
        result = self.detect(target_date)
        return result.active_sectors

    def load_latest(self) -> Optional[dict]:
        """가장 최근 저장된 Early 신호 JSON 로드."""
        files = sorted(self.output_dir.glob("early_signal_*.json"), reverse=True)
        if not files:
            return None
        with open(files[0], "r", encoding="utf-8") as f:
            return json.load(f)

    # ── 섹터 분석 ─────────────────────────────────────────────────────────────

    def _analyze_sector(self, sector: str, tickers: List[str]) -> SectorSignal:
        """섹터 내 종목 데이터 수집 후 3개 조건 평가."""
        # 각 종목 지표 수집
        rows = []
        for ticker in tickers:
            row = self._get_ticker_indicators(ticker)
            if row is not None:
                rows.append(row)

        n = len(rows)
        if n < self.min_stocks:
            return SectorSignal(
                sector=sector, signal=False, cond_count=0,
                breadth_jump=0.0, vol_ratio=0.0, new_high_ratio=0.0,
                stock_count=n,
            )

        df = pd.DataFrame(rows)

        # ① Breadth Jump
        breadth_today = df["above_ma20"].mean()
        breadth_prev  = df["above_ma20_prev"].mean()
        breadth_jump  = breadth_today - breadth_prev
        cond1 = breadth_jump >= self.breadth_jump

        # ② Volume Surge
        vol_ratio = df["vol_ratio"].mean()
        cond2 = vol_ratio >= self.vol_ratio_min

        # ③ New High Ratio
        new_high_ratio = df["new_high"].mean()
        cond3 = new_high_ratio >= self.new_high_min

        cond_count = int(cond1) + int(cond2) + int(cond3)
        signal     = cond_count >= self.cond_threshold

        # 신호 충족 종목
        active_codes = []
        if signal:
            active_codes = [
                r["ticker"] for r in rows
                if r["above_ma20"] and r["new_high"]
            ]

        return SectorSignal(
            sector=sector,
            signal=signal,
            cond_count=cond_count,
            breadth_jump=round(breadth_jump, 4),
            vol_ratio=round(vol_ratio, 4),
            new_high_ratio=round(new_high_ratio, 4),
            stock_count=n,
            active_codes=active_codes,
        )

    def _get_ticker_indicators(self, ticker: str) -> Optional[dict]:
        """
        종목별 지표 계산 (incremental-ready 구조).
        - above_ma20     : 오늘 종가 > MA20 여부
        - above_ma20_prev: 전일 종가 > MA20 여부  (Breadth Jump용)
        - vol_ratio      : 오늘 거래량 / 20일 평균
        - new_high       : 오늘 종가 ≥ 252일 최고가 여부
        """
        # 종가 기준이므로 일봉 확정 데이터 사용 (장 마감 후 호출 가정)
        days_needed = max(LOOKBACK_HIGH, LOOKBACK_VOL, MA_PERIOD) + 5
        df = self.provider.get_stock_ohlcv(ticker, days=days_needed)

        if df is None or len(df) < MA_PERIOD + 2:
            return None

        close  = df["close"]
        volume = df["volume"]

        # MA20 (incremental: 마지막 MA20만 필요)
        ma20_series = close.rolling(MA_PERIOD).mean()
        if ma20_series.isna().iloc[-1]:
            return None

        ma20_today = ma20_series.iloc[-1]
        ma20_prev  = ma20_series.iloc[-2]

        above_ma20      = float(close.iloc[-1]) > float(ma20_today)
        above_ma20_prev = float(close.iloc[-2]) > float(ma20_prev)

        # Volume Surge (오늘 거래량 / 20일 평균)
        vol_ma20 = volume.iloc[-LOOKBACK_VOL-1:-1].mean()  # 전일까지 20일 평균
        vol_today = float(volume.iloc[-1])
        vol_ratio = vol_today / vol_ma20 if vol_ma20 > 0 else 0.0

        # New High (252일 종가 기준)
        lookback = min(LOOKBACK_HIGH, len(df) - 1)
        high_252 = float(close.iloc[-lookback-1:-1].max())
        new_high = float(close.iloc[-1]) >= high_252

        return {
            "ticker":         ticker,
            "above_ma20":     above_ma20,
            "above_ma20_prev": above_ma20_prev,
            "vol_ratio":      vol_ratio,
            "new_high":       new_high,
        }

    # ── 저장 ──────────────────────────────────────────────────────────────────

    def _save_json(self, result: EarlySignalResult, date_str: str) -> None:
        """결과를 JSON 파일로 저장 (익일 파이프라인에서 로드용)."""
        path = self.output_dir / f"early_signal_{date_str}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)
        print(f"[EarlySignal] JSON 저장: {path}")

    def _init_db(self) -> None:
        """SQLite DB 초기화 (incremental 업데이트용)."""
        os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else ".", exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS early_signal_history (
                    date            TEXT NOT NULL,
                    sector          TEXT NOT NULL,
                    signal          INTEGER NOT NULL,
                    cond_count      INTEGER,
                    breadth_jump    REAL,
                    vol_ratio       REAL,
                    new_high_ratio  REAL,
                    stock_count     INTEGER,
                    active_codes    TEXT,
                    PRIMARY KEY (date, sector)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_indicators (
                    ticker      TEXT NOT NULL,
                    date        TEXT NOT NULL,
                    ma20        REAL,
                    vol_ma20    REAL,
                    high_252    REAL,
                    above_ma20  INTEGER,
                    vol_ratio   REAL,
                    PRIMARY KEY (ticker, date)
                )
            """)
            conn.commit()

    def _save_db(self, result: EarlySignalResult, date_str: str) -> None:
        """SQLite에 섹터 신호 이력 저장."""
        with sqlite3.connect(self.db_path) as conn:
            for sig in result.sector_signals:
                conn.execute("""
                    INSERT OR REPLACE INTO early_signal_history
                    (date, sector, signal, cond_count, breadth_jump,
                     vol_ratio, new_high_ratio, stock_count, active_codes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_str,
                    sig.sector,
                    int(sig.signal),
                    sig.cond_count,
                    sig.breadth_jump,
                    sig.vol_ratio,
                    sig.new_high_ratio,
                    sig.stock_count,
                    json.dumps(sig.active_codes, ensure_ascii=False),
                ))
            conn.commit()


# ── 독립 실행 (스케줄러 연동용) ───────────────────────────────────────────────

def run_early_signal_detection(
    signal_dir:  str = "data/signals_gen2",
    sector_map_path: str = "data/sector_map.json",
    output_dir:  str = "data/early_signals",
    db_path:     str = "data/early_signals.db",
    provider=None,
) -> dict:
    """
    장 마감 후 독립 실행용 함수.
    스케줄러(APScheduler 등)에서 15:30 이후 호출.

    Returns:
        결과 dict (active_sectors 포함)
    """
    # sector_map 로드
    with open(sector_map_path, "r", encoding="utf-8") as f:
        sector_map = json.load(f)

    # provider가 없으면 mock 모드
    if provider is None:
        from core.data_provider import DataProvider
        provider = DataProvider()  # mock provider

    detector = EarlySignalDetector(
        provider=provider,
        sector_map=sector_map,
        output_dir=output_dir,
        db_path=db_path,
    )

    result = detector.detect()
    return result.to_dict()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Q-TRON Early Entry 신호 감지")
    parser.add_argument("--sector-map", default="data/sector_map.json")
    parser.add_argument("--output-dir", default="data/early_signals")
    parser.add_argument("--db-path",    default="data/early_signals.db")
    parser.add_argument("--date",       default=None, help="분석 기준일 (YYYY-MM-DD)")
    args = parser.parse_args()

    result = run_early_signal_detection(
        sector_map_path=args.sector_map,
        output_dir=args.output_dir,
        db_path=args.db_path,
    )

    print("\n── Early Entry 결과 ──────────────────────────")
    print(f"날짜       : {result['date']}")
    print(f"활성 섹터  : {result['active_sectors']}")
    print(f"총 분석    : {result['total_sectors']}개 섹터")
    print(f"실행 시간  : {result['execution_time']}")
