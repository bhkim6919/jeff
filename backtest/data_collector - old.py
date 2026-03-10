"""
data_collector.py
=================
pykrx를 이용해 백테스트용 과거 데이터를 수집하고 CSV로 저장.

저장 구조:
  backtest/data/YYYYMMDD.csv      ← 날짜별 전 유니버스 종목 OHLCV
  backtest/data/index/KOSPI.csv   ← KOSPI 지수 일봉 전체
  backtest/data/index/KOSDAQ.csv  ← KOSDAQ 지수 일봉 전체

날짜별 CSV 컬럼:
  code, open, high, low, close, volume, amount(거래대금), market_cap

사용:
  python backtest/data_collector.py
  python backtest/data_collector.py --start 20220101 --end 20251231 --top 300
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import pandas as pd
from pykrx import stock as krx


# ── 경로 설정 ────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent
DATA_DIR  = BASE_DIR / "data"
INDEX_DIR = DATA_DIR / "index"
DATA_DIR.mkdir(parents=True, exist_ok=True)
INDEX_DIR.mkdir(parents=True, exist_ok=True)

API_DELAY     = 0.35
INDEX_TICKER  = {"KOSPI": "1001", "KOSDAQ": "2001"}
DEFAULT_START = "20220101"
DEFAULT_END   = "20251231"
DEFAULT_TOP   = 300


def _sleep():
    time.sleep(API_DELAY)


def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")


def _format_ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


# ── 영업일 리스트 생성 ───────────────────────────────────────────────────────
def get_bizdays(start: str, end: str) -> List[str]:
    """
    pykrx 지수 일봉(KOSPI)을 이용해 영업일 리스트 생성.
    name_display=False 로 '지수명' 컬럼 의존 제거.
    """
    print(f"[BizDays] KOSPI 기준 영업일 계산 ({start}~{end})")
    df = krx.get_index_ohlcv_by_date(start, end, INDEX_TICKER["KOSPI"],
                                     freq="d", name_display=False)
    _sleep()
    if df is None or df.empty:
        raise RuntimeError("영업일 계산 실패 — KOSPI 지수 데이터 없음")

    df = df.reset_index()
    date_col = df.columns[0]  # 보통 '날짜'
    bdays = pd.to_datetime(df[date_col]).dt.strftime("%Y%m%d").tolist()
    print(f"[BizDays] 총 {len(bdays)}개")
    return bdays


# ── 유니버스 구성 (간소화 버전) ─────────────────────────────────────────────
def build_universe(ref_date: str, top_n: int) -> List[str]:
    """
    ref_date 기준 KOSPI + KOSDAQ 종목코드 리스트에서 앞에서 top_n개만 사용.
    (get_market_cap_by_ticker 의 '시가총액' 컬럼 의존을 제거하기 위한 우회 버전)

    ※ 실제 시가총액 상위 N개는 아님에 주의.
    """
    print(f"[Universe] {ref_date} 기준 종목코드 기반 상위 {top_n}개 구성 중...")
    codes: List[str] = []

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            lst = krx.get_stock_ticker_list(ref_date, market=market)
            _sleep()
            if not lst:
                print(f"[WARN] {market} 종목 리스트 비어 있음")
                continue
            codes.extend([str(c) for c in lst])
            print(f"[Universe] {market} 종목 {len(lst)}개 수집")
        except Exception as e:
            print(f"[WARN] {market} 종목 리스트 조회 실패: {e}")

    codes = sorted(set(codes))
    if not codes:
        raise RuntimeError("유니버스 구성 실패 — 종목코드 조회 불가")

    universe = codes[:top_n]
    print(f"[Universe] 전체 종목 {len(codes)}개 중 {len(universe)}개 사용")
    return universe


# ── 지수 수집 ────────────────────────────────────────────────────────────────
def collect_index(start: str, end: str):
    """
    KOSPI / KOSDAQ 지수 일봉 수집 → INDEX_DIR/KOSPI.csv, KOSDAQ.csv 저장
    name_display=False 로 '지수명' 컬럼 의존 제거.
    """
    for name, ticker in INDEX_TICKER.items():
        print(f"[Index] {name} 수집 중... ({start} ~ {end})")
        save_path = INDEX_DIR / f"{name}.csv"
        try:
            df = krx.get_index_ohlcv_by_date(start, end, ticker,
                                             freq="d", name_display=False)
            _sleep()
            if df is None or df.empty:
                print(f"[WARN] {name} 지수 데이터 없음")
                continue

            df = df.reset_index()
            date_col = df.columns[0]

            # 컬럼명 매핑 (한글/영문 모두 대응)
            rename_map = {}
            for src, dst in [
                ("시가", "open"), ("고가", "high"), ("저가", "low"),
                ("종가", "close"), ("거래량", "volume"),
                ("Open", "open"), ("High", "high"), ("Low", "low"),
                ("Close", "close"), ("Volume", "volume"),
            ]:
                if src in df.columns:
                    rename_map[src] = dst

            df = df.rename(columns=rename_map)
            df = df.rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")

            keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
            df[keep].to_csv(save_path, index=False, encoding="utf-8-sig")
            print(f"[Index] {name} 저장 완료 → {save_path} ({len(df)}일)")
        except Exception as e:
            print(f"[ERROR] {name} 지수 수집 실패: {e}")


# ── 종목 일봉 수집 (날짜별 CSV) ───────────────────────────────────────────────
def collect_stocks(universe: List[str], bdays: List[str], start: str, end: str):
    """
    유니버스 전 종목의 일봉을 수집해 날짜별 CSV로 저장.
    - 종목별로 전체 기간 일봉을 한 번에 받아와서
    - 날짜별로 쪼개어 저장
    """
    print(f"\n[Stock] {len(universe)}개 종목 × {len(bdays)}일 수집 시작")
    start_dt = _parse_ymd(start)
    end_dt   = _parse_ymd(end)

    # 날짜 버퍼: { "YYYYMMDD": [ {row}, ... ] }
    day_buffer: Dict[str, List[dict]] = {d: [] for d in bdays}

    for i, code in enumerate(universe, 1):
        print(f"[{i:4d}/{len(universe):4d}] {code} 수집 중...", end="")
        try:
            df = krx.get_market_ohlcv_by_date(start, end, code)
            _sleep()
            if df is None or df.empty:
                print(" → 데이터 없음")
                continue

            df = df.reset_index()
            date_col = df.columns[0]

            # 컬럼명 매핑
            rename_map = {}
            for src, dst in [
                ("시가", "open"), ("고가", "high"), ("저가", "low"),
                ("종가", "close"), ("거래량", "volume"), ("거래대금", "amount"),
                ("Open", "open"), ("High", "high"), ("Low", "low"),
                ("Close", "close"), ("Volume", "volume"), ("Amount", "amount"),
            ]:
                if src in df.columns:
                    rename_map[src] = dst

            df = df.rename(columns=rename_map)
            df = df.rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"])

            # 시가총액 컬럼 (있으면 market_cap으로 저장)
            if "시가총액" in df.columns:
                df = df.rename(columns={"시가총액": "market_cap"})
            elif "Marcap" in df.columns:
                df = df.rename(columns={"Marcap": "market_cap"})
            else:
                df["market_cap"] = 0

            row_count = 0
            for _, row in df.iterrows():
                d = row["date"]
                if d < start_dt or d > end_dt:
                    continue
                d_str = d.strftime("%Y%m%d")
                if d_str not in day_buffer:
                    continue

                day_buffer[d_str].append({
                    "code":       code,
                    "open":       float(row.get("open", 0) or 0),
                    "high":       float(row.get("high", 0) or 0),
                    "low":        float(row.get("low", 0) or 0),
                    "close":      float(row.get("close", 0) or 0),
                    "volume":     int(row.get("volume", 0) or 0),
                    "amount":     float(row.get("amount", 0) or 0),
                    "market_cap": float(row.get("market_cap", 0) or 0),
                })
                row_count += 1

            print(f" → {row_count}일치 버퍼 적재")
        except Exception as e:
            print(f" → 실패: {e}")
            continue

    # 날짜별 CSV 저장
    print(f"\n[Stock] 날짜별 CSV 저장 중...")
    saved = 0
    for date_str, rows in day_buffer.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        save_path = DATA_DIR / f"{date_str}.csv"
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        saved += 1
    print(f"[Stock] 날짜별 CSV 저장 완료 → {saved}개 파일")


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Q-TRON 백테스트 데이터 수집기")
    parser.add_argument("--start", default=DEFAULT_START, help="시작일 (YYYYMMDD)")
    parser.add_argument("--end",   default=DEFAULT_END,   help="종료일 (YYYYMMDD)")
    parser.add_argument("--top",   type=int, default=DEFAULT_TOP,
                        help="유니버스 종목 수 (기본 300개)")
    args = parser.parse_args()

    print("=" * 60)
    print("Q-TRON 백테스트 데이터 수집기")
    print(f"  기간: {args.start} ~ {args.end}")
    print(f"  유니버스: 종목코드 기준 상위 {args.top}개")
    print(f"  저장 경로: {DATA_DIR}")
    print("=" * 60)

    # 1) 지수 수집
    collect_index(args.start, args.end)

    # 2) 유니버스 구성 (중간 날짜 기준)
    start_dt = _parse_ymd(args.start)
    end_dt   = _parse_ymd(args.end)
    mid_dt   = start_dt + (end_dt - start_dt) // 2
    mid_date = _format_ymd(mid_dt)
    universe = build_universe(mid_date, args.top)

    # 3) 영업일 리스트
    bdays = get_bizdays(args.start, args.end)

    # 4) 종목 일봉 수집
    collect_stocks(universe, bdays, args.start, args.end)

    print("\n" + "=" * 60)
    print("[완료] 데이터 수집 종료")
    print(f"  날짜별 CSV: {DATA_DIR}/*.csv")
    print(f"  지수 CSV:   {INDEX_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()