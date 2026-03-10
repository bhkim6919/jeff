"""
update_data_incremental.py
==========================
매일 장마감(15:30) 이후 실행 — KOSPI 데이터를 증분 업데이트.

Step 1: KOSPI 지수 5년치   → data/kospi_index_daily_5y.csv
Step 2: KOSPI 종목 목록    → data/universe_kospi.csv
Step 3: 종목별 OHLCV 증분  → data/ohlcv_kospi_daily/{ticker}.csv

Usage:
  python update_data_incremental.py           # 전체 업데이트
  python update_data_incremental.py --index   # 지수만
  python update_data_incremental.py --fast    # 인덱스 + 유니버스만 (OHLCV 스킵)
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

try:
    from pykrx import stock as krx
except ImportError:
    print("ERROR: pykrx 미설치. pip install pykrx")
    sys.exit(1)

OHLCV_DIR    = BASE_DIR / "data" / "ohlcv_kospi_daily"
INDEX_FILE   = BASE_DIR / "data" / "kospi_index_daily_5y.csv"
UNIVERSE_FILE= BASE_DIR / "data" / "universe_kospi.csv"
SECTOR_MAP   = BASE_DIR / "data" / "sector_map.json"

API_DELAY    = 0.35   # pykrx 과부하 방지
_PREF_PAT    = re.compile(r"\d{5}[5-9]$")

# pykrx 지수/종목리스트 엔드포인트가 장마감 후 접근 불가 시 사용하는
# KODEX 200 ETF (069500) — KOSPI 200 추종, MA200 레짐 산출에 사용
_KOSPI_PROXY = "069500"


# ── 날짜 헬퍼 ─────────────────────────────────────────────────────────────────

def _bday_str(offset_days: int = 0) -> str:
    """오늘부터 offset_days 이전의 가장 가까운 영업일 (YYYYMMDD)."""
    d = datetime.today() - timedelta(days=offset_days)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _progress(i: int, total: int, label: str = "") -> None:
    filled = int(40 * i / total) if total else 0
    bar    = "#" * filled + "-" * (40 - filled)
    pct    = i / total * 100 if total else 0
    print(f"\r  [{bar}] {pct:5.1f}%  {i}/{total}  {label:<12}", end="", flush=True)


# ── Step 1: KOSPI 지수 ────────────────────────────────────────────────────────

def update_index() -> bool:
    """
    KOSPI 지수 5년치 → data/kospi_index_daily_5y.csv.
    우선: pykrx get_index_ohlcv_by_date (장마감 후 엔드포인트 불가 시 있음)
    폴백: KODEX 200 ETF (069500) — KOSPI 200 추종 ETF 로 대체
    """
    print("[Step1] KOSPI 지수 업데이트...")
    fromdate = (datetime.today() - timedelta(days=365 * 5 + 30)).strftime("%Y%m%d")
    todate   = _bday_str()
    df       = None

    # 1차 시도: 원본 KOSPI 지수 (1001)
    try:
        df = krx.get_index_ohlcv_by_date(fromdate, todate, "1001", name_display=False)
        time.sleep(API_DELAY)
        if df is not None and not df.empty:
            print("  KOSPI 1001 성공")
        else:
            df = None
    except Exception as e:
        print(f"  KOSPI 1001 실패: {e}")

    # 2차 시도: KODEX 200 ETF 프록시 (069500)
    if df is None or df.empty:
        print(f"  → KODEX 200 ETF({_KOSPI_PROXY}) 프록시 사용...")
        try:
            df = krx.get_market_ohlcv_by_date(fromdate, todate, _KOSPI_PROXY)
            time.sleep(API_DELAY)
        except Exception as e:
            print(f"  KODEX 200 실패: {e}")
            return False

    if df is None or df.empty:
        print("  지수 데이터 없음")
        return False

    df = df.reset_index()
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "date"})
    # pykrx 컬럼명은 한글 또는 인코딩 깨짐 → 위치 기반 매핑 (date/o/h/l/c/v 순)
    cols = list(df.columns)
    col_map = {}
    for tgt, src in [("open", "시가"), ("high", "고가"), ("low", "저가"),
                     ("close", "종가"), ("volume", "거래량")]:
        if src in cols:
            col_map[src] = tgt
    if not col_map and len(cols) >= 6:
        # 인코딩 깨진 경우: 위치 기반 (1~5번째 컬럼)
        for i, tgt in enumerate(["open", "high", "low", "close", "volume"], 1):
            if i < len(cols):
                col_map[cols[i]] = tgt
    df = df.rename(columns=col_map)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    keep = ["date"] + [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df   = df[keep].copy()
    df   = df.sort_values("date").reset_index(drop=True)

    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(INDEX_FILE, index=False, encoding="utf-8")
    print(f"  완료: {INDEX_FILE.name}  ({len(df)}일)")
    return True


# ── Step 2: 유니버스 종목 목록 ────────────────────────────────────────────────

def _load_tickers_from_sector_map() -> list:
    """sector_map.json 에서 KOSPI/KOSDAQ 전 종목 추출 (폴백용)."""
    if not SECTOR_MAP.exists():
        return []
    import json
    try:
        with open(SECTOR_MAP, encoding="utf-8") as f:
            sm = json.load(f)
        tickers = [k for k in sm if k.isdigit() and len(k) == 6 and not _PREF_PAT.match(k)]
        return tickers
    except Exception:
        return []


def update_universe() -> list:
    """
    KOSPI 종목 목록 → data/universe_kospi.csv.
    우선: pykrx get_market_ticker_list
    폴백: data/sector_map.json (장마감 후 KRX 엔드포인트 불가 시)
    """
    print("[Step2] KOSPI 유니버스 업데이트...")
    bday    = _bday_str()
    tickers = []

    # 1차: pykrx
    try:
        tickers = list(krx.get_market_ticker_list(bday, market="KOSPI"))
        time.sleep(API_DELAY)
        tickers = [t for t in tickers if not _PREF_PAT.match(t) and t.isdigit() and len(t) == 6]
    except Exception as e:
        print(f"  pykrx 실패: {e}")

    # 2차: sector_map.json 폴백
    if not tickers:
        print("  → sector_map.json 폴백 사용...")
        tickers = _load_tickers_from_sector_map()

    if not tickers:
        print("  ERROR: 종목 목록 취득 실패")
        return []

    UNIVERSE_FILE.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ticker": tickers}).to_csv(UNIVERSE_FILE, index=False, encoding="utf-8")
    print(f"  완료: {UNIVERSE_FILE.name}  ({len(tickers)}개 종목)")
    return tickers


# ── Step 3: OHLCV 증분 업데이트 ──────────────────────────────────────────────

def update_ohlcv(tickers: list) -> None:
    print(f"[Step3] OHLCV 증분 업데이트 ({len(tickers)}개)...")
    OHLCV_DIR.mkdir(parents=True, exist_ok=True)

    todate  = _bday_str()
    ok_cnt  = 0
    err_cnt = 0
    skip_cnt= 0

    # 3년치 초기 구간
    init_from = (datetime.today() - timedelta(days=365 * 3 + 30)).strftime("%Y%m%d")

    for i, ticker in enumerate(tickers, 1):
        _progress(i, len(tickers), ticker)

        path = OHLCV_DIR / f"{ticker}.csv"

        # 기존 파일에서 마지막 날짜 확인 (증분)
        existing = None
        fromdate = init_from
        if path.exists():
            try:
                existing  = pd.read_csv(path, dtype={"date": str})
                last_date = existing["date"].max()
                last_dt   = datetime.strptime(last_date, "%Y-%m-%d")
                fromdate  = (last_dt + timedelta(days=1)).strftime("%Y%m%d")
                if fromdate > todate:
                    skip_cnt += 1
                    continue   # 이미 최신
            except Exception:
                existing  = None
                fromdate  = init_from

        try:
            df = krx.get_market_ohlcv_by_date(fromdate, todate, ticker)
            time.sleep(API_DELAY)
        except Exception:
            err_cnt += 1
            continue

        if df is None or df.empty:
            ok_cnt += 1
            continue

        df = df.reset_index()
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "date"})
        cols = list(df.columns)
        col_map = {}
        for tgt, src in [("open", "시가"), ("high", "고가"), ("low", "저가"),
                         ("close", "종가"), ("volume", "거래량")]:
            if src in cols:
                col_map[src] = tgt
        if not col_map and len(cols) >= 6:
            for i, tgt in enumerate(["open", "high", "low", "close", "volume"], 1):
                if i < len(cols):
                    col_map[cols[i]] = tgt
        df = df.rename(columns=col_map)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        keep = ["date"] + [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df   = df[keep].copy()

        # 기존 데이터와 합치기
        if existing is not None and not existing.empty:
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

        df.to_csv(path, index=False, encoding="utf-8")
        ok_cnt += 1

    print(f"\n  완료: {ok_cnt}개 업데이트, {skip_cnt}개 최신, {err_cnt}개 실패")


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Q-TRON Gen3 데이터 증분 업데이트")
    parser.add_argument("--index", action="store_true", help="지수만 업데이트")
    parser.add_argument("--fast",  action="store_true", help="지수 + 유니버스만 (OHLCV 스킵)")
    args = parser.parse_args()

    t0 = datetime.now()
    print(f"=== Q-TRON Gen3 데이터 업데이트 시작: {t0.strftime('%Y-%m-%d %H:%M:%S')} ===\n")

    update_index()

    if not args.index:
        tickers = update_universe()
        if not args.fast and tickers:
            update_ohlcv(tickers)

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\n=== 완료: {elapsed:.0f}초 ({elapsed/60:.1f}분) ===")


if __name__ == "__main__":
    main()
