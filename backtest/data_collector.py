"""
data_collector.py
=================
pykrx를 이용해 백테스트용 과거 데이터를 수집하고 CSV로 저장.

[pykrx 1.0.45 + KRX 서버 부분 장애 대응]
  동작하는 API : get_market_ohlcv_by_date(start, end, code)
  동작 안 하는 : get_index_ohlcv_by_date, get_market_cap_by_ticker,
                get_market_ticker_list, get_market_ohlcv_by_ticker

  대응 전략:
  - 영업일  : 삼성전자(005930) 일봉 인덱스로 계산
  - 유니버스 : KOSPI/KOSDAQ 주요 종목 하드코딩 (2022~2025 상장 유지 종목)
  - 지수    : 삼성전자(KOSPI proxy), NAVER(KOSDAQ proxy) 일봉으로 대체

저장 구조:
  backtest/data/YYYYMMDD.csv
  backtest/data/index/KOSPI.csv
  backtest/data/index/KOSDAQ.csv

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
DEFAULT_START = "20220101"
DEFAULT_END   = "20251231"
DEFAULT_TOP   = 300

BIZDAY_ANCHOR = "005930"
INDEX_PROXY   = {"KOSPI": "005930", "KOSDAQ": "035420"}


# ── 하드코딩 유니버스 (2022~2025 상장 유지 종목, 시가총액 상위) ──────────────
_KOSPI = [
    "005930","000660","005935","207940","005380","000270","068270","105560",
    "035420","012330","055550","066570","028260","096770","003550","032830",
    "051910","086790","017670","003490","034730","011200","009150","018260",
    "015760","011170","030200","316140","010950","003670","033780","009830",
    "010130","000810","042700","008770","161390","090430","004020","006400",
    "018880","034020","024110","011780","000100","001040","047050","097950",
    "071050","006800","029780","010140","001800","032640","001450","004370",
    "002380","000720","016360","005490","139480","267250","011070","004490",
    "000080","021240","007070","138930","069960","002350","011210","001120",
    "023530","004000","009540","006260","004170","005940","001630","006120",
    "000880","014820","002760","005010","006650","000150","001740","005850",
    "001430","007310","000990","002600","001060","000070","004140","007160",
    "002320","000210","001530","007860","002240","006490","003460","004830",
    "001390","004560","002310","000950","003080","005960","002200","001270",
    "001680","007570","002450","003410","006110","001290","000230","004890",
    "002030","000400","001940","002630","005250","006740","001550","003230",
    "007700","002990","001140","006040","003560","001810","006360","007690",
    "004060","003580","006200","004430","006730","003350","004310","003960",
    "003690","002700","004650","002820","002880","004380","002870","004410",
    "002790","001570",
]

_KOSDAQ = [
    "035420","247540","086900","196170","263750","112040","357780","145020",
    "091990","141080","214150","293490","066970","041510","054040","950130",
    "039030","253450","112610","236200","122870","058470","039440","064760",
    "035080","048260","067160","096530","048410","060310","036930","049430",
    "060250","078890","053800","036620","049800","079960","053210","036810",
    "049770","060150","079170","053050","036710","079000","052900","052790",
    "078150","052460","048910","059210","052020","035720","059100","035610",
    "048470","263720","328130","041960","066575","032640","950160","011560",
    "068760","078130","036930","041510","054040","066970","086900","091990",
    "096530","112040","112610","122870","141080","145020","196170","214150",
    "236200","247540","253450","263750","293490","357780",
]

HARDCODED_UNIVERSE = list(dict.fromkeys(_KOSPI + _KOSDAQ))


# ── 유틸 ─────────────────────────────────────────────────────────────────────
def _sleep():
    time.sleep(API_DELAY)

def _parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d")

def _format_ymd(d: datetime) -> str:
    return d.strftime("%Y%m%d")


# ── 영업일 리스트 ─────────────────────────────────────────────────────────────
def get_bizdays(start: str, end: str) -> List[str]:
    print(f"[BizDays] 삼성전자 기준 영업일 계산 ({start}~{end})")
    df = krx.get_market_ohlcv_by_date(start, end, BIZDAY_ANCHOR)
    _sleep()
    if df is None or df.empty:
        raise RuntimeError("영업일 계산 실패")
    bdays = df.index.strftime("%Y%m%d").tolist()
    print(f"[BizDays] 총 {len(bdays)}개")
    return bdays


# ── 유니버스 구성 ─────────────────────────────────────────────────────────────
def build_universe(top_n: int) -> List[str]:
    universe = HARDCODED_UNIVERSE[:top_n]
    print(f"[Universe] 하드코딩 유니버스 {len(universe)}개 사용")
    return universe


# ── 지수 수집 ─────────────────────────────────────────────────────────────────
def collect_index(start: str, end: str):
    for name, proxy_code in INDEX_PROXY.items():
        save_path = INDEX_DIR / f"{name}.csv"
        print(f"[Index] {name} 수집 중...")

        index_ticker = "1001" if name == "KOSPI" else "2001"
        try:
            df = krx.get_index_ohlcv_by_date(start, end, index_ticker)
            _sleep()
            if df is not None and not df.empty:
                df = df.reset_index()
                date_col = df.columns[0]
                rename_map = {s: d for s, d in [
                    ("시가","open"),("고가","high"),("저가","low"),
                    ("종가","close"),("거래량","volume"),
                ] if s in df.columns}
                df = df.rename(columns=rename_map).rename(columns={date_col: "date"})
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
                keep = [c for c in ["date","open","high","low","close","volume"] if c in df.columns]
                df[keep].to_csv(save_path, index=False, encoding="utf-8-sig")
                print(f"[Index] {name} 실제 지수 저장 완료 ({len(df)}일)")
                continue
        except Exception:
            pass

        try:
            df = krx.get_market_ohlcv_by_date(start, end, proxy_code)
            _sleep()
            if df is None or df.empty:
                print(f"[WARN] {name} 데이터 없음")
                continue
            df = df.reset_index()
            date_col = df.columns[0]
            rename_map = {s: d for s, d in [
                ("시가","open"),("고가","high"),("저가","low"),
                ("종가","close"),("거래량","volume"),
            ] if s in df.columns}
            df = df.rename(columns=rename_map).rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d")
            keep = [c for c in ["date","open","high","low","close","volume"] if c in df.columns]
            df[keep].to_csv(save_path, index=False, encoding="utf-8-sig")
            print(f"[Index] {name} 대체종목({proxy_code}) 저장 완료 ({len(df)}일)")
        except Exception as e:
            print(f"[ERROR] {name} 수집 실패: {e}")


# ── 종목 일봉 수집 ────────────────────────────────────────────────────────────
def collect_stocks(universe: List[str], bdays: List[str], start: str, end: str):
    print(f"\n[Stock] {len(universe)}개 종목 × {len(bdays)}일 수집 시작")
    start_dt = _parse_ymd(start)
    end_dt   = _parse_ymd(end)
    bday_set = set(bdays)
    day_buffer: Dict[str, List[dict]] = {d: [] for d in bdays}

    success_codes = []
    failed_codes  = []

    for i, code in enumerate(universe, 1):
        print(f"[{i:4d}/{len(universe):4d}] {code} ...", end="", flush=True)
        try:
            df = krx.get_market_ohlcv_by_date(start, end, code)
            _sleep()
            if df is None or df.empty:
                print(" 없음")
                failed_codes.append(code)
                continue

            df = df.reset_index()
            date_col = df.columns[0]
            rename_map = {s: d for s, d in [
                ("시가","open"),("고가","high"),("저가","low"),
                ("종가","close"),("거래량","volume"),("거래대금","amount"),
                ("시가총액","market_cap"),("Marcap","market_cap"),
            ] if s in df.columns}
            df = df.rename(columns=rename_map).rename(columns={date_col: "date"})
            df["date"] = pd.to_datetime(df["date"])
            for col in ["amount", "market_cap"]:
                if col not in df.columns:
                    df[col] = 0

            row_count = 0
            for _, row in df.iterrows():
                d = row["date"]
                if not (start_dt <= d <= end_dt):
                    continue
                d_str = d.strftime("%Y%m%d")
                if d_str not in bday_set:
                    continue
                day_buffer[d_str].append({
                    "code":       str(code).zfill(6),
                    "open":       float(row.get("open", 0) or 0),
                    "high":       float(row.get("high", 0) or 0),
                    "low":        float(row.get("low", 0) or 0),
                    "close":      float(row.get("close", 0) or 0),
                    "volume":     int(row.get("volume", 0) or 0),
                    "amount":     float(row.get("amount", 0) or 0),
                    "market_cap": float(row.get("market_cap", 0) or 0),
                })
                row_count += 1

            print(f" {row_count}일")
            success_codes.append(code)

        except Exception as e:
            print(f" 실패: {e}")
            failed_codes.append(code)

    # ── 결과 요약 ──
    print(f"\n[Stock] 수집 결과 요약")
    print(f"  성공: {len(success_codes)}개")
    print(f"  실패: {len(failed_codes)}개" + (f" → {failed_codes}" if failed_codes else ""))

    # ── CSV 저장 ──
    print(f"\n[Stock] CSV 저장 중...")
    saved = 0
    for date_str, rows in day_buffer.items():
        if not rows:
            continue
        df_out = pd.DataFrame(rows)
        df_out["code"] = df_out["code"].astype(str).str.zfill(6)
        df_out.to_csv(DATA_DIR / f"{date_str}.csv", index=False, encoding="utf-8-sig")
        saved += 1
    print(f"[Stock] 완료 → {saved}개 파일")


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Q-TRON 백테스트 데이터 수집기")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end",   default=DEFAULT_END)
    parser.add_argument("--top",   type=int, default=DEFAULT_TOP)
    args = parser.parse_args()

    print("=" * 60)
    print("Q-TRON 데이터 수집기 v3 (pykrx 1.0.45 / KRX 장애 대응)")
    print(f"  기간      : {args.start} ~ {args.end}")
    print(f"  유니버스  : 하드코딩 상위 {args.top}개")
    print(f"  저장 경로 : {DATA_DIR}")
    print("=" * 60)

    collect_index(args.start, args.end)
    universe = build_universe(args.top)
    bdays    = get_bizdays(args.start, args.end)
    collect_stocks(universe, bdays, args.start, args.end)

    print("\n" + "=" * 60)
    print("[완료]")
    print(f"  날짜별 CSV : {DATA_DIR}/*.csv")
    print(f"  지수 CSV   : {INDEX_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()