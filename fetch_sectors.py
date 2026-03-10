"""
fetch_sectors.py
================
KRX에서 KOSPI + KOSDAQ 전 종목의 업종(섹터) 매핑을 수집하여
data/sector_map.json 으로 저장한다.

출력 형식:
  {
    "005930": "전기·전자",
    "000660": "전기·전자",
    "035420": "서비스업",
    ...
  }

사용법:
  python fetch_sectors.py                        # 기본 (data/sector_map.json)
  python fetch_sectors.py --output my_map.json   # 출력 경로 지정
  python fetch_sectors.py --verbose              # 섹터별 종목 수 출력

의존:
  pip install pykrx  (이미 설치되어 있음)

특이사항:
  - Kiwoom / 증권사 API 불필요, KRX 공식 데이터 직접 수집
  - 수집 실패 종목은 "기타" 섹터로 분류
  - 약 2,700~2,800개 종목 수집, 소요 시간 약 30~60초
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


def _get_recent_business_day() -> str:
    """
    pykrx는 휴장일 데이터를 반환하지 않으므로
    오늘 또는 최근 영업일(월~금) 날짜를 YYYYMMDD 형식으로 반환.
    """
    d = date.today()
    # 토/일이면 가장 최근 금요일로
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def fetch_sector_map(
    output_path: str = "data/sector_map.json",
    verbose: bool = False,
    retry: int = 3,
) -> dict[str, str]:
    """
    KRX에서 KOSPI + KOSDAQ 섹터 매핑 수집.

    Returns:
        {ticker: sector_name} dict
    """
    try:
        from pykrx import stock as krx
    except ImportError:
        raise ImportError(
            "pykrx가 설치되지 않았습니다.\n"
            "pip install pykrx  를 실행한 후 다시 시도하세요."
        )

    base_date = _get_recent_business_day()
    print(f"[fetch_sectors] 기준일: {base_date}")
    print(f"[fetch_sectors] KOSPI + KOSDAQ 섹터 수집 시작...")

    sector_map: dict[str, str] = {}

    for market in ["KOSPI", "KOSDAQ"]:
        print(f"  [{market}] 수집 중...", end=" ", flush=True)

        for attempt in range(retry):
            try:
                # pykrx: 업종별 종목 목록 (ticker → 업종명)
                df = krx.get_market_sector_classifications(base_date, market=market)
                break
            except Exception as e:
                if attempt < retry - 1:
                    print(f"재시도 ({attempt+1}/{retry})...", end=" ", flush=True)
                    time.sleep(2)
                else:
                    print(f"실패: {e}")
                    df = None

        if df is None or df.empty:
            print(f"[경고] {market} 데이터 없음 → 건너뜀")
            continue

        # pykrx 반환 컬럼: '업종명' (인덱스가 ticker)
        count = 0
        for ticker, row in df.iterrows():
            ticker_str = str(ticker).zfill(6)
            sector = str(row.get("업종명", "기타")).strip()
            if not sector:
                sector = "기타"
            sector_map[ticker_str] = sector
            count += 1

        print(f"{count}개 완료")
        time.sleep(0.5)  # KRX 서버 부하 방지

    total = len(sector_map)
    print(f"\n[fetch_sectors] 총 {total}개 종목 수집 완료")

    # 섹터별 통계
    if verbose or total == 0:
        sector_counts: dict[str, int] = defaultdict(int)
        for s in sector_map.values():
            sector_counts[s] += 1
        print("\n── 섹터별 종목 수 ──────────────────────────")
        for sector, cnt in sorted(sector_counts.items(), key=lambda x: -x[1]):
            print(f"  {sector:<20} {cnt:>4}개")
        print("─" * 40)

    # 저장
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sector_map, f, ensure_ascii=False, indent=2)

    print(f"\n[fetch_sectors] 저장 완료: {output_path}  ({total}개 종목)")
    return sector_map


def validate_sector_map(path: str) -> None:
    """저장된 sector_map.json 유효성 간단 검증."""
    if not os.path.exists(path):
        print(f"[검증] 파일 없음: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)
    sectors = set(data.values())
    sample = list(data.items())[:5]

    print(f"\n[검증] {path}")
    print(f"  종목 수  : {total}개")
    print(f"  섹터 수  : {len(sectors)}개")
    print(f"  샘플     : {sample}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KRX 섹터 매핑 수집 → sector_map.json")
    parser.add_argument("--output",  default="data/sector_map.json", help="저장 경로")
    parser.add_argument("--verbose", action="store_true",            help="섹터별 종목 수 출력")
    parser.add_argument("--validate", action="store_true",           help="기존 파일 검증만 실행")
    args = parser.parse_args()

    if args.validate:
        validate_sector_map(args.output)
    else:
        fetch_sector_map(output_path=args.output, verbose=args.verbose)
        validate_sector_map(args.output)
