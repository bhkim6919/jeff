"""
ohlcv_collector.py — Overnight OHLCV bulk downloader
======================================================
KOSPI + KOSDAQ 전종목 OHLCV를 pykrx로 다운로드 후 CSV 저장.
기존 949종목 backtest/data_full/ohlcv/ 와 별도 경로에 저장.

출력 경로: backtest/data_full/ohlcv_expanded/
기존 경로: backtest/data_full/ohlcv/  (변경 없음)

특징:
  - 재시작 안전 (이미 수집된 종목 스킵)
  - 진행률 + ETA 출력
  - 실패 종목 별도 로그
  - 완료 후 통계 출력

Usage:
    cd C:\\Q-TRON-32_ARCHIVE\\kr-legacy
    ..\\.venv\\Scripts\\python.exe -u backtest\\ohlcv_collector.py
    ..\\.venv\\Scripts\\python.exe -u backtest\\ohlcv_collector.py --start 20190102 --delay 0.5
    ..\\.venv\\Scripts\\python.exe -u backtest\\ohlcv_collector.py --retry-failed
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

# ── pykrx import ──────────────────────────────────────────────────────────────
try:
    from pykrx import stock as krx
    PYKRX_OK = True
except ImportError:
    krx = None
    PYKRX_OK = False

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent.parent  # Q-TRON-32_ARCHIVE/
OUTPUT_DIR    = BASE_DIR / "backtest" / "data_full" / "ohlcv_expanded"
LOG_DIR       = BASE_DIR / "kr-legacy" / "data" / "logs"
FAILED_LOG    = LOG_DIR / "collector_failed.log"
PROGRESS_FILE = OUTPUT_DIR / "_progress.txt"  # 완료된 종목 목록

# ── 파라미터 기본값 ──────────────────────────────────────────────────────────
DEFAULT_START   = "20190102"
DEFAULT_MARKETS = ["KOSPI", "KOSDAQ"]
DEFAULT_DELAY   = 0.5    # 초 (pykrx 안전 딜레이)
MAX_RETRY       = 3      # 종목당 재시도 횟수
RETRY_DELAY     = 5.0    # 재시도 간격 (초)

# ── 로깅 설정 ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("collector")


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _today_str() -> str:
    """장 마감 이후(16:00) 기준 오늘, 그 이전이면 어제."""
    d = datetime.today()
    if d.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _pykrx_call(func, *args, **kwargs):
    """pykrx 내부 로그 노이즈 억제 후 호출."""
    root = logging.getLogger()
    prev = root.level
    root.setLevel(logging.ERROR)
    try:
        return func(*args, **kwargs)
    finally:
        root.setLevel(prev)


def _get_tickers(markets: List[str]) -> List[str]:
    """KOSPI + KOSDAQ 보통주 코드 목록 (6자리 숫자, 끝자리 0).

    1차: pykrx API
    2차: OUTPUT_DIR 기존 CSV 파일명
    3차: backtest/data_full/ohlcv/ 기존 CSV 파일명 (배치가 유지하는 2769종목)
    """
    all_codes = []
    ref_date = _today_str()

    for market in markets:
        success = False
        d_cursor = datetime.strptime(ref_date, "%Y%m%d")
        for attempt in range(1, 6):
            while d_cursor.weekday() >= 5:
                d_cursor -= timedelta(days=1)
            date_str = d_cursor.strftime("%Y%m%d")
            try:
                tickers = _pykrx_call(
                    krx.get_market_ticker_list, date_str, market=market
                )
                if tickers:
                    common = [t for t in tickers
                              if len(t) == 6 and t.isdigit() and t[-1] == '0']
                    logger.info(f"  {market}: {len(tickers)} total, "
                                f"{len(common)} common stocks (pykrx)")
                    all_codes.extend(common)
                    success = True
                    break
                logger.warning(f"  {market} attempt {attempt}: empty response")
            except Exception as e:
                logger.warning(f"  {market} attempt {attempt}: {e}")
            time.sleep(1)
            d_cursor -= timedelta(days=1)

        if not success:
            logger.warning(f"  {market}: pykrx 실패 → CSV 폴백")

    # pykrx 완전 실패 시 기존 CSV에서 종목 목록 구성
    # 반드시 ohlcv/ (배치 유지 마스터) 사용 — ohlcv_expanded/ 는 출력 디렉토리라 사용 금지
    if not all_codes:
        master_dir = BASE_DIR / "backtest" / "data_full" / "ohlcv"
        if master_dir.exists():
            codes = sorted(
                f.stem for f in master_dir.glob("*.csv")
                if len(f.stem) == 6 and f.stem.isdigit() and f.stem[-1] == '0'
            )
            if codes:
                logger.info(f"  CSV 폴백: {len(codes)} stocks from ohlcv/ (master)")
                all_codes = codes

    # 중복 제거, 정렬
    return sorted(set(all_codes))


def _load_completed() -> set:
    """이미 수집 완료된 종목 코드 집합."""
    if PROGRESS_FILE.exists():
        lines = PROGRESS_FILE.read_text(encoding="utf-8").splitlines()
        return set(l.strip() for l in lines if l.strip())
    return set()


def _mark_completed(code: str) -> None:
    """완료 종목 추가."""
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(code + "\n")


def _log_failed(code: str, reason: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {code}  {reason}\n")


def _is_fresh(path: Path, today_str: str) -> bool:
    """
    기존 CSV가 오늘 날짜 데이터를 포함하고 있으면 스킵.
    (장이 끝난 이후 기준 → _today_str() = 오늘 또는 어제)
    """
    if not path.exists():
        return False
    try:
        df = pd.read_csv(path, usecols=["date"])
        if df.empty:
            return False
        last_date = str(df["date"].max()).replace("-", "")
        # 최신 날짜가 기준일 3일 이내면 fresh로 간주 (주말/공휴일 고려)
        last_dt = datetime.strptime(last_date, "%Y%m%d")
        today_dt = datetime.strptime(today_str, "%Y%m%d")
        return (today_dt - last_dt).days <= 3
    except Exception:
        return False


def _download_one(code: str, start: str, end: str, delay: float) -> Optional[pd.DataFrame]:
    """단일 종목 OHLCV 다운로드. 실패 시 None."""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            df = _pykrx_call(krx.get_market_ohlcv_by_date, start, end, code)
            time.sleep(delay)

            if df is None or df.empty:
                if attempt < MAX_RETRY:
                    time.sleep(RETRY_DELAY)
                    continue
                return None

            df = df.reset_index()
            col_map = {
                "날짜": "date", "시가": "open", "고가": "high",
                "저가": "low",  "종가": "close", "거래량": "volume",
            }
            df = df.rename(columns=col_map)

            if "date" not in df.columns and df.index.name in ("날짜", "date"):
                df = df.reset_index()
                df = df.rename(columns=col_map)

            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

            if "date" not in df.columns:
                return None

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

            result = df[["date", "open", "high", "low", "close", "volume"]]
            if len(result) < 20:  # 데이터 너무 적으면 skip
                return None
            return result

        except Exception as e:
            if attempt < MAX_RETRY:
                logger.debug(f"    retry {attempt}/{MAX_RETRY} {code}: {e}")
                time.sleep(RETRY_DELAY)
            else:
                return None

    return None


def _eta(elapsed: float, done: int, total: int) -> str:
    if done == 0:
        return "?"
    per = elapsed / done
    remaining = per * (total - done)
    h = int(remaining // 3600)
    m = int((remaining % 3600) // 60)
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m"


# ── 메인 수집 루프 ─────────────────────────────────────────────────────────────

def run_collect(args):
    if not PYKRX_OK:
        logger.error("pykrx 설치 필요: pip install pykrx")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    end_str = _today_str()
    start_str = args.start.replace("-", "")

    logger.info("=" * 60)
    logger.info("  OHLCV Collector — Overnight Bulk Download")
    logger.info(f"  Start : {start_str}")
    logger.info(f"  End   : {end_str}")
    logger.info(f"  Output: {OUTPUT_DIR}")
    logger.info(f"  Delay : {args.delay}s")
    logger.info("=" * 60)

    # 1. 종목 목록
    logger.info("\n[1/3] Getting ticker list...")
    tickers = _get_tickers(args.markets)
    if not tickers:
        logger.error("종목 목록 취득 실패")
        sys.exit(1)
    logger.info(f"  Total: {len(tickers)} common stocks")

    # 2. 이미 완료된 종목 스킵 (재시작 복원)
    completed = _load_completed()

    if args.retry_failed:
        # --retry-failed: 실패 로그에서 코드 추출해서 재시도
        if FAILED_LOG.exists():
            failed_codes = set()
            for line in FAILED_LOG.read_text(encoding="utf-8").splitlines():
                parts = line.strip().split()
                if len(parts) >= 3:
                    failed_codes.add(parts[2])
            tickers = [t for t in tickers if t in failed_codes]
            completed = set()  # 실패 재시도이므로 completed 무시
            logger.info(f"  Retry mode: {len(tickers)} failed stocks")
        else:
            logger.info("  실패 로그 없음. 일반 수집 실행.")

    todo = [t for t in tickers if t not in completed]
    fresh_skip = 0
    for t in list(todo):
        if _is_fresh(OUTPUT_DIR / f"{t}.csv", end_str):
            fresh_skip += 1
            todo.remove(t)

    logger.info(f"\n[2/3] Download plan:")
    logger.info(f"  전체   : {len(tickers)}")
    logger.info(f"  완료   : {len(completed)}")
    logger.info(f"  최신   : {fresh_skip} (스킵)")
    logger.info(f"  수집 대상: {len(todo)}")
    estimated_min = len(todo) * args.delay / 60
    logger.info(f"  예상 시간: ~{estimated_min:.0f}분 (API 딜레이만 기준)")

    if not todo:
        logger.info("\n모든 종목이 이미 수집되어 있습니다.")
        _print_summary(OUTPUT_DIR, start_str)
        return

    # 3. 다운로드
    logger.info(f"\n[3/3] Downloading...")
    t_start = time.time()
    success = 0
    failed = 0
    skipped = 0

    for i, code in enumerate(todo, 1):
        path = OUTPUT_DIR / f"{code}.csv"

        # 기존 파일 있으면 merge, 없으면 신규 저장
        df = _download_one(code, start_str, end_str, args.delay)

        if df is None:
            failed += 1
            _log_failed(code, "download_failed")
            if i % 10 == 0 or failed <= 5:
                elapsed = time.time() - t_start
                logger.warning(
                    f"  [{i:4d}/{len(todo)}] FAIL {code}  "
                    f"(fail={failed}, ETA={_eta(elapsed, i, len(todo))})"
                )
            continue

        # Merge with existing (증분)
        if path.exists():
            try:
                old = pd.read_csv(path)
                df = pd.concat([old, df]).drop_duplicates(
                    subset=["date"], keep="last"
                ).sort_values("date").reset_index(drop=True)
            except Exception:
                pass  # 기존 파일 오류면 새 데이터로 덮어쓰기

        df.to_csv(path, index=False)
        _mark_completed(code)
        success += 1

        # 진행 출력 (50종목마다 + 처음 5종목)
        if i <= 5 or i % 50 == 0 or i == len(todo):
            elapsed = time.time() - t_start
            pct = i / len(todo) * 100
            logger.info(
                f"  [{i:4d}/{len(todo)}] {pct:5.1f}%  "
                f"OK={success} FAIL={failed}  "
                f"ETA={_eta(elapsed, i, len(todo))}"
            )

    # 4. 완료 리포트
    elapsed_total = time.time() - t_start
    logger.info("\n" + "=" * 60)
    logger.info("  수집 완료")
    logger.info(f"  성공  : {success}")
    logger.info(f"  실패  : {failed}  (로그: {FAILED_LOG})")
    logger.info(f"  소요  : {elapsed_total/60:.1f}분")
    logger.info("=" * 60)

    _print_summary(OUTPUT_DIR, start_str)

    if failed > 0:
        logger.info(f"\n  실패 종목 재수집: python ohlcv_collector.py --retry-failed")


def _print_summary(output_dir: Path, start_str: str):
    """수집된 데이터 통계 출력."""
    csvs = list(output_dir.glob("*.csv"))
    valid = [f for f in csvs if f.name != "_progress.txt"]

    if not valid:
        return

    total_rows = 0
    min_rows = 99999
    max_rows = 0
    short = []

    for f in valid:
        try:
            n = sum(1 for _ in open(f)) - 1  # header 제외
            total_rows += n
            if n < min_rows:
                min_rows = n
            if n > max_rows:
                max_rows = n
            if n < 260:
                short.append((f.stem, n))
        except Exception:
            pass

    logger.info(f"\n[수집 통계]")
    logger.info(f"  종목 수    : {len(valid)}")
    logger.info(f"  총 행 수   : {total_rows:,}")
    logger.info(f"  행 범위    : {min_rows} ~ {max_rows}")
    logger.info(f"  260일 미만 : {len(short)}종목 (백테스트 제외 예정)")
    if short[:5]:
        logger.info(f"    예시: {[c for c, _ in short[:5]]}")
    logger.info(f"\n  백테스터 OHLCV_DIR 변경 방법:")
    logger.info(f"    config.py OHLCV_DIR → '{output_dir}'")


# ── 진입점 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="OHLCV Collector — KOSPI+KOSDAQ overnight download"
    )
    parser.add_argument("--start",  default=DEFAULT_START,
                        help=f"시작 날짜 YYYYMMDD (기본: {DEFAULT_START})")
    parser.add_argument("--markets", nargs="+", default=DEFAULT_MARKETS,
                        choices=["KOSPI", "KOSDAQ"],
                        help="수집 시장 (기본: KOSPI KOSDAQ)")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"API 호출 간격 초 (기본: {DEFAULT_DELAY})")
    parser.add_argument("--retry-failed", action="store_true",
                        help="이전 실패 종목만 재수집")
    args = parser.parse_args()

    run_collect(args)


if __name__ == "__main__":
    main()
