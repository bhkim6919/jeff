"""
bootstrap_kosdaq.py — Download KOSDAQ OHLCV history (2019~present)
=================================================================
Fetches 7-year OHLCV for all KOSDAQ stocks listed in sector_map.json
and saves to backtest/data_full/ohlcv/.

Usage:
    cd Gen04
    python -m data.bootstrap_kosdaq          # download missing only
    python -m data.bootstrap_kosdaq --force   # re-download all
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    print("ERROR: pykrx not installed. pip install pykrx")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("bootstrap_kosdaq")

BASE_DIR = Path(__file__).resolve().parent.parent
OHLCV_DIR = BASE_DIR.parent / "backtest" / "data_full" / "ohlcv"
SECTOR_MAP = BASE_DIR.parent / "backtest" / "data_full" / "sector_map.json"

API_DELAY = 0.35
START_DATE = "20190102"


def _suppress_pykrx(func, *args, **kwargs):
    """Call pykrx with suppressed logging noise."""
    pykrx_logger = logging.getLogger("pykrx")
    old_level = pykrx_logger.level
    pykrx_logger.setLevel(logging.CRITICAL)
    try:
        return func(*args, **kwargs)
    finally:
        pykrx_logger.setLevel(old_level)


def _end_date() -> str:
    from datetime import datetime, timedelta
    d = datetime.today()
    if d.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def download_ohlcv(code: str, start: str, end: str) -> pd.DataFrame | None:
    """Download OHLCV for a single stock."""
    try:
        df = _suppress_pykrx(krx.get_market_ohlcv_by_date, start, end, code)
        time.sleep(API_DELAY)

        if df is None or df.empty:
            return None

        # pykrx returns index=날짜, columns=[시가,고가,저가,종가,거래량,등락률]
        df = df.reset_index()
        col_map = {
            "\ub0a0\uc9dc": "date", "\uc2dc\uac00": "open", "\uace0\uac00": "high",
            "\uc800\uac00": "low", "\uc885\uac00": "close", "\uac70\ub798\ub7c9": "volume",
        }
        df = df.rename(columns=col_map)

        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        else:
            logger.warning(f"  FAIL {code}: 'date' column not found after rename, cols={df.columns.tolist()}")
            return None

        return df[["date", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logger.warning(f"  FAIL {code}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Bootstrap KOSDAQ OHLCV data")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if CSV exists")
    parser.add_argument("--market", default="KOSDAQ",
                        help="Market to download (default: KOSDAQ)")
    args = parser.parse_args()

    if not SECTOR_MAP.exists():
        logger.error(f"sector_map not found: {SECTOR_MAP}")
        sys.exit(1)

    OHLCV_DIR.mkdir(parents=True, exist_ok=True)

    with open(SECTOR_MAP, encoding="utf-8") as f:
        sector_map = json.load(f)

    # sector_map: {code: "섹터명"} (str) or {code: {"market": ...}} (dict)
    codes = sorted(
        k for k, v in sector_map.items()
        if (isinstance(v, dict) and v.get("market") == args.market)
        or (isinstance(v, str))  # str = 마켓 구분 없음 → 전체 포함
    )
    logger.info(f"[BOOTSTRAP] {args.market}: {len(codes)} stocks")

    end = _end_date()
    logger.info(f"[BOOTSTRAP] Range: {START_DATE} ~ {end}")

    existing = set(f.stem for f in OHLCV_DIR.glob("*.csv"))
    if not args.force:
        skip_count = sum(1 for c in codes if c in existing)
        codes = [c for c in codes if c not in existing]
        if skip_count > 0:
            logger.info(f"[BOOTSTRAP] Skipping {skip_count} existing, downloading {len(codes)}")

    if not codes:
        logger.info("[BOOTSTRAP] Nothing to download")
        return

    success = 0
    fail = 0
    for i, code in enumerate(codes, 1):
        df = download_ohlcv(code, START_DATE, end)
        if df is not None and len(df) > 0:
            path = OHLCV_DIR / f"{code}.csv"
            df.to_csv(path, index=False)
            success += 1
        else:
            fail += 1

        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(codes)} (ok={success}, fail={fail})")

    logger.info(f"[BOOTSTRAP] Done: {success} downloaded, {fail} failed, "
                f"total in dir: {len(list(OHLCV_DIR.glob('*.csv')))}")


if __name__ == "__main__":
    main()
