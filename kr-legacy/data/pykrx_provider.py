"""
pykrx_provider.py — OHLCV data via pykrx
==========================================
Adapted from Gen3 (95% reuse).
Provides historical price data for batch scoring and universe building.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    krx = None

logger = logging.getLogger("gen4.pykrx")

_API_DELAY = 0.3


def _today() -> str:
    """Last business day with available data.

    KRX returns empty for weekends/holidays, and for the current day
    before market close (~15:30). Use previous business day if before 16:00.
    """
    d = datetime.today()
    if d.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _n_days_ago(n: int) -> str:
    return (datetime.today() - timedelta(days=n)).strftime("%Y%m%d")


def get_stock_ohlcv(code: str, days: int = 400) -> Optional[pd.DataFrame]:
    """
    Get stock OHLCV from pykrx.

    Returns DataFrame with columns: date, open, high, low, close, volume
    """
    if krx is None:
        logger.error("pykrx not installed")
        return None

    try:
        start = _n_days_ago(int(days * 1.5))  # buffer for weekends/holidays
        end = _today()
        df = _pykrx_call_suppressed(krx.get_market_ohlcv_by_date, start, end, code)
        time.sleep(_API_DELAY)

        if df.empty:
            return None

        df = df.reset_index()
        col_map = {"날짜": "date", "시가": "open", "고가": "high",
                    "저가": "low", "종가": "close", "거래량": "volume"}
        df = df.rename(columns=col_map)

        # Handle index name
        if "date" not in df.columns and df.index.name in ("날짜", "date"):
            df = df.reset_index()
            df = df.rename(columns=col_map)

        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # FIX-A3: close=0 행 제거 (거래정지/상폐 → -100% return spike 방지)
        before_len = len(df)
        df = df[df["close"] > 0]
        removed = before_len - len(df)
        if removed > 0:
            logger.info(f"[OHLCV_FILTER] {code}: removed {removed}/{before_len} "
                        f"rows with close=0")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

        return df[["date", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logger.warning(f"Failed to get OHLCV for {code}: {e}")
        return None


def _pykrx_call_suppressed(func, *args, **kwargs):
    """Call pykrx function with root logger noise suppressed.

    pykrx internally calls logging.info(args, kwargs) which triggers
    TypeError due to printf-style formatting. We temporarily raise
    the root logger level to WARNING to suppress this noise.
    """
    root = logging.getLogger()
    prev_level = root.level
    root.setLevel(logging.WARNING)
    try:
        return func(*args, **kwargs)
    finally:
        root.setLevel(prev_level)


def get_stock_list(market: str = "KOSPI",
                   ohlcv_dir: Optional[Path] = None) -> List[str]:
    """Get list of tickers for a market.

    Tries pykrx first with retry; if broken (known issue with pykrx<=1.0.51
    and KRX API changes in 2026), falls back to existing CSV filenames.
    """
    MAX_RETRY = 5
    # Try pykrx
    if krx is not None:
        d = datetime.today()
        # pykrx ticker list is unreliable for today (especially after-hours).
        # Always start from the previous day to ensure valid trading date.
        d -= timedelta(days=1)
        for attempt in range(1, MAX_RETRY + 1):
            while d.weekday() >= 5:
                d -= timedelta(days=1)
            date_str = d.strftime("%Y%m%d")
            try:
                tickers = _pykrx_call_suppressed(
                    krx.get_market_ticker_list, date_str, market=market
                )
                if tickers:
                    logger.info("pykrx ticker list: %d (%s)", len(tickers), market)
                    return tickers
                logger.warning(
                    "[PYKRX_RETRY] %d/%d date=%s market=%s error=empty response",
                    attempt, MAX_RETRY, date_str, market,
                )
            except Exception as e:
                err_name = type(e).__name__
                logger.warning(
                    "[PYKRX_RETRY] %d/%d date=%s market=%s error=%s: %s",
                    attempt, MAX_RETRY, date_str, market, err_name, e,
                )
            d -= timedelta(days=1)
            if attempt < MAX_RETRY:
                time.sleep(1)
        logger.warning(
            "[PYKRX_FAIL] get_market_ticker_list failed after %d retries", MAX_RETRY
        )

    # Fallback: CSV directory (market 구분 불가 — 전체 종목 반환)
    if ohlcv_dir and ohlcv_dir.exists():
        tickers = sorted(f.stem for f in ohlcv_dir.glob("*.csv"))
        if tickers:
            logger.info(
                "[PYKRX_FALLBACK] ticker list from CSV: %d stocks "
                "(market=%s ignored — CSV has no market info)", len(tickers), market
            )
            return tickers

    logger.warning("[PYKRX_FALLBACK] no ticker source available")
    return []


def get_index_ohlcv(days: int = 400) -> Optional[pd.DataFrame]:
    """
    Get KOSPI index OHLCV via KODEX 200 ETF (069500) proxy.
    Direct index API has encoding issues.
    """
    return get_stock_ohlcv("069500", days)


def update_ohlcv_incremental(ohlcv_dir: Path, codes: List[str],
                              days: int = 60) -> int:
    """
    Incrementally update per-stock OHLCV CSVs.

    Args:
        ohlcv_dir: Directory containing {code}.csv files.
        codes: List of ticker codes to update.
        days: Number of recent days to fetch.

    Returns:
        Number of stocks updated.
    """
    ohlcv_dir.mkdir(parents=True, exist_ok=True)
    updated = 0

    for i, code in enumerate(codes):
        try:
            path = ohlcv_dir / f"{code}.csv"
            new_df = get_stock_ohlcv(code, days)
            if new_df is None or new_df.empty:
                continue

            if path.exists():
                existing = pd.read_csv(path, parse_dates=["date"])
                combined = pd.concat([existing, new_df]).drop_duplicates(
                    subset=["date"], keep="last")
                combined = combined.sort_values("date").reset_index(drop=True)
                combined.to_csv(path, index=False)
            else:
                new_df.to_csv(path, index=False)

            updated += 1

            if (i + 1) % 50 == 0:
                logger.info(f"  Updated {i+1}/{len(codes)}...")

        except Exception as e:
            logger.warning(f"Failed to update {code}: {e}")
            continue

    logger.info(f"Updated {updated}/{len(codes)} stocks")
    return updated
