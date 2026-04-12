"""
universe_builder.py — Tradeable universe construction
======================================================
Quality filters:
  1. Close >= UNIV_MIN_CLOSE (2000 KRW)
  2. 20-day avg daily traded value >= UNIV_MIN_AMOUNT (2B KRW)
  3. History >= UNIV_MIN_HISTORY (260 days)
  4. Not preferred stock (code ending in 5-9)
  5. Not trading halt (close > 0)

Warns if universe < UNIV_MIN_COUNT (500).
"""
from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger("gen4.universe")


def is_preferred_stock(code: str) -> bool:
    """Check if code is a preferred stock (ends in 5-9)."""
    if len(code) == 6 and code.isdigit():
        return int(code[-1]) >= 5
    return False


def build_universe_from_ohlcv(ohlcv_dir: Path,
                              min_close: int = 2000,
                              min_amount: float = 2e9,
                              min_history: int = 260,
                              min_count: int = 500,
                              allowed_markets: Optional[List[str]] = None,
                              sector_map: Optional[dict] = None) -> List[str]:
    """
    Build universe from per-stock OHLCV CSV files.

    Args:
        ohlcv_dir: Directory containing {code}.csv files.
        min_close: Minimum latest close price.
        min_amount: Minimum 20-day avg daily traded value.
        min_history: Minimum number of trading days.
        min_count: Warn if universe below this.

    Returns:
        List of ticker codes passing all filters.
    """
    universe = []
    total = 0
    filtered = {"preferred": 0, "history": 0, "close": 0, "amount": 0}

    for f in sorted(ohlcv_dir.glob("*.csv")):
        code = f.stem
        total += 1

        if is_preferred_stock(code):
            filtered["preferred"] += 1
            continue

        # Market filter (KOSPI only, etc.)
        # sector_map format: {code: {"market": "KOSPI", ...}} or {code: "섹터명"}
        # If sector_map has no "market" key (str values = sector names), skip filter
        if allowed_markets and sector_map:
            entry = sector_map.get(code)
            if isinstance(entry, dict):
                ticker_market = entry.get("market", "")
                if ticker_market and ticker_market not in allowed_markets:
                    continue
            # str entries = sector name, not market → skip market filter

        try:
            df = pd.read_csv(f, parse_dates=["date"])
            for c in ("close", "volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        except Exception:
            continue

        if len(df) < min_history:
            filtered["history"] += 1
            continue

        last_close = float(df["close"].iloc[-1])
        if last_close < min_close:
            filtered["close"] += 1
            continue

        # 20-day avg daily traded value
        recent = df.tail(20)
        avg_value = (recent["close"] * recent["volume"]).mean()
        if avg_value < min_amount:
            filtered["amount"] += 1
            continue

        universe.append(code)

    logger.info(f"Universe: {len(universe)}/{total} stocks passed "
                f"(pref={filtered['preferred']}, hist={filtered['history']}, "
                f"close={filtered['close']}, amount={filtered['amount']})")

    if len(universe) < min_count:
        logger.warning(f"Universe size {len(universe)} < minimum {min_count}!")

    return universe


def build_universe_from_pykrx(min_close: int = 2000,
                               min_amount: float = 2e9,
                               markets: Optional[List[str]] = None) -> List[str]:
    """
    Build universe using pykrx live data.
    For live/batch mode.
    """
    if markets is None:
        markets = ["KOSPI", "KOSDAQ"]
    try:
        from pykrx import stock as krx
        from datetime import datetime, timedelta

        today = datetime.today().strftime("%Y%m%d")
        tickers = []
        for market in markets:
            market_tickers = krx.get_market_ticker_list(today, market=market)
            if market_tickers:
                tickers.extend(market_tickers)
                logger.info(f"pykrx {market}: {len(market_tickers)} tickers")
        if not tickers:
            logger.warning("pykrx returned 0 tickers, market may be closed")
            return []

        universe = []
        for code in tickers:
            if is_preferred_stock(code):
                continue

            try:
                # Get recent OHLCV
                start = (datetime.today() - timedelta(days=40)).strftime("%Y%m%d")
                df = krx.get_market_ohlcv_by_date(start, today, code)
                if len(df) < 5:
                    continue

                last_close = float(df.iloc[-1]["종가"])
                if last_close < min_close:
                    continue

                avg_value = float((df["종가"] * df["거래량"]).tail(20).mean())
                if avg_value < min_amount:
                    continue

                universe.append(code)
            except Exception:
                continue

        logger.info(f"pykrx universe: {len(universe)} stocks")
        return universe

    except ImportError:
        logger.error("pykrx not installed")
        return []
