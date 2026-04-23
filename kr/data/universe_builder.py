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

Data source principle (R7, 2026-04-23):
  - `build_universe_from_ohlcv`: CSV (현재 default, R4 Stage 1 shadow 중)
  - `build_universe_from_db`: DB (shadow mode, R4 Stage 3 전환 후 primary)
  - DB = canonical truth, CSV = performance cache — 2026-04-22 universe=0 사고
    방지 구조 해결책. 자세한 내용 CLAUDE.md §Data Source 원칙 참조.
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


def build_universe_from_db(db_provider,
                           min_close: int = 2000,
                           min_amount: float = 2e9,
                           min_history: int = 260,
                           min_count: int = 500,
                           allowed_markets: Optional[List[str]] = None,
                           sector_map: Optional[dict] = None) -> List[str]:
    """R4 Stage 1 (2026-04-23): DB-direct universe builder (SHADOW MODE).

    Mirrors `build_universe_from_ohlcv` logic but reads from PostgreSQL
    `ohlcv` table instead of per-stock CSV files. Intended for shadow
    comparison only — NOT default (see work_plan_20260423.md §R4 원칙).

    Motivation: RCA 20260423 identified CSV/DB layer drift as the root
    structural cause of the 10-day loop. CSV truncation → empty universe
    → batch fails. DB holds canonical truth (2019~latest, 2770 codes).
    Decoupling step2 from CSV eliminates this failure mode entirely.

    Algorithm (single SQL aggregation, then Python post-filter):
      1. SQL: per-code aggregate — hist_count, last_close, avg_amount_20d
      2. SQL WHERE: hist_count >= min_history, last_close >= min_close,
                    avg_amount_20d >= min_amount
      3. Python: exclude preferred stocks (code ending in 5-9)
      4. Python: market filter via sector_map (matches CSV behavior)

    Jeff R4 원칙:
      - Default 전환 금지 (이 함수는 shadow only)
      - CSV 실사용 유지, DB 는 diff 로그만

    Args:
        db_provider: DbProvider instance (uses pg_base connection).
        (Rest match build_universe_from_ohlcv exactly.)

    Returns:
        List of ticker codes passing all filters.
    """
    query = """
        WITH recent AS (
            SELECT code, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM ohlcv
        ),
        stats AS (
            SELECT code,
                   COUNT(*) AS hist_count,
                   MAX(CASE WHEN rn = 1 THEN close END) AS last_close,
                   AVG(CASE WHEN rn <= 20 THEN close * volume END) AS avg_amount_20d
            FROM recent
            GROUP BY code
        )
        SELECT code, hist_count, last_close, avg_amount_20d
        FROM stats
        WHERE hist_count >= %(min_history)s
          AND last_close >= %(min_close)s
          AND avg_amount_20d >= %(min_amount)s
        ORDER BY code
    """
    params = {
        "min_history": int(min_history),
        "min_close": float(min_close),
        "min_amount": float(min_amount),
    }

    universe: List[str] = []
    pre_filter_count = 0
    filtered = {"preferred": 0, "market": 0}

    conn = None
    try:
        conn = db_provider._conn()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    except Exception as e:
        logger.error(f"[UNIVERSE_DB] query failed: {e!r}")
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    pre_filter_count = len(rows)

    for row in rows:
        code = row[0]

        # Preferred stock filter (matches CSV builder)
        if is_preferred_stock(code):
            filtered["preferred"] += 1
            continue

        # Market filter via sector_map (matches CSV builder line 66)
        # sector_map format: {code: {"market": "KOSPI", ...}} OR {code: "섹터명"}
        if allowed_markets and sector_map:
            entry = sector_map.get(code)
            if isinstance(entry, dict):
                ticker_market = entry.get("market", "")
                if ticker_market and ticker_market not in allowed_markets:
                    filtered["market"] += 1
                    continue

        universe.append(code)

    logger.info(
        f"[UNIVERSE_DB] {len(universe)}/{pre_filter_count} passed "
        f"(pref={filtered['preferred']}, market={filtered['market']})"
    )

    if len(universe) < min_count:
        logger.warning(
            f"[UNIVERSE_DB] size {len(universe)} < minimum {min_count}"
        )

    return universe


def compare_universes(csv_universe: List[str],
                      db_universe: List[str]) -> Dict[str, object]:
    """R4 Stage 1: compute diff metrics between CSV and DB universes.

    Used by batch step2 for shadow comparison logging. Structure matches
    Jeff spec in work_plan_20260423.md §R4 원칙:
      csv_count, db_count, only_csv_count, only_db_count, diff_pct

    Returns dict ready for logging/marker metadata.
    """
    csv_set = set(csv_universe or [])
    db_set = set(db_universe or [])
    only_csv = csv_set - db_set
    only_db = db_set - csv_set
    total_union = csv_set | db_set
    diff = len(only_csv) + len(only_db)
    diff_pct = (diff / len(total_union) * 100.0) if total_union else 0.0
    return {
        "csv_count": len(csv_set),
        "db_count": len(db_set),
        "only_csv_count": len(only_csv),
        "only_db_count": len(only_db),
        "diff_pct": round(diff_pct, 3),
        "only_csv_sample": sorted(only_csv)[:10],
        "only_db_sample": sorted(only_db)[:10],
    }


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
