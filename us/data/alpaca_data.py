# -*- coding: utf-8 -*-
"""
alpaca_data.py — US OHLCV Data Collection
==========================================
yfinance = batch truth (scoring 기준)
Alpaca realtime = monitor only (scoring 기준 아님)
"""
from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger("qtron.us.data")


class USDataCollector:
    """US OHLCV collection: yfinance batch → DB."""

    def __init__(self, db):
        self._db = db
        # US-P0-003: expose last-run stats for downstream quality gate
        self.last_total: int = 0
        self.last_count: int = 0
        self.last_errors: int = 0
        self.last_failed_ratio: float = 0.0

    def collect_ohlcv(self, symbols: List[str], period: str = "2y") -> int:
        """Download OHLCV via yfinance → upsert to DB."""
        import yfinance as yf

        logger.info(f"[DATA] Downloading {len(symbols)} symbols ({period})...")
        data = yf.download(
            symbols,
            period=period,
            group_by="ticker",
            threads=True,
            progress=False,
        )

        self.last_total = len(symbols)
        count = 0
        errors = 0
        for sym in symbols:
            try:
                if len(symbols) == 1:
                    df = data.copy()
                else:
                    df = data[sym].copy()

                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue

                # Normalize columns
                df = df.reset_index()
                col_map = {}
                for c in df.columns:
                    cl = str(c).lower()
                    if cl in ("date", "datetime"):
                        col_map[c] = "date"
                    elif cl == "open":
                        col_map[c] = "open"
                    elif cl == "high":
                        col_map[c] = "high"
                    elif cl == "low":
                        col_map[c] = "low"
                    elif cl == "close":
                        col_map[c] = "close"
                    elif cl == "volume":
                        col_map[c] = "volume"
                df = df.rename(columns=col_map)

                needed = ["date", "open", "high", "low", "close", "volume"]
                if not all(c in df.columns for c in needed):
                    continue

                df = df[needed]
                df["date"] = pd.to_datetime(df["date"]).dt.date
                n = self._db.upsert_ohlcv(sym, df)
                count += 1
                if count % 50 == 0:
                    logger.info(f"[DATA] {count}/{len(symbols)} done...")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"[DATA] {sym}: {e}")

        self.last_count = count
        self.last_errors = errors
        self.last_failed_ratio = (errors / max(len(symbols), 1))
        logger.info(
            f"[DATA] Complete: {count}/{len(symbols)} stocks, "
            f"{errors} errors (failed_ratio={self.last_failed_ratio:.2%})")
        return count

    def collect_ohlcv_research(self, symbols: List[str], period: str = "2y",
                                universe_tag: str = "R1000") -> int:
        """Download OHLCV via yfinance → upsert to research DB (ohlcv_us_research).

        Used by Lab/Forward EOD pipeline. Separate from collect_ohlcv (operating table)
        so live trading and research universes stay decoupled. universe_tag is persisted
        per-row so a symbol can belong to R1000 or R3000 (small-cap extension).
        """
        import yfinance as yf

        if not symbols:
            logger.info(f"[RESEARCH_DATA] empty symbol list, skip (tag={universe_tag})")
            return 0

        logger.info(
            f"[RESEARCH_DATA] Downloading {len(symbols)} symbols "
            f"(period={period}, tag={universe_tag})..."
        )
        data = yf.download(
            symbols,
            period=period,
            group_by="ticker",
            threads=True,
            progress=False,
        )

        self._db.ensure_research_table()

        count = 0
        errors = 0
        for sym in symbols:
            try:
                if len(symbols) == 1:
                    df = data.copy()
                else:
                    df = data[sym].copy()

                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue

                df = df.reset_index()
                col_map = {}
                for c in df.columns:
                    cl = str(c).lower()
                    if cl in ("date", "datetime"):
                        col_map[c] = "date"
                    elif cl == "open":
                        col_map[c] = "open"
                    elif cl == "high":
                        col_map[c] = "high"
                    elif cl == "low":
                        col_map[c] = "low"
                    elif cl == "close":
                        col_map[c] = "close"
                    elif cl == "volume":
                        col_map[c] = "volume"
                df = df.rename(columns=col_map)

                needed = ["date", "open", "high", "low", "close", "volume"]
                if not all(c in df.columns for c in needed):
                    continue

                df = df[needed]
                df["date"] = pd.to_datetime(df["date"]).dt.date
                self._db.upsert_ohlcv_research(sym, df, universe_tag=universe_tag)
                count += 1
                if count % 100 == 0:
                    logger.info(f"[RESEARCH_DATA] {count}/{len(symbols)} done (tag={universe_tag})...")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.warning(f"[RESEARCH_DATA] {sym}: {e}")

        failed_ratio = errors / max(len(symbols), 1)
        logger.info(
            f"[RESEARCH_DATA] Complete ({universe_tag}): {count}/{len(symbols)} stocks, "
            f"{errors} errors (failed_ratio={failed_ratio:.2%})"
        )
        return count

    def collect_index(self, symbols: List[str] = None, period: str = "7y") -> int:
        """Download index data (SPY, QQQ, IWM) → DB."""
        import yfinance as yf

        if symbols is None:
            symbols = ["SPY", "QQQ", "IWM"]

        count = 0
        for sym in symbols:
            try:
                df = yf.download(sym, period=period, progress=False)
                if df.empty:
                    continue
                # yfinance may return MultiIndex columns for single ticker
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                col_map = {}
                for c in df.columns:
                    cl = str(c).lower().strip()
                    if cl in ("date", "datetime"):
                        col_map[c] = "date"
                    elif cl == "open":
                        col_map[c] = "open"
                    elif cl == "high":
                        col_map[c] = "high"
                    elif cl == "low":
                        col_map[c] = "low"
                    elif cl == "close":
                        col_map[c] = "close"
                    elif cl == "volume":
                        col_map[c] = "volume"
                df = df.rename(columns=col_map)
                if "date" not in df.columns:
                    # Fallback: first column is likely date
                    df = df.rename(columns={df.columns[0]: "date"})
                df["date"] = pd.to_datetime(df["date"]).dt.date
                n = self._db.upsert_index(sym, df[["date", "open", "high", "low", "close", "volume"]])
                count += 1
                logger.info(f"[INDEX] {sym}: {n} rows")
            except Exception as e:
                logger.error(f"[INDEX] {sym}: {e}")

        return count
