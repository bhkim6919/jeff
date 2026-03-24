"""
intraday_collector.py — Real-time tick to 1-minute bar aggregator
=================================================================
Collects Kiwoom real-time ticks (FID 10=price, FID 27=volume),
aggregates into 1-minute OHLCV bars, and persists to per-stock CSVs.

CSV schema: datetime,open,high,low,close,volume,status
  - datetime: "2026-03-23 09:01"
  - status: HOLD | SOLD

Files are append-only. Data is retained after sell for post-analysis.
"""
from __future__ import annotations
import csv
import logging
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

logger = logging.getLogger("gen4.intraday")

CSV_COLUMNS = ["datetime", "open", "high", "low", "close", "volume", "status"]


class _MinuteBarAccumulator:
    """Accumulates ticks within a single minute into OHLCV."""

    __slots__ = ("open", "high", "low", "close", "volume", "tick_count")

    def __init__(self):
        self.open = 0.0
        self.high = 0.0
        self.low = float("inf")
        self.close = 0.0
        self.volume = 0
        self.tick_count = 0

    def update(self, price: float, vol: int) -> None:
        if self.tick_count == 0:
            self.open = price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += vol
        self.tick_count += 1

    def is_empty(self) -> bool:
        return self.tick_count == 0

    def to_row(self, dt_str: str, status: str) -> list:
        return [dt_str, self.open, self.high, self.low,
                self.close, self.volume, status]

    def reset(self):
        self.open = 0.0
        self.high = 0.0
        self.low = float("inf")
        self.close = 0.0
        self.volume = 0
        self.tick_count = 0


class IntradayCollector:
    """
    Aggregates Kiwoom real-time ticks into 1-minute OHLCV bars.
    Persists to per-stock CSV files in append mode.

    Usage:
        collector = IntradayCollector(config.INTRADAY_DIR, "2026-03-23")
        collector.set_active_codes(["028050", "055550", ...])
        provider.set_real_data_callback(collector.on_tick)
        # ... during monitor loop:
        collector.check_and_flush()
        # ... at EOD:
        collector.flush_all()
    """

    def __init__(self, intraday_dir: Path, today_str: str = ""):
        self._dir = Path(intraday_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._today = today_str or date.today().strftime("%Y-%m-%d")

        # Per-code accumulators: {code: _MinuteBarAccumulator}
        self._accumulators: Dict[str, _MinuteBarAccumulator] = {}

        # Active codes (currently held, receive HOLD status)
        self._active_codes: Set[str] = set()

        # Sold codes (still process remaining ticks in buffer, then stop)
        self._sold_codes: Set[str] = set()

        # Last flushed minute (to detect boundary)
        self._last_flush_minute: str = ""

        # Last known price per code (for current_price updates)
        self._last_prices: Dict[str, float] = {}

        # Thread safety (Qt callbacks may interleave with flush)
        self._lock = threading.Lock()

        # Stats
        self._tick_count = 0
        self._bar_count = 0

    # ── Public API ──────────────────────────────────────────────────────

    def set_active_codes(self, codes: List[str]) -> None:
        """Set which codes are currently held (HOLD status)."""
        with self._lock:
            self._active_codes = set(str(c).zfill(6) for c in codes)
            # Initialize accumulators for new codes
            for code in self._active_codes:
                if code not in self._accumulators:
                    self._accumulators[code] = _MinuteBarAccumulator()
        logger.info("[Intraday] Active codes: %d", len(self._active_codes))

    def mark_sold(self, code: str) -> None:
        """Mark a code as SOLD — flush remaining bar, stop collecting."""
        code = str(code).zfill(6)
        with self._lock:
            self._sold_codes.add(code)
            self._active_codes.discard(code)
            # Flush any pending bar for this code with SOLD status
            acc = self._accumulators.get(code)
            if acc and not acc.is_empty():
                minute_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                self._write_bar(code, acc.to_row(minute_str, "SOLD"))
                acc.reset()
                self._bar_count += 1
        logger.info("[Intraday] Marked SOLD: %s", code)

    def on_tick(self, code: str, price: float, volume: int) -> None:
        """
        Called from kiwoom_provider OnReceiveRealData callback.
        Accumulates tick into the current minute bar.
        """
        code = str(code).strip().zfill(6)
        if code in self._sold_codes:
            return
        if code not in self._active_codes:
            return

        with self._lock:
            acc = self._accumulators.get(code)
            if acc is None:
                acc = _MinuteBarAccumulator()
                self._accumulators[code] = acc
            acc.update(price, volume)
            self._last_prices[code] = price
            self._tick_count += 1

    def check_and_flush(self) -> None:
        """
        Called every monitor cycle (60s). Flush if minute boundary crossed.
        """
        now = datetime.now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")

        if current_minute == self._last_flush_minute:
            return

        with self._lock:
            if self._last_flush_minute:
                self._flush_bars(self._last_flush_minute)
            self._last_flush_minute = current_minute

    def flush_all(self) -> None:
        """Flush all pending bars (call at EOD)."""
        with self._lock:
            minute_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            self._flush_bars(minute_str)
        logger.info("[Intraday] EOD flush complete: %d bars, %d ticks total",
                     self._bar_count, self._tick_count)

    def get_last_prices(self) -> Dict[str, float]:
        """Return last known prices from real-time ticks."""
        with self._lock:
            return dict(self._last_prices)

    # ── Data Loading (for reports) ──────────────────────────────────────

    def load_today_bars(self, code: str) -> pd.DataFrame:
        """Load today's minute bars for a specific stock."""
        return self._load_bars(code, self._today)

    def load_all_today(self) -> Dict[str, pd.DataFrame]:
        """Load all stocks' today bars."""
        result = {}
        if not self._dir.exists():
            return result
        for csv_file in self._dir.glob("*.csv"):
            code = csv_file.stem
            df = self._load_bars(code, self._today)
            if not df.empty:
                result[code] = df
        return result

    # ── Static loaders (for standalone report generation) ───────────────

    @staticmethod
    def load_bars_for_date(intraday_dir: Path, code: str,
                           target_date: str) -> pd.DataFrame:
        """Load minute bars for a specific code and date (static)."""
        path = Path(intraday_dir) / f"{code}.csv"
        if not path.exists():
            return pd.DataFrame(columns=CSV_COLUMNS)
        try:
            df = pd.read_csv(path, encoding="utf-8-sig",
                             dtype={"status": str})
            if df.empty or "datetime" not in df.columns:
                return pd.DataFrame(columns=CSV_COLUMNS)
            # Filter by date prefix
            mask = df["datetime"].str.startswith(target_date)
            return df[mask].reset_index(drop=True)
        except Exception:
            return pd.DataFrame(columns=CSV_COLUMNS)

    @staticmethod
    def load_all_for_date(intraday_dir: Path,
                          target_date: str) -> Dict[str, pd.DataFrame]:
        """Load all stocks' bars for a date (static, for reports)."""
        result = {}
        intraday_dir = Path(intraday_dir)
        if not intraday_dir.exists():
            return result
        for csv_file in intraday_dir.glob("*.csv"):
            code = csv_file.stem
            df = IntradayCollector.load_bars_for_date(
                intraday_dir, code, target_date)
            if not df.empty:
                result[code] = df
        return result

    # ── Internal ─────────────────────────────────────────────────────────

    def _flush_bars(self, minute_str: str) -> None:
        """Flush all accumulators that have data. Must hold lock."""
        for code in list(self._accumulators.keys()):
            if code in self._sold_codes:
                continue
            acc = self._accumulators[code]
            if acc.is_empty():
                continue
            status = "HOLD" if code in self._active_codes else "SOLD"
            self._write_bar(code, acc.to_row(minute_str, status))
            acc.reset()
            self._bar_count += 1

    def _write_bar(self, code: str, row: list) -> None:
        """Append one bar to the per-stock CSV file."""
        path = self._dir / f"{code}.csv"
        try:
            need_header = not path.exists()
            with open(path, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                if need_header:
                    writer.writerow(CSV_COLUMNS)
                writer.writerow(row)
        except Exception as e:
            logger.warning("[Intraday] Write failed %s: %s", code, e)

    def _load_bars(self, code: str, target_date: str) -> pd.DataFrame:
        """Load bars for a code filtered by date."""
        return IntradayCollector.load_bars_for_date(self._dir, code, target_date)
