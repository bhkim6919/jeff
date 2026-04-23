# -*- coding: utf-8 -*-
"""
swing_collector.py — Snapshot replay data collector
=====================================================
30-min interval ranking snapshot + dynamic intraday code registration.

This is a RAW DATA COLLECTOR, not a strategy engine.
Snapshot-based replay input data only.
Do not interpret as real-time rank rotation backtest input.

CSV schema: snapshot_time,rank,code,name,price,change_pct
"""
from __future__ import annotations

import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger("gen4.swing")


class SwingRankingCollector:
    """30-min ranking snapshot collector.

    - Queries real-time ranking TR every 30 minutes
    - Appends to ranking CSV (with snapshot_time)
    - Registers new codes in IntradayCollector + MicrostructureCollector
      via public API only (no internal state access)
    """

    MAX_SEEN_CODES = 100
    SNAPSHOT_INTERVAL = 600   # 10 minutes (FIX-002: from 1800)

    def __init__(self, swing_dir, today_str: str, provider,
                 intraday_collector, micro_collector=None):
        self._swing_dir = Path(swing_dir)
        self._today_str = today_str
        self._provider = provider
        self._intraday = intraday_collector   # public methods only
        self._micro = micro_collector          # public methods only (optional)

        # Directories
        self._ranking_dir = self._swing_dir / "ranking"
        self._ranking_dir.mkdir(parents=True, exist_ok=True)
        self._ranking_file = self._ranking_dir / f"{today_str}.csv"

        # State
        self._active_codes: Set[str] = set()
        self._all_seen_codes: Set[str] = set()
        self._registered_codes: Set[str] = set()
        self._last_snapshot_time: float = 0.0
        self._snapshot_count: int = 0
        self._capped_codes_count: int = 0

        # Capability gate — REST provider lacks ranking TR; disable silently
        # rather than spamming "[SWING] snapshot error" every monitor tick.
        self._disabled = not hasattr(provider, "query_realtime_ranking")

        # CSV header
        self._ensure_csv_header()
        # Recover from restart
        self._recover_seen_codes()

        if self._disabled:
            logger.info(
                "[SwingCollector] disabled — provider %s has no "
                "query_realtime_ranking (ranking TR not available on REST)",
                type(provider).__name__)
        else:
            logger.info(
                "[SwingCollector] init: dir=%s, today=%s, recovered=%d",
                self._ranking_dir, today_str, len(self._all_seen_codes))

    # ── FIX-001: Pre-market seed ─────────────────────────────────
    def seed_pre_market(self):
        """Force immediate first snapshot + registration.

        Call ONCE right after init, before monitor loop starts.
        This ensures ranked stocks begin collecting minute bars
        from market open instead of waiting for the first
        30-min (now 10-min) scheduled snapshot.
        """
        if self._disabled:
            return
        logger.info("[SwingCollector] Pre-market seed — querying ranking...")
        self._take_snapshot()
        self._last_snapshot_time = time.time()
        logger.info(
            "[SwingCollector] Seed done: ranked=%d, registered=%d codes",
            len(self._active_codes), len(self._registered_codes))

    def _ensure_csv_header(self):
        if self._ranking_file.exists():
            try:
                first = self._ranking_file.read_text(encoding="utf-8").split("\n")[0]
                if first.startswith("snapshot_time,"):
                    return
            except Exception:
                pass
        with open(self._ranking_file, "w", encoding="utf-8") as f:
            f.write("snapshot_time,rank,code,name,price,change_pct\n")

    def _recover_seen_codes(self):
        if not self._ranking_file.exists():
            return
        try:
            with open(self._ranking_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    code = row.get("code", "").strip()
                    if code:
                        self._all_seen_codes.add(code)
            if self._all_seen_codes:
                logger.info("[SwingCollector] recovered %d seen codes",
                            len(self._all_seen_codes))
        except Exception as e:
            logger.warning("[SwingCollector] recovery failed: %s", e)

    def check_and_snapshot(self):
        """Called every monitor cycle. Takes snapshot if 30min elapsed."""
        if self._disabled:
            return
        now = time.time()
        if self._last_snapshot_time and \
           (now - self._last_snapshot_time) < self.SNAPSHOT_INTERVAL:
            return
        self._take_snapshot()
        self._last_snapshot_time = now

    def _take_snapshot(self):
        if self._disabled:
            return
        rankings = self._provider.query_realtime_ranking(top_n=20)
        if not rankings:
            logger.warning("[SwingCollector] snapshot empty (TR failed)")
            return

        # 1. CSV append
        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            with open(self._ranking_file, "a", encoding="utf-8") as f:
                for r in rankings:
                    f.write(
                        f"{timestamp},{r['rank']},{r['code']},{r['name']},"
                        f"{r['price']},{r['change_pct']}\n")
        except Exception as e:
            logger.warning("[SwingCollector] CSV write failed: %s", e)
            return

        self._snapshot_count += 1
        self._active_codes = set(r["code"] for r in rankings)

        # 2. New codes — cap check
        new_codes = self._active_codes - self._all_seen_codes
        if new_codes:
            available = self.MAX_SEEN_CODES - len(self._all_seen_codes)
            if available <= 0:
                self._capped_codes_count += len(new_codes)
                logger.warning(
                    "[SwingCollector] CAP=%d reached, ignoring %d: %s",
                    self.MAX_SEEN_CODES, len(new_codes),
                    list(new_codes)[:5])
                new_codes = set()
            elif len(new_codes) > available:
                dropped = list(new_codes)[available:]
                new_codes = set(list(new_codes)[:available])
                self._capped_codes_count += len(dropped)
                logger.warning(
                    "[SwingCollector] CAP partial: +%d, dropped %d",
                    len(new_codes), len(dropped))

        if new_codes:
            new_list = list(new_codes)
            # IntradayCollector — public API
            self._intraday.add_active_codes(new_list)
            # MicrostructureCollector — public API (optional)
            if self._micro:
                self._micro.add_active_codes(new_list)
            # SetRealReg — swing screen (분봉용)
            to_register = [c for c in new_list if c not in self._registered_codes]
            if to_register:
                n = self._provider.register_real_append(
                    to_register,
                    fids=self._provider.SWING_FIDS,
                    screen=self._provider.SCREEN_SWING_REAL)
                if n:
                    self._registered_codes.update(to_register)
                    logger.info(
                        "[SwingCollector] Registered %d codes for real-time: %s",
                        len(to_register), to_register[:5])
                else:
                    logger.warning(
                        "[SwingCollector] register_real_append returned 0 "
                        "for %d codes: %s", len(to_register), to_register[:5])
            self._all_seen_codes |= new_codes

        logger.info(
            "[SwingCollector] snapshot #%d at %s: ranked=%d, new=%d, "
            "total_seen=%d",
            self._snapshot_count, timestamp, len(rankings),
            len(new_codes), len(self._all_seen_codes))

    def flush(self):
        """EOD cleanup + stats log + data coverage report (FIX-005)."""
        logger.info(
            "[SwingCollector] EOD: snapshots=%d, seen=%d, "
            "registered=%d, capped=%d, last=%s",
            self._snapshot_count, len(self._all_seen_codes),
            len(self._registered_codes), self._capped_codes_count,
            datetime.fromtimestamp(self._last_snapshot_time).strftime(
                "%H:%M:%S") if self._last_snapshot_time else "NEVER")

        # FIX-005: Data coverage report
        intraday_dir = self._swing_dir.parent / "intraday"
        date_prefix = (self._today_str[:4] + "-"
                       + self._today_str[4:6] + "-"
                       + self._today_str[6:])
        coverage = {}
        for code in self._all_seen_codes:
            csv_path = intraday_dir / f"{code}.csv"
            bar_count = 0
            if csv_path.exists():
                try:
                    with open(csv_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if date_prefix in line:
                                bar_count += 1
                except Exception:
                    pass
            coverage[code] = bar_count
        total = len(coverage)
        with_data = sum(1 for n in coverage.values() if n >= 10)
        zero_codes = [c for c, n in coverage.items() if n == 0]
        logger.info(
            "[SwingCollector] COVERAGE: %d/%d stocks have >=10 bars. "
            "Zero-bar codes(%d): %s",
            with_data, total, len(zero_codes), zero_codes[:10])

        # Unregister swing screen
        self._provider.unregister_real_screen(
            self._provider.SCREEN_SWING_REAL)
