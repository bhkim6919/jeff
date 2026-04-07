# -*- coding: utf-8 -*-
"""
microstructure_collector.py — 5-second slot-based orderbook sampler
====================================================================
Collects bid/ask depth + trade data at 5-second intervals.

This is a RAW DATA COLLECTOR, not a strategy engine.
- Tick storage prohibited — 5-second sampling only
- Derived indicators prohibited — raw only (GUI computes imbalance, spread, VWAP)
- Noisy data — never use as standalone signal

CSV schema:
    timestamp,code,price,best_ask,best_bid,
    ask_qty_1,bid_qty_1,total_ask,total_bid,net_bid,volume
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

logger = logging.getLogger("gen4.micro")


class MicrostructureCollector:
    """5-second time-slot orderbook + trade sampler.

    Key design:
    - Time-slot based: int(time/5) ensures exact 5-second intervals, no drift
    - Copy-then-clear: flush atomicity, no tick loss during write
    - File handles kept open: no open/close overhead per sample
    - Periodic fh.flush(): crash tolerance (every 100 samples)
    - Tick timestamp preserved: flush-time != data-time
    - Reverse-tick ignored: timestamp monotonicity guaranteed
    """

    SAMPLING_INTERVAL = 5.0   # seconds
    MAX_CODES = 60
    FILE_FLUSH_INTERVAL = 100  # samples between forced disk flush

    def __init__(self, micro_dir, today_str: str, provider):
        self._micro_dir = Path(micro_dir)
        self._micro_dir.mkdir(parents=True, exist_ok=True)
        self._today_str = today_str
        self._provider = provider

        self._active_codes: Set[str] = set()
        self._registered_codes: Set[str] = set()
        self._buffer: Dict[str, dict] = {}    # {code: latest fid_data}
        self._last_slot: int = 0               # time-slot ID
        self._sample_count: int = 0
        self._lock = threading.Lock()
        self._file_handles: Dict[str, object] = {}  # {code: file_handle}

        logger.info("[Micro] init: dir=%s, today=%s", self._micro_dir, today_str)

    # ── Public API ──────────────────────────────────────────────────────

    def add_active_codes(self, codes: list) -> int:
        """Add codes via public API. Registers on SCREEN_MICRO_REAL.
        Returns count of newly added codes."""
        new_reg = []
        with self._lock:
            added = 0
            for code in codes:
                code = str(code).zfill(6)
                if code in self._active_codes:
                    continue
                if len(self._active_codes) >= self.MAX_CODES:
                    logger.warning("[Micro] CAP=%d, skip %s", self.MAX_CODES, code)
                    continue
                self._active_codes.add(code)
                self._ensure_file(code)
                added += 1

            # Collect codes to register (outside lock to avoid deadlock
            # with on_real_data callback)
            new_reg = [c for c in self._active_codes
                       if c not in self._registered_codes]

        # Register OUTSIDE lock — SetRealReg can trigger immediate
        # OnReceiveRealData callback which needs self._lock
        if new_reg:
            n = self._provider.register_real_append(
                new_reg,
                fids=self._provider.MICRO_FIDS,
                screen=self._provider.SCREEN_MICRO_REAL)
            if n:
                with self._lock:
                    self._registered_codes.update(new_reg)
        return added

    def on_real_data(self, code: str, fid_data: dict):
        """Real-time callback — update buffer only (no file I/O).
        Ignores reverse-timestamp ticks."""
        if fid_data is None:
            return
        code = str(code).strip().zfill(6)
        with self._lock:
            if code not in self._active_codes:
                return
            # Reverse-tick check
            ts = fid_data.get("timestamp", "")
            prev = self._buffer.get(code, {}).get("timestamp", "")
            if ts and prev and ts < prev:
                return
            self._buffer[code] = fid_data

    def check_and_sample(self):
        """Called every monitor cycle. Flushes only on slot boundary."""
        current_slot = int(time.time() / self.SAMPLING_INTERVAL)
        if current_slot == self._last_slot:
            return
        self._flush_buffer()
        self._last_slot = current_slot

    # ── Internal ─────────────────────────────────────────────────────────

    def _flush_buffer(self):
        """Slot boundary: copy-then-clear + write."""
        with self._lock:
            buffer_copy = dict(self._buffer)
            self._buffer.clear()
        # I/O outside lock
        for code, data in buffer_copy.items():
            ts = data.get("timestamp",
                          datetime.now().strftime("%H:%M:%S"))
            self._write_sample(code, ts, data)
        self._sample_count += 1
        # Periodic disk flush
        if self._sample_count % self.FILE_FLUSH_INTERVAL == 0:
            for fh in self._file_handles.values():
                try:
                    fh.flush()
                except Exception:
                    pass

    def _ensure_file(self, code: str):
        """Create CSV + file handle if not exists."""
        if code in self._file_handles:
            return
        path = self._micro_dir / f"{code}_{self._today_str}.csv"
        is_new = not path.exists()
        try:
            fh = open(path, "a", encoding="utf-8", buffering=8192)
            if is_new:
                fh.write(
                    "timestamp,code,price,best_ask,best_bid,"
                    "ask_qty_1,bid_qty_1,total_ask,total_bid,"
                    "net_bid,volume\n")
            self._file_handles[code] = fh
        except Exception as e:
            logger.warning("[Micro] file create failed %s: %s", code, e)

    def _write_sample(self, code: str, timestamp: str, data: dict):
        """Write one sample row (file handle kept open)."""
        fh = self._file_handles.get(code)
        if not fh:
            return
        try:
            fh.write(
                f"{timestamp},{code},"
                f"{data.get('price', 0)},"
                f"{data.get('best_ask', 0)},"
                f"{data.get('best_bid', 0)},"
                f"{data.get('ask_qty_1', 0)},"
                f"{data.get('bid_qty_1', 0)},"
                f"{data.get('total_ask', 0)},"
                f"{data.get('total_bid', 0)},"
                f"{data.get('net_bid', 0)},"
                f"{data.get('volume', 0)}\n")
        except Exception as e:
            logger.warning("[Micro] write %s: %s", code, e)

    def flush(self):
        """EOD: flush remaining buffer + close handles + stats."""
        self._flush_buffer()
        for code, fh in self._file_handles.items():
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
        self._file_handles.clear()
        # Unregister micro screen
        self._provider.unregister_real_screen(
            self._provider.SCREEN_MICRO_REAL)
        file_count = len(list(
            self._micro_dir.glob(f"*_{self._today_str}.csv")))
        logger.info(
            "[Micro] EOD: codes=%d, registered=%d, samples=%d, files=%d",
            len(self._active_codes), len(self._registered_codes),
            self._sample_count, file_count)
