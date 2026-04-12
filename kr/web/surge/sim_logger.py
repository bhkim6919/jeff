# -*- coding: utf-8 -*-
"""
sim_logger.py -- Structured Surge Simulator Logger
====================================================
14 log tags + ring buffer for SSE + file flush.
"""
from __future__ import annotations

import csv
import logging
import time
from collections import deque
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.rest.surge")


class SurgeLogTag(str, Enum):
    SURGE_TR_RECEIVED = "SURGE_TR_RECEIVED"
    SURGE_CANDIDATE = "SURGE_CANDIDATE"
    HOGA_SNAPSHOT = "HOGA_SNAPSHOT"
    ENTRY_CHECK = "ENTRY_CHECK"
    ENTRY_BLOCKED = "ENTRY_BLOCKED"
    ENTRY_SIM_FILLED = "ENTRY_SIM_FILLED"
    TP_HIT = "TP_HIT"
    SL_HIT = "SL_HIT"
    TIME_EXIT = "TIME_EXIT"
    STALE_SKIP = "STALE_SKIP"
    COOLDOWN_SKIP = "COOLDOWN_SKIP"
    DUPLICATE_BLOCK = "DUPLICATE_BLOCK"
    DAILY_STOP_TRIGGER = "DAILY_STOP_TRIGGER"
    SIM_RESULT = "SIM_RESULT"


# CSV header for decisions_sim.csv
DECISION_FIELDS = [
    "timestamp", "tag", "code", "name", "strategy_state", "trigger_reason",
    "bid", "ask", "bid_size", "ask_size", "last_price",
    "expected_fill_price", "pnl_pct", "holding_seconds",
]


class SurgeLogger:
    """Structured logger with ring buffer and CSV append."""

    def __init__(self, buffer_size: int = 500):
        self._buffer: deque = deque(maxlen=buffer_size)
        self._csv_path: Optional[Path] = None
        self._csv_writer_initialized = False

    def set_csv_path(self, path: Path) -> None:
        self._csv_path = path
        self._csv_writer_initialized = False

    def log(self, tag: SurgeLogTag, **fields: Any) -> None:
        """Log structured event to Python logger, ring buffer, and CSV."""
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:23],
            "tag": tag.value,
            "code": fields.get("code", ""),
            "name": fields.get("name", ""),
            "strategy_state": fields.get("strategy_state", ""),
            "trigger_reason": fields.get("trigger_reason", ""),
            "bid": fields.get("bid", 0),
            "ask": fields.get("ask", 0),
            "bid_size": fields.get("bid_size", 0),
            "ask_size": fields.get("ask_size", 0),
            "last_price": fields.get("last_price", 0),
            "expected_fill_price": fields.get("expected_fill_price", 0),
            "pnl_pct": fields.get("pnl_pct", 0.0),
            "holding_seconds": fields.get("holding_seconds", 0),
        }

        # Python logger
        msg_parts = [f"[{tag.value}]"]
        if entry["code"]:
            msg_parts.append(f"{entry['code']}")
        if entry["name"]:
            msg_parts.append(f"({entry['name']})")
        if entry["trigger_reason"]:
            msg_parts.append(f"reason={entry['trigger_reason']}")
        if entry["pnl_pct"]:
            msg_parts.append(f"pnl={entry['pnl_pct']:.2f}%")
        logger.info(" ".join(msg_parts))

        # Ring buffer for SSE
        self._buffer.append(entry)

        # CSV append
        self._append_csv(entry)

    def get_recent(self, n: int = 50) -> List[dict]:
        items = list(self._buffer)
        return items[-n:]

    def flush_debug_log(self, path: Path) -> None:
        """Write all buffered events to debug log file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
            writer.writeheader()
            for entry in self._buffer:
                writer.writerow({k: entry.get(k, "") for k in DECISION_FIELDS})

    def _append_csv(self, entry: dict) -> None:
        if not self._csv_path:
            return
        try:
            self._csv_path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not self._csv_writer_initialized and not self._csv_path.exists()
            with open(self._csv_path, "a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=DECISION_FIELDS)
                if write_header:
                    writer.writeheader()
                writer.writerow({k: entry.get(k, "") for k in DECISION_FIELDS})
            self._csv_writer_initialized = True
        except Exception as e:
            logger.warning(f"[SURGE_LOG] CSV append failed: {e}")

    def clear(self) -> None:
        self._buffer.clear()
        self._csv_writer_initialized = False
