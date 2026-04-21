# -*- coding: utf-8 -*-
"""kr/pipeline/schema.py — Pipeline state schema constants.

Single source of truth for status/mode enums and schema version.
Kept intentionally small and dependency-free so every module can import
it without triggering side effects.
"""
from __future__ import annotations

SCHEMA_VERSION = 1

# Step status enum (string-based for JSON portability)
STATUS_NOT_STARTED = "NOT_STARTED"
STATUS_PENDING = "PENDING"          # in-progress (background thread running)
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"          # precondition unmet, intentional no-run

ALL_STATUSES = frozenset({
    STATUS_NOT_STARTED,
    STATUS_PENDING,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_SKIPPED,
})

TERMINAL_STATUSES = frozenset({STATUS_DONE, STATUS_SKIPPED})

# Mode enum
MODE_LIVE = "live"
MODE_PAPER_FORWARD = "paper_forward"
MODE_LAB = "lab"
MODE_BACKTEST = "backtest"

ALL_MODES = frozenset({MODE_LIVE, MODE_PAPER_FORWARD, MODE_LAB, MODE_BACKTEST})

# Default step ordering. Phase 2 step wrappers will register against these names.
# Order here = default precondition chain hint, but real precondition is defined per step.
DEFAULT_STEPS = (
    "bootstrap_env",
    "ohlcv_sync",
    "batch",
    "lab_eod_kr",
    "lab_eod_us",
    "gate_observer",
    "backup",
)

# Default file name pattern for daily pipeline state
STATE_FILENAME_FMT = "state_{yyyymmdd}.json"
