"""
Advisor data schemas — DailySnapshot, DataMeta, LogEvent, SnapshotWindow.
All data flows through these structures for consistency and validation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DataMeta:
    """Metadata tag for contamination prevention."""
    source: str            # "engine" | "external" | "derived"
    mode: str              # "live" | "paper" | "paper_test" | "mock"
    strategy_version: str  # "4.0" | "4.0-02"
    is_operational: bool   # True = incident/patch period (exclude from strategy analysis)
    timestamp: str         # ISO datetime


@dataclass
class LogEvent:
    """Parsed log entry."""
    timestamp: str         # "2026-04-01 09:00:03"
    level: str             # "INFO" | "WARNING" | "CRITICAL" | "ERROR"
    logger: str            # "gen4.live" | "Gen4KiwoomProvider" etc
    tag: str               # "[RECON]" | "[DD_GUARD]" | "[GHOST_FILL]" etc
    message: str           # Full message after tag
    raw: str               # Original line


@dataclass
class DailySnapshot:
    """Single trading day data, validated and normalized."""

    # ── Time reference (mandatory) ──
    trading_day: str             # "20260401"
    data_cutoff_time: str        # "2026-04-01T16:30:00"
    reference_point: str         # "EOD" only

    # ── Per-source timestamps ──
    timestamps: dict[str, str] = field(default_factory=dict)

    # ── Integrity hash ──
    snapshot_hash: str = ""

    # ── Meta ──
    meta: DataMeta = field(default_factory=lambda: DataMeta(
        source="engine", mode="paper", strategy_version="4.0",
        is_operational=False, timestamp=""))

    # ── Strategy intent ──
    config_snapshot: dict = field(default_factory=dict)
    target: Optional[dict] = None  # target_portfolio signal

    # ── Execution results ──
    equity: dict = field(default_factory=dict)
    trades: list[dict] = field(default_factory=list)
    closes: list[dict] = field(default_factory=list)
    positions: dict = field(default_factory=dict)  # code -> position dict

    # ── Operational state ──
    reconcile: list[dict] = field(default_factory=list)
    log_events: list[LogEvent] = field(default_factory=list)
    operational_flags: list[str] = field(default_factory=list)

    # ── External ──
    kospi_close: float = 0.0
    regime: str = ""


@dataclass
class SnapshotWindow:
    """Multi-day analysis window with operational gap handling."""
    snapshots: list[DailySnapshot] = field(default_factory=list)
    start_date: str = ""
    end_date: str = ""
    window_size: int = 0
    valid_mask: list[bool] = field(default_factory=list)
    valid_count: int = 0
    coverage_ratio: float = 0.0
