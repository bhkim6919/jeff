"""
lifecycle/context.py — LiveContext dataclass
=============================================
Shared session state for Gen4 live mode.

LiveContext bundles all variables that run_live() currently passes between
helper functions via positional/keyword args.  Extracting them into a single
dataclass makes the dependency graph explicit and enables future refactors
(e.g. splitting run_live into phase functions) without signature churn.

This file is PURE DATA — no business logic, no imports beyond stdlib + typing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# BUY may be blocked, SELL always allowed — enforced by guard + executor, not context
# RECON 중 주문 발행 금지 — recon_complete=False 동안 executor는 주문 거부


@dataclass
class LiveContext:
    """Shared mutable state bag for a single live trading session.

    Grouped by lifecycle:
      - Immutable (session lifetime): set once at startup, never mutated.
      - Core services: injected broker/engine objects.
      - Auxiliary: caches, helpers.
      - Regime: observation-only market state (no trading logic impact).
      - Session flags: mutable booleans/counters toggled during the session.
      - Collectors: Phase 2.5 microstructure / intraday objects.
      - Phase gate: current execution phase for ordering guarantees.
    """

    # ── Immutable (session lifetime) ──────────────────────────────────
    config: Any
    trading_mode: str       # "live"|"paper"|"paper_test"|"shadow_test"
    mode_label: str         # UPPER version
    server_type: str        # "MOCK"|"REAL"

    # ── Core services ─────────────────────────────────────────────────
    provider: Any           # BrokerProvider
    portfolio: Any          # PortfolioManager
    state_mgr: Any          # StateManager
    executor: Any           # OrderExecutor
    tracker: Any            # OrderTracker
    guard: Any              # ExposureGuard
    trade_logger: Any       # TradeLogger

    # ── Auxiliary ─────────────────────────────────────────────────────
    name_cache: dict = field(default_factory=dict)

    # ── Regime (observation) ──────────────────────────────────────────
    session_regime: str = ""
    session_kospi_ma200: float = 0.0
    session_breadth: float = 0.0

    # ── Session flags (mutable) ───────────────────────────────────────
    recovery_ok: bool = True
    rebalance_executed: bool = False
    price_fail_count: int = 0
    monitor_only: bool = False
    reconcile_corrections: int = 0
    stop_requested: bool = False
    dirty_exit: bool = False

    # ── Collectors (set in Phase 2.5) ─────────────────────────────────
    collector: Any = None
    kospi_collector: Any = None
    swing_collector: Any = None
    micro_collector: Any = None

    # ── Phase gate ────────────────────────────────────────────────────
    current_phase: str = "INIT"  # INIT/STARTUP/RECON/PENDING_BUY/REBALANCE/MONITOR/EOD
    recon_complete: bool = False  # True after RECON — orders allowed only after this
