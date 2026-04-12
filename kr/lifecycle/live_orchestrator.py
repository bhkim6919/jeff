"""
lifecycle/live_orchestrator.py — LIVE Mode Orchestrator
========================================================
Orchestrate LIVE mode: Phase 0 -> 1 -> 1.5 -> 2 -> 3 -> 4.

Thin facade that calls phase modules in order with LiveContext.
"""
from __future__ import annotations

import logging

from lifecycle.phase_skeleton import Phase
from lifecycle.startup_phase import run_startup
from lifecycle.pending_buy_phase import run_pending_buy
from lifecycle.rebalance_phase import run_rebalance
from lifecycle.monitor_phase import run_monitor
from lifecycle.eod_phase import run_eod

logger = logging.getLogger("gen4.live")


def run_live(config) -> None:
    """Orchestrate LIVE mode: Phase 0 -> 1 -> 1.5 -> 2 -> 3 -> 4."""
    # Phase 0+1: Startup + Reconciliation
    ctx = run_startup(config)

    # Phase 1.5: Pending Buy Recovery
    ctx.current_phase = Phase.PENDING_BUY.value
    run_pending_buy(ctx)

    # Phase 2+2A: Rebalance
    ctx.current_phase = Phase.REBALANCE.value
    run_rebalance(ctx)

    # Phase 3: Monitor Loop
    ctx.current_phase = Phase.MONITOR.value
    run_monitor(ctx)

    # Phase 4: EOD
    ctx.current_phase = Phase.EOD.value
    run_eod(ctx)
