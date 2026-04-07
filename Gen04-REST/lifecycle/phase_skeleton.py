"""
phase_skeleton.py — Phase Gate Definitions
==========================================
Defines the execution phases and gate conditions for Gen4 LIVE mode.

Phase order (strict sequential):
  Phase 0  STARTUP     — Provider init, login
  Phase 1  RECON       — State restore, broker reconciliation
  Phase 1.5 PENDING_BUY — Pending buy recovery, pending external
  Phase 2  REBALANCE   — Rebalance check + execution
  Phase 3  MONITOR     — 60s loop, trail warnings, real-time
  Phase 4  EOD         — Trail stop execution, reports, shutdown

Safety rules enforced by phase gates:
  - BUY may be blocked (by guard), SELL always allowed
  - RECON 중 주문 발행 금지 — orders blocked until Phase 1 complete
  - Phase transitions are one-way (no backward transitions)
"""
from __future__ import annotations
from enum import Enum

class Phase(Enum):
    INIT = "INIT"
    STARTUP = "STARTUP"           # Phase 0
    RECON = "RECON"               # Phase 1
    PENDING_BUY = "PENDING_BUY"   # Phase 1.5
    REBALANCE = "REBALANCE"       # Phase 2
    MONITOR = "MONITOR"           # Phase 3
    EOD = "EOD"                   # Phase 4
    SHUTDOWN = "SHUTDOWN"

# Phase order for validation
_PHASE_ORDER = [Phase.INIT, Phase.STARTUP, Phase.RECON, Phase.PENDING_BUY,
                Phase.REBALANCE, Phase.MONITOR, Phase.EOD, Phase.SHUTDOWN]

def can_transition(current: Phase, target: Phase) -> bool:
    """Only forward transitions allowed."""
    try:
        cur_idx = _PHASE_ORDER.index(current)
        tgt_idx = _PHASE_ORDER.index(target)
        return tgt_idx > cur_idx
    except ValueError:
        return False

def is_order_allowed(phase: Phase, side: str = "BUY") -> bool:
    """
    Check if orders are allowed in current phase.

    SELL always allowed (CLAUDE.md rule: "SELL always allowed, BUY may be blocked")
    BUY only allowed after RECON complete (Phase 1.5+)
    No orders during STARTUP or RECON.
    """
    if side.upper() == "SELL":
        return phase not in (Phase.INIT, Phase.STARTUP, Phase.SHUTDOWN)
    # BUY
    return phase in (Phase.PENDING_BUY, Phase.REBALANCE, Phase.MONITOR)
