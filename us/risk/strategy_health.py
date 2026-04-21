"""
strategy_health.py — US strategy health (advisory stub)
=======================================================
Returns minimal health dict consumed by execution_guard_hook / auto_trading_gate.
Current P2 policy: advisory-only (no block). Full implementation pending
Phase 5 diagnostics layer.

Contract (inferred from call sites):
    compute_strategy_health(equity_dd_pct: float) -> dict
        status: "HEALTHY" | "DEGRADED" | "UNHEALTHY"
        equity_dd_pct: float
"""
from __future__ import annotations
from typing import Dict, Any


def compute_strategy_health(equity_dd_pct: float = 0.0) -> Dict[str, Any]:
    dd = float(equity_dd_pct or 0.0)
    if dd <= -7.0:
        status = "UNHEALTHY"
    elif dd <= -4.0:
        status = "DEGRADED"
    else:
        status = "HEALTHY"
    return {
        "status": status,
        "equity_dd_pct": dd,
    }
