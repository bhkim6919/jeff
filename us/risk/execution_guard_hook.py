"""
execution_guard_hook.py — BUY gate hook (advisory stub)
=======================================================
P2.4 Auto Trading Gate hook. Current policy is advisory-only (enforce=OFF)
per p2_advisory_observation memo: observation phase, no block yet.

This stub returns enabled=False + block_buy=False so callers log
[BUY_ADVISORY] without altering execution. Swap in enforce logic
when Phase 5 diagnostics + enforcement criteria are agreed.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class GuardDecision:
    block_buy: bool = False
    enabled: bool = False        # enforce flag; False = advisory
    mode: str = "advisory"       # "advisory" | "enforce"
    highest_blocker: str = "NONE"
    reason: str = "advisory_stub"
    buy_scale: float = 1.0


def guard_buy_execution(
    runtime: Optional[Dict[str, Any]] = None,
    strategy_health: Optional[Dict[str, Any]] = None,
) -> GuardDecision:
    rt = runtime or {}
    health = strategy_health or {}
    enforce = bool(rt.get("auto_gate_enforce", False))

    health_status = str(health.get("status", "HEALTHY")).upper()
    if health_status == "UNHEALTHY":
        top = "STRATEGY_UNHEALTHY"
    elif rt.get("data_stale"):
        top = "DATA_STALE"
    elif rt.get("last_recon_ok") is False:
        top = "RECON_FAIL"
    else:
        top = "NONE"

    if enforce and top != "NONE":
        return GuardDecision(
            block_buy=True, enabled=True, mode="enforce",
            highest_blocker=top, reason=f"enforce_blocked:{top}",
            buy_scale=0.0,
        )

    return GuardDecision(
        block_buy=False, enabled=enforce,
        mode="enforce" if enforce else "advisory",
        highest_blocker=top,
        reason="advisory_stub" if not enforce else "enforce_allowed",
        buy_scale=1.0,
    )
