"""
auto_trading_gate.py — Auto trading state (advisory stub)
=========================================================
Consumed by /api/status/summary to surface auto_trading block info on UI.
Current P2 stance: advisory-only, no enforcement.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .execution_guard_hook import guard_buy_execution


@dataclass
class AutoTradingState:
    enabled: bool = False
    blockers: List[str] = field(default_factory=list)
    risk_level: str = "NORMAL"
    strategy_health: str = "HEALTHY"
    buy_scale: float = 1.0
    reason_summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "blockers": list(self.blockers),
            "risk_level": self.risk_level,
            "strategy_health": self.strategy_health,
            "buy_scale": self.buy_scale,
            "reason_summary": self.reason_summary,
        }


def compute_auto_trading_state(
    runtime: Optional[Dict[str, Any]] = None,
    strategy_health: Optional[Dict[str, Any]] = None,
) -> AutoTradingState:
    rt = runtime or {}
    health = strategy_health or {}
    decision = guard_buy_execution(runtime=rt, strategy_health=health)

    blockers: List[str] = []
    if decision.highest_blocker and decision.highest_blocker != "NONE":
        blockers.append(decision.highest_blocker)

    health_status = str(health.get("status", "HEALTHY")).upper()
    dd = float(rt.get("equity_dd_pct", 0.0) or 0.0)
    if dd <= -7.0:
        risk_level = "HIGH"
    elif dd <= -4.0:
        risk_level = "ELEVATED"
    else:
        risk_level = "NORMAL"

    return AutoTradingState(
        enabled=decision.enabled,
        blockers=blockers,
        risk_level=risk_level,
        strategy_health=health_status,
        buy_scale=decision.buy_scale,
        reason_summary=decision.reason,
    )
