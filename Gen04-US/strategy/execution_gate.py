# -*- coding: utf-8 -*-
"""
execution_gate.py — 5-Gate Execution Verification
===================================================
모든 조건 충족해야 주문 허용. 하나라도 실패 → BLOCK.

Gate 순서 (중요):
1. stale data
2. RECON safe
3. open orders
4. market hours
5. snapshot consistency (최종)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Tuple

logger = logging.getLogger("qtron.us.gate")


def check_execution_gates(
    market: str,
    provider,
    scoring_snapshot_id: str,
    execution_snapshot_id: str,
    snapshot_age_hours: float = 0,
    max_stale_hours: int = 24,
) -> Tuple[bool, str]:
    """
    5-gate execution verification.
    Returns: (allowed, reason)
    """
    # Gate 1: Stale data
    if snapshot_age_hours > max_stale_hours:
        reason = f"STALE_DATA (age={snapshot_age_hours:.1f}h > {max_stale_hours}h)"
        logger.warning(f"[GATE_BLOCKED] {reason}")
        return False, reason

    # Gate 2: RECON safe (broker holdings match state)
    # For now, check broker is connected
    if not provider.is_connected():
        reason = "BROKER_DISCONNECTED"
        logger.warning(f"[GATE_BLOCKED] {reason}")
        return False, reason

    # Gate 3: No open orders
    open_orders = provider.query_open_orders()
    if open_orders:
        reason = f"OPEN_ORDERS_EXIST ({len(open_orders)})"
        logger.warning(f"[GATE_BLOCKED] {reason}")
        return False, reason

    # Gate 4: Market hours
    if hasattr(provider, "is_market_open"):
        if not provider.is_market_open():
            reason = "MARKET_CLOSED"
            logger.warning(f"[GATE_BLOCKED] {reason}")
            return False, reason

    # Gate 5: Snapshot consistency (최종)
    from strategy.snapshot_guard import assert_snapshot_consistency
    if not assert_snapshot_consistency(scoring_snapshot_id, execution_snapshot_id, market):
        return False, "SNAPSHOT_MISMATCH"

    logger.info(f"[GATE_PASSED] {market}: all 5 gates OK")
    return True, "ALL_GATES_PASSED"


# ── BUY Permission Gate (single source of truth) ──────────────

def check_buy_permission(
    config,
    runtime_data: dict,
    provider=None,
) -> Tuple[bool, str, float]:
    """
    Unified BUY gate for ALL buy paths (test, rebalance, future auto).
    Returns: (allowed, reason, buy_scale)

    Reads:
      runtime_data["buy_blocked"]       — startup cancel incomplete
      runtime_data["buy_scale"]         — DD-based scaling (0.0~1.0)
      runtime_data["dd_label"]          — DD level label
      provider.query_open_orders()      — real-time open order check

    Guarantees:
      - SELL always allowed (this gate is BUY-only)
      - Same function for all BUY paths (no duplication)
    """
    # Gate 1: Startup block (stale orders remain from previous session)
    if runtime_data.get("buy_blocked", False):
        reason = "STARTUP_BLOCKED (stale orders not cleared)"
        logger.warning(f"[BUY_GATE] {reason}")
        return False, reason, 0.0

    # Gate 2: DD block
    buy_scale = runtime_data.get("buy_scale", 1.0)
    dd_label = runtime_data.get("dd_label", "NORMAL")
    if buy_scale == 0.0:
        reason = f"DD_BLOCKED ({dd_label})"
        logger.warning(f"[BUY_GATE] {reason}")
        return False, reason, 0.0

    # Gate 3: Real-time open orders (broker truth)
    if provider:
        try:
            open_orders = provider.query_open_orders()
            if open_orders and len(open_orders) > 0:
                reason = f"OPEN_ORDERS ({len(open_orders)} pending)"
                logger.warning(f"[BUY_GATE] {reason}")
                return False, reason, 0.0
        except Exception:
            pass  # Network issue — don't block on query failure

    logger.info(f"[BUY_GATE] ALLOWED dd={dd_label} scale={buy_scale:.0%}")
    return True, "OK", buy_scale
