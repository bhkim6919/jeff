# -*- coding: utf-8 -*-
"""
snapshot_guard.py — Snapshot Consistency Enforcement
=====================================================
Scoring과 execution이 동일 snapshot 기준인지 검증.
불일치 시 주문 차단 + 텔레그램 알림.

적용 위치:
1. rebalancer — compute_orders() 호출 전
2. main — execution 직전
3. rebalance_phase — rebalance 시작 시
"""
from __future__ import annotations

import logging
from datetime import date, datetime

logger = logging.getLogger("qtron.us.snapshot")


def make_snapshot_id(market: str, dt: str = "") -> str:
    """Generate snapshot_id: MARKET_YYYYMMDD."""
    if not dt:
        dt = date.today().isoformat()
    return f"{market}_{dt.replace('-', '')}"


def assert_snapshot_consistency(scoring_snapshot_id: str,
                                 execution_snapshot_id: str,
                                 market: str = "US") -> bool:
    """
    Verify scoring and execution use the same snapshot.
    Returns False (BLOCK) on mismatch.
    """
    if scoring_snapshot_id != execution_snapshot_id:
        logger.critical(
            f"[SNAPSHOT_MISMATCH] market={market} "
            f"scoring={scoring_snapshot_id} execution={execution_snapshot_id}"
        )
        # Telegram alert
        try:
            from notify.telegram_bot import send
            send(
                f"<b>SNAPSHOT MISMATCH</b>\n"
                f"Market: {market}\n"
                f"Scoring: {scoring_snapshot_id}\n"
                f"Execution: {execution_snapshot_id}\n"
                f"→ Orders BLOCKED",
                "CRITICAL"
            )
        except Exception:
            pass
        return False

    logger.info(f"[SNAPSHOT_OK] {market}: {scoring_snapshot_id}")
    return True
