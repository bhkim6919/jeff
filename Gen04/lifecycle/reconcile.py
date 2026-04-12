"""
Broker reconciliation logic extracted from main.py.
"""
from __future__ import annotations
import logging
from datetime import date


def _reconcile_with_broker(portfolio, provider, logger, trade_logger=None,
                            buy_cost: float = 0.00115):
    """
    Sync internal state TO broker truth (limited, guarded).

    Returns:
        dict with keys:
            "ok": bool — True if sync succeeded (even if corrections were made)
            "error": str — non-empty if sync failed entirely
            "corrections": int — number of fields corrected
            "safe_mode": bool — True if excessive corrections detected
            "safe_mode_reason": str — reason for safe_mode
    """
    from core.portfolio_manager import Position

    # Safety thresholds
    MAX_RECON_CORRECTIONS = 10
    QTY_SPIKE_RATIO = 1.0    # 100% change = spike → block correction
    CASH_SPIKE_RATIO = 0.5   # 50% change = critical alert

    summary = provider.query_account_summary()
    if summary.get("error") and summary["error"] not in ("", "empty_account"):
        logger.warning(f"Broker sync failed: {summary['error']}")
        return {"ok": False, "error": summary["error"], "corrections": 0,
                "safe_mode": False, "safe_mode_reason": ""}
    if summary.get("holdings_reliable") is False:
        logger.critical(
            "[BROKER_STATE_UNRELIABLE] Holdings data unreliable (msg_rejected). "
            "Cash-only sync applied. Rebalance and new orders will be BLOCKED "
            "for this session (monitor-only).")
        broker_cash = summary.get("available_cash", 0)
        if broker_cash > 0:
            old_cash = portfolio.cash
            portfolio.cash = broker_cash
            logger.info(f"Cash synced: {old_cash:,.0f} -> {broker_cash:,.0f}")
        return {"ok": True, "error": "", "corrections": 0,
                "safe_mode": True,
                "safe_mode_reason": "holdings_unreliable — monitor-only session"}

    # ── 1. Cash: broker wins (with spike detection) ──────────────────
    corrections = 0
    correction_details = []  # (type, code, old, new)
    cash_spike = False
    broker_cash = summary.get("available_cash", 0)
    old_cash = portfolio.cash
    if old_cash > 0 and broker_cash > 0:
        cash_change_ratio = abs(broker_cash - old_cash) / old_cash
        if cash_change_ratio > CASH_SPIKE_RATIO:
            cash_spike = True
            logger.critical(
                f"[RECON_CASH_SPIKE] {old_cash:,.0f} -> {broker_cash:,.0f} "
                f"({cash_change_ratio * 100:.0f}% change)")
    portfolio.cash = broker_cash  # always apply (broker authoritative for margin)
    if old_cash != broker_cash:
        corrections += 1
        correction_details.append(("CASH", "-", f"{old_cash:,.0f}", f"{broker_cash:,.0f}"))
        logger.info(f"[RECON] Cash synced: {old_cash:,.0f} -> {broker_cash:,.0f}")

    # ── 2. Holdings: broker is truth (with guards) ──────────────────
    broker_holdings = {h["code"]: h for h in summary.get("holdings", [])}
    internal_codes = set(portfolio.positions.keys())
    broker_codes = set(broker_holdings.keys())

    # 2a. ENGINE-ONLY → remove (broker sold or never held)
    for code in internal_codes - broker_codes:
        corrections += 1
        old_pos = portfolio.positions[code]
        correction_details.append(("ENGINE_ONLY", code, str(old_pos.quantity), "0"))
        logger.warning(f"[RECON] Removing ENGINE-ONLY position {code}")
        if trade_logger:
            trade_logger.log_reconcile(
                code, "ENGINE_ONLY",
                engine_qty=old_pos.quantity, broker_qty=0,
                engine_avg=old_pos.avg_price, broker_avg=0,
                resolution="REMOVED")
        del portfolio.positions[code]

    # 2b. BROKER-ONLY → add to engine (manual buy or missed fill)
    today = str(date.today())
    for code in broker_codes - internal_codes:
        h = broker_holdings[code]
        corrections += 1
        pos = Position(
            code=code,
            quantity=h["qty"],
            avg_price=h["avg_price"],
            entry_date=today,
            high_watermark=h.get("cur_price", h["avg_price"]),
            current_price=h.get("cur_price", 0),
            invested_total=h["qty"] * h["avg_price"] * (1 + buy_cost),
        )
        portfolio.positions[code] = pos
        correction_details.append(("BROKER_ONLY", code, "0", str(h["qty"])))
        logger.warning(f"[RECON_BROKER_ONLY] Added {code}: "
                       f"qty={h['qty']}, avg={h['avg_price']:,.0f} — "
                       f"verify: manual buy or missed fill?")
        if trade_logger:
            trade_logger.log_reconcile(
                code, "BROKER_ONLY",
                engine_qty=0, broker_qty=h["qty"],
                engine_avg=0, broker_avg=h["avg_price"],
                resolution="ADDED")

    # 2c. BOTH → sync qty, avg_price, current_price from broker
    for code in internal_codes & broker_codes:
        pos = portfolio.positions[code]
        h = broker_holdings[code]
        brk_qty = h["qty"]
        brk_avg = h["avg_price"]
        brk_cur = h.get("cur_price", 0)

        if pos.quantity != brk_qty:
            old_qty = pos.quantity
            ratio = abs(brk_qty - old_qty) / max(old_qty, 1)
            if ratio > QTY_SPIKE_RATIO and old_qty > 0:
                # QTY spike — skip correction, manual review required
                corrections += 1
                correction_details.append(("QTY_SPIKE_BLOCKED", code, str(old_qty), str(brk_qty)))
                logger.critical(
                    "[RECON_QTY_SPIKE] %s: %d -> %d (%.0f%% change) — "
                    "SKIPPED, manual review required!",
                    code, old_qty, brk_qty, ratio * 100)
                if trade_logger:
                    trade_logger.log_reconcile(
                        code, "QTY_SPIKE_BLOCKED",
                        engine_qty=old_qty, broker_qty=brk_qty,
                        engine_avg=pos.avg_price, broker_avg=brk_avg,
                        resolution="SKIPPED_MANUAL_REVIEW")
            else:
                corrections += 1
                correction_details.append(("QTY_FIX", code, str(pos.quantity), str(brk_qty)))
                logger.warning(f"[RECON] QTY fix {code}: {pos.quantity} -> {brk_qty}")
                if trade_logger:
                    trade_logger.log_reconcile(
                        code, "QTY_MISMATCH",
                        engine_qty=pos.quantity, broker_qty=brk_qty,
                        engine_avg=pos.avg_price, broker_avg=brk_avg,
                        resolution="SYNCED_TO_BROKER")
                pos.quantity = brk_qty
        if brk_avg > 0 and pos.avg_price != brk_avg:
            corrections += 1
            correction_details.append(("AVG_FIX", code, f"{pos.avg_price:,.0f}", f"{brk_avg:,.0f}"))
            logger.info(f"[RECON] AvgPrice fix {code}: {pos.avg_price:,.0f} -> {brk_avg:,.0f}")
            if trade_logger:
                trade_logger.log_reconcile(
                    code, "AVG_MISMATCH",
                    engine_qty=pos.quantity, broker_qty=brk_qty,
                    engine_avg=pos.avg_price, broker_avg=brk_avg,
                    resolution="SYNCED_TO_BROKER")
            pos.avg_price = brk_avg
        if brk_cur > 0:
            pos.current_price = brk_cur
            pos.high_watermark = max(pos.high_watermark, brk_cur)

    # ── 3. Safety evaluation ──────────────────────────────────────────
    synced = len(broker_holdings)
    safe_mode = False
    safe_mode_reason = ""

    if corrections > MAX_RECON_CORRECTIONS:
        safe_mode = True
        safe_mode_reason = (f"[RECON] {corrections} corrections exceed limit "
                            f"{MAX_RECON_CORRECTIONS}")
        logger.critical(
            "[RECON_SAFETY] %d corrections exceed limit %d — SAFE_MODE recommended",
            corrections, MAX_RECON_CORRECTIONS)

    if cash_spike and corrections > 5:
        safe_mode = True
        reason_part = f"[RECON] cash spike + {corrections} corrections"
        safe_mode_reason = f"{safe_mode_reason}; {reason_part}" if safe_mode_reason else reason_part
        logger.critical("[RECON_SAFETY] Cash spike combined with %d corrections — SAFE_MODE",
                        corrections)

    if corrections > 0:
        logger.warning(f"[RECON] {corrections} corrections applied:")
        for ctype, ccode, cold, cnew in correction_details:
            logger.warning(f"  [{ctype}] {ccode}: {cold} -> {cnew}")
    logger.info(f"[RECON] Done — cash={broker_cash:,.0f}, positions={synced}")

    # Log equity divergence for manual review (no auto-correction)
    if corrections > 0:
        current_equity = portfolio.cash + sum(
            p.quantity * p.avg_price for p in portfolio.positions.values())
        divergence = (abs(portfolio.prev_close_equity - current_equity)
                      / max(current_equity, 1))
        if divergence > 0.3:
            logger.warning(
                "[RECON_EQUITY_DIVERGENCE] prev_close=%s current=%s "
                "divergence=%.1f%% — NOT auto-correcting, manual review needed",
                f"{portfolio.prev_close_equity:,.0f}",
                f"{current_equity:,.0f}", divergence * 100)

    return {"ok": True, "error": "", "corrections": corrections,
            "safe_mode": safe_mode, "safe_mode_reason": safe_mode_reason,
            "correction_details": correction_details}
