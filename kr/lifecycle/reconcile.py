"""
Broker reconciliation logic extracted from main.py.
"""
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta


def _reconcile_with_broker(portfolio, provider, logger, trade_logger=None,
                            buy_cost: float = 0.00115, guard=None,
                            saved_state: dict | None = None):
    """
    Sync internal state TO broker truth (limited, guarded).

    Args:
        saved_state: Pre-RECON snapshot from ``state_mgr.load_portfolio()`` —
            used by the BROKER_ONLY branch to preserve a position's
            historical ``entry_date`` when the engine temporarily lost
            track of it. Without this, a hiccup that briefly empties the
            engine's portfolio (e.g. the 2026-04-30 incident where the
            entire ``portfolio_state_live.json`` vanished overnight)
            resets every position's entry_date to ``today``, losing
            trail-stop day counters and 21-day cycle tracking. ``None``
            falls back to the pre-fix behaviour (entry_date=today for
            every BROKER_ONLY add) — only callers that have a stable
            disk-side snapshot should pass this.

    Returns:
        dict with keys:
            "ok": bool — True if sync succeeded. False on API fail or PARTIAL holdings.
            "error": str — "holdings_unreliable" on PARTIAL; upstream error otherwise.
            "corrections": int — number of APPLIED corrections (excludes QTY_SPIKE_BLOCKED)
            "spike_blocked_count": int — count of QTY_SPIKE cases (symbol-isolated, not applied)
            "safe_mode": bool — True if excessive corrections detected
            "safe_mode_reason": str — reason for safe_mode
            "correction_details": list — audit trail of every detected mismatch
    """
    from core.portfolio_manager import Position

    # Safety thresholds
    MAX_RECON_CORRECTIONS = 10
    QTY_SPIKE_RATIO = 1.0    # 100% change = spike → block correction
    CASH_SPIKE_RATIO = 0.5   # 50% change = critical alert

    summary = provider.query_account_summary()

    # REST latency/status → exposure_guard 반영
    if guard and hasattr(guard, 'update_rest_latency'):
        from web.api_state import tracker as api_tracker
        # PaginatedResult 메타데이터에서 추출
        _elapsed = summary.get("_batch_end_ts", 0) - summary.get("_snapshot_ts", 0)
        _latency_ms = _elapsed * 1000 if _elapsed > 0 else 0
        guard.update_rest_latency(
            latency_ms=_latency_ms,
            status=summary.get("_status", "COMPLETE"),
            consistency=summary.get("_consistency", "CLEAN"),
        )

    if summary.get("error") and summary["error"] not in ("", "empty_account"):
        logger.warning(f"Broker sync failed: {summary['error']}")
        return {"ok": False, "error": summary["error"], "corrections": 0,
                "spike_blocked_count": 0,
                "safe_mode": False, "safe_mode_reason": ""}
    # ── PARTIAL snapshot: return ok=False so caller forces monitor-only ──
    # (was ok=True+safe_mode — caller often proceeded silently; KR-P0-002 fix)
    if summary.get("holdings_reliable") is False:
        _status = summary.get("_status", "unknown")
        _consistency = summary.get("_consistency", "unknown")
        logger.critical(
            f"[RECON_BLOCKED_PARTIAL] Holdings data unreliable. "
            f"status={_status} consistency={_consistency} "
            f"pages={summary.get('_pages_fetched', '?')} "
            f"ws_events={summary.get('_ws_events_during_batch', '?')}. "
            f"Cash-only sync applied. Rebalance and new orders will be BLOCKED "
            f"for this session (monitor-only).")
        # Cash: sync broker value if present (incl. 0 for empty/margin); None = API field missing
        broker_cash = summary.get("available_cash")
        if broker_cash is not None:
            old_cash = portfolio.cash
            portfolio.cash = broker_cash
            logger.info(f"Cash synced (PARTIAL): {old_cash:,.0f} -> {broker_cash:,.0f}")
        else:
            logger.warning("[RECON_PARTIAL_CASH_MISSING] available_cash=None, cash NOT synced")
        return {"ok": False, "error": "holdings_unreliable",
                "corrections": 0, "spike_blocked_count": 0,
                "safe_mode": True,
                "safe_mode_reason": "holdings_unreliable — monitor-only session"}

    # ── 1. Cash: broker wins (with spike detection) ──────────────────
    # broker_cash=None means API field missing (distinct from legitimate 0 / negative margin).
    corrections = 0
    spike_blocked_count = 0
    correction_details = []  # (type, code, old, new)
    cash_spike = False
    broker_cash = summary.get("available_cash")
    old_cash = portfolio.cash
    if broker_cash is None:
        logger.warning("[RECON_CASH_MISSING] available_cash=None (API field absent); "
                       "cash NOT synced")
    else:
        if old_cash > 0 and broker_cash > 0:
            cash_change_ratio = abs(broker_cash - old_cash) / old_cash
            if cash_change_ratio > CASH_SPIKE_RATIO:
                cash_spike = True
                logger.critical(
                    f"[RECON_CASH_SPIKE] {old_cash:,.0f} -> {broker_cash:,.0f} "
                    f"({cash_change_ratio * 100:.0f}% change)")
        portfolio.cash = broker_cash  # apply even if 0 (empty account / margin)
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
    #
    # entry_date preservation (Jeff 2026-04-30 — RECON BROKER_ONLY fix):
    # When the engine temporarily lost a position from in-memory state
    # but the broker still holds it, the pre-fix code created a fresh
    # Position with ``entry_date=today``. On the 04-30 incident the
    # entire portfolio_state_live.json vanished overnight, RECON re-added
    # all 17 positions, and every position's entry_date was reset to
    # 2026-04-30 — wiping trail-stop day counters and 21-day cycle
    # tracking. The fix: if the caller passed a ``saved_state`` snapshot
    # (the disk-side portfolio state captured before this RECON sweep),
    # use that ``entry_date`` for any matching code; fall back to today
    # only for genuinely new tickers.
    today = str(date.today())
    saved_positions: dict = {}
    if saved_state and isinstance(saved_state, dict):
        # ``state_mgr.load_portfolio()`` returns ``{"cash": ..., "positions": {code: {...}}}``
        saved_positions = saved_state.get("positions") or {}
    for code in broker_codes - internal_codes:
        h = broker_holdings[code]
        corrections += 1
        # Preserved entry_date if the disk snapshot saw this code
        # before; today otherwise.
        prev_entry = (
            saved_positions.get(code, {}).get("entry_date")
            if isinstance(saved_positions.get(code), dict)
            else None
        )
        # Reject obviously-bad saved values (empty string, garbage) so a
        # corrupted snapshot can't poison the new position.
        try:
            if prev_entry:
                date.fromisoformat(prev_entry)
                effective_entry = prev_entry
            else:
                effective_entry = today
        except (TypeError, ValueError):
            effective_entry = today
        pos = Position(
            code=code,
            quantity=h["qty"],
            avg_price=h["avg_price"],
            entry_date=effective_entry,
            high_watermark=h.get("cur_price", h["avg_price"]),
            current_price=h.get("cur_price", 0),
            invested_total=h["qty"] * h["avg_price"] * (1 + buy_cost),
        )
        portfolio.positions[code] = pos
        correction_details.append(("BROKER_ONLY", code, "0", str(h["qty"])))
        if effective_entry != today:
            logger.warning(
                f"[RECON_BROKER_ONLY] Added {code}: "
                f"qty={h['qty']}, avg={h['avg_price']:,.0f}, "
                f"entry_date={effective_entry} (preserved from disk) — "
                f"likely transient state loss + recovery"
            )
        else:
            logger.warning(
                f"[RECON_BROKER_ONLY] Added {code}: "
                f"qty={h['qty']}, avg={h['avg_price']:,.0f}, "
                f"entry_date={today} (no prior record) — "
                f"verify: manual buy or missed fill?"
            )
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
                # QTY spike — isolate symbol (reconcile_pending=True), do NOT count
                # toward correction threshold; trade gates (rebalance, trail stop)
                # must skip positions with reconcile_pending=True.
                spike_blocked_count += 1
                _now_iso = datetime.now().isoformat()
                try:
                    pos.reconcile_pending = True
                    # set timestamp only on first spike; preserve older ts so
                    # 24h fallback measures from the original isolation.
                    if not getattr(pos, "reconcile_pending_since", None):
                        pos.reconcile_pending_since = _now_iso
                except Exception:
                    # field may be absent in very old state; set as attribute anyway
                    setattr(pos, "reconcile_pending", True)
                    if not getattr(pos, "reconcile_pending_since", None):
                        setattr(pos, "reconcile_pending_since", _now_iso)
                correction_details.append(("QTY_SPIKE_BLOCKED", code, str(old_qty), str(brk_qty)))
                logger.critical(
                    "[RECON_QTY_SPIKE] %s: %d -> %d (%.0f%% change) — "
                    "SYMBOL ISOLATED (reconcile_pending=True); manual review required!",
                    code, old_qty, brk_qty, ratio * 100)
                if trade_logger:
                    trade_logger.log_reconcile(
                        code, "QTY_SPIKE_BLOCKED",
                        engine_qty=old_qty, broker_qty=brk_qty,
                        engine_avg=pos.avg_price, broker_avg=brk_avg,
                        resolution="ISOLATED_RECONCILE_PENDING")
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
                # clear isolation on successful sync
                if getattr(pos, "reconcile_pending", False):
                    pos.reconcile_pending = False
                    pos.reconcile_pending_since = None
                    logger.info(f"[RECON_PENDING_RELEASED] {code} — "
                                f"qty resynced to broker {brk_qty}")
        else:
            # BOTH match with no quantity diff & no spike → release pending (if any)
            if getattr(pos, "reconcile_pending", False):
                pos.reconcile_pending = False
                pos.reconcile_pending_since = None
                logger.info(f"[RECON_PENDING_RELEASED] {code} — "
                            f"broker qty matches engine ({brk_qty}); isolation cleared")
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

    # ── 2d. 24h fallback: forcibly release stale reconcile_pending ──
    # If a position has been isolated >24h and is still in broker holdings at
    # a stable qty (no spike this round), trust broker and clear isolation.
    # Positions that disappeared from broker are already handled in 2a.
    _now = datetime.now()
    for code, pos in list(portfolio.positions.items()):
        if not getattr(pos, "reconcile_pending", False):
            continue
        _since = getattr(pos, "reconcile_pending_since", None)
        if not _since:
            continue
        try:
            _since_dt = datetime.fromisoformat(_since)
        except (ValueError, TypeError):
            continue
        if (_now - _since_dt) < timedelta(hours=24):
            continue
        # Only release if broker still reports this symbol (truth available)
        if code not in broker_holdings:
            continue
        logger.warning(
            f"[RECON_PENDING_RELEASED] {code} — 24h fallback: "
            f"pending since {_since}, forcibly clearing (broker qty={broker_holdings[code]['qty']})")
        pos.reconcile_pending = False
        pos.reconcile_pending_since = None

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

    if corrections > 0 or spike_blocked_count > 0:
        logger.warning(
            f"[RECON] {corrections} corrections applied, "
            f"{spike_blocked_count} SPIKE isolations (manual review):")
        for ctype, ccode, cold, cnew in correction_details:
            logger.warning(f"  [{ctype}] {ccode}: {cold} -> {cnew}")
    _cash_disp = broker_cash if broker_cash is not None else portfolio.cash
    logger.info(
        f"[RECON] Done — cash={_cash_disp:,.0f}, positions={synced}, "
        f"spike_isolated={spike_blocked_count}")

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
            "spike_blocked_count": spike_blocked_count,
            "safe_mode": safe_mode, "safe_mode_reason": safe_mode_reason,
            "correction_details": correction_details,
            "cash_spike": cash_spike}
