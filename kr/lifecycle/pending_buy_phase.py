"""
lifecycle/pending_buy_phase.py — Phase 1.5: Pending Buy Recovery
================================================================
Execute pending buys from previous session (T+1 model).

Extracted from run_live() Phase 1.5 block.
"""
from __future__ import annotations

import logging
from datetime import date, datetime

from lifecycle.context import LiveContext
from lifecycle.phase_skeleton import Phase, is_order_allowed
from lifecycle.utils import _safe_save, _count_trading_days

logger = logging.getLogger("gen4.live")


def run_pending_buy(ctx: LiveContext) -> None:
    """Phase 1.5: Execute pending buy recovery."""
    from risk.exposure_guard import BuyPermission

    config = ctx.config
    state_mgr = ctx.state_mgr
    portfolio = ctx.portfolio
    guard = ctx.guard
    executor = ctx.executor
    provider = ctx.provider
    trade_logger = ctx.trade_logger
    tracker = ctx.tracker
    mode_label = ctx.mode_label
    name_cache = ctx.name_cache

    _is_paper_test = mode_label.lower() in ("paper_test", "shadow_test")
    _cycle = config.PAPER_TEST_CYCLE if _is_paper_test else "full"

    # sell_only: skip buy entirely
    _skip_phase15 = False
    if _is_paper_test:
        if _cycle == "sell_only":
            _skip_phase15 = True
            logger.info("[PAPER_TEST] sell_only mode — skipping pending buys")
        elif _cycle == "full" and config.PAPER_TEST_FAST_REENTRY:
            rt_check = state_mgr.load_runtime()
            if rt_check.get("test_reentry_ready_at"):
                _skip_phase15 = True
                logger.info("[PAPER_TEST] Pending buys deferred to monitor loop (fast reentry)")
        elif _cycle == "buy_only":
            _skip_phase15 = False
            logger.info("[PAPER_TEST] buy_only mode — executing pending buys immediately")

    pending_buys, pb_sell_status = state_mgr.load_pending_buys()
    logger.info(f"[PENDING_BUY_LOAD] {len(pending_buys)} buys, "
                f"sell_status={pb_sell_status or 'NONE'}")

    # buy_only: force sell_status to COMPLETE
    if _cycle == "buy_only" and pb_sell_status not in ("COMPLETE",):
        logger.warning(f"[PAPER_TEST_BUY_ONLY] Forcing sell_status: "
                       f"{pb_sell_status} -> COMPLETE")
        pb_sell_status = "COMPLETE"
        state_mgr.save_pending_buys(pending_buys, "COMPLETE")

    # Guard: block pending buys if recovery failed or safe_mode active
    if pending_buys and not ctx.recovery_ok:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_RECOVERY] "
            f"{len(pending_buys)} buys blocked — recovery_ok=False")
        pending_buys = []

    if pending_buys and guard.safe_mode_reason:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_SAFE_MODE] "
            f"{len(pending_buys)} buys blocked — {guard.safe_mode_reason}")
        pending_buys = []

    # BuyPermission check
    if pending_buys:
        _pb_perm, _pb_reason = guard.get_buy_permission()
        if _pb_perm in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
            logger.warning(
                f"[PENDING_BUY_BLOCKED_PERMISSION] "
                f"{len(pending_buys)} buys blocked — {_pb_perm.value}: {_pb_reason}")
            pending_buys = []

    if pending_buys and not ctx.monitor_only and not _skip_phase15:
        if pb_sell_status not in ("COMPLETE",):
            # Auto-upgrade from RECON if safe
            from lifecycle.reconcile import _reconcile_with_broker
            recon_details = []  # Already done in startup; use cached result
            # We don't have recon details cached here; re-check sell status logic
            if pb_sell_status == "PARTIAL":
                logger.warning(
                    f"[SELL_STATUS_CHECK] sell_status={pb_sell_status} — "
                    f"cannot auto-upgrade without RECON correction details")

        if pb_sell_status not in ("COMPLETE",):
            logger.warning(
                f"[PENDING_BUY_BLOCKED_UNSETTLED_REBAL] "
                f"sell_status={pb_sell_status}, "
                f"{len(pending_buys)} buys blocked. "
                f"Manual intervention or next successful rebal required.")
            logger.info(f"[PENDING_BUY_SKIP] reason=unsettled_sell_status:{pb_sell_status}")
        else:
            # COMPLETE → signal_date validity check
            signal_date = pending_buys[0].get("signal_date", "")
            expired = False
            if signal_date:
                try:
                    sig_dt = datetime.strptime(signal_date, "%Y%m%d").date()
                    age_days = _count_trading_days(sig_dt, date.today(), config)
                    if age_days > 2:
                        logger.warning(
                            f"[PENDING_BUY_EXPIRED] signal_date={signal_date} "
                            f"age={age_days}d > 2 — discarding")
                        expired = True
                except (ValueError, TypeError):
                    logger.warning("[PENDING_BUY_EXPIRED] invalid signal_date — discarding")
                    expired = True

            if not expired:
                logger.info(f"[PENDING_BUY_EXECUTE] {len(pending_buys)} buys "
                            f"from signal_date={signal_date}")
                # Import the execution function from main
                from main import _execute_pending_buys
                _pb_ok, _pb_fail = _execute_pending_buys(
                    portfolio, pending_buys, config, executor, provider,
                    trade_logger, state_mgr, logger, mode_label,
                    tracker=tracker, name_cache=name_cache)

                if _pb_fail == 0:
                    state_mgr.clear_pending_buys()
                elif _pb_ok == 0:
                    logger.critical(
                        "[PENDING_BUY_ALL_FAILED] %d buys all failed — "
                        "pending_buys RETAINED for next session retry", _pb_fail)
                else:
                    logger.warning(
                        "[PENDING_BUY_PARTIAL_CLEAR] success=%d fail=%d total=%d "
                        "— clearing all, relying on RECON + EXISTING_POSITION guard",
                        _pb_ok, _pb_fail, _pb_ok + _pb_fail)
                    state_mgr.clear_pending_buys()
            else:
                logger.info(f"[PENDING_BUY_SKIP] reason=expired signal_date={signal_date}")
                state_mgr.clear_pending_buys()
    elif pending_buys and ctx.monitor_only:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_MONITOR_ONLY] "
            f"{len(pending_buys)} buys blocked — session is monitor-only")
