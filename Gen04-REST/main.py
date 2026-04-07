"""
Gen4 Core — Main Entry Point
==============================
LowVol + Momentum 12-1 monthly rebalance strategy.

Usage:
  python main.py --batch      # OHLCV update + scoring + save target
  python main.py --live       # Kiwoom live: monitor trail + execute rebalance
  python main.py --rebalance  # Force rebalance now (manual)
  python main.py --backtest   # Run backtester
  python main.py --mock       # Mock mode (no broker, simulated)

Requires: venv python with pykrx
  C:\\Q-TRON-32_ARCHIVE\\.venv\\Scripts\\python.exe main.py --batch
"""
from __future__ import annotations
import argparse
import hashlib
import json
import logging
import signal
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Kakao notifier (optional — failure never blocks trading) ─────────────────
try:
    from notify.kakao_notify import notify_buy as _kakao_buy
    from notify.kakao_notify import notify_sell as _kakao_sell
    from notify.kakao_notify import notify_trail_stop as _kakao_trail
    from notify.kakao_notify import notify as _kakao_notify
    from notify.kakao_notify import notify_safe_mode as _kakao_safe_mode
    from notify.kakao_notify import notify_buy_blocked as _kakao_buy_blocked
    _KAKAO_OK = True
except Exception:
    _KAKAO_OK = False

# ── Phase 1 extracted modules ─────────────────────────────────────────────────
from notify.helpers import (
    _load_name_cache, _save_name_cache, _enrich_name_cache,
    _kname, _notify_buy, _notify_sell, _notify_trail,
)
from lifecycle.utils import (
    _file_hash, setup_logging, is_weekday, is_trading_day, is_market_hours,
    _resolve_trading_mode, validate_trading_mode,
    _safe_save, _save_test_reentry_meta,
    _count_trading_days, _compute_regime_snapshot,
)
from lifecycle.batch import run_batch
from lifecycle.mock import run_mock
from lifecycle.reconcile import _reconcile_with_broker

# ── Dead code removed: 12 duplicate functions (already in lifecycle/utils, notify/helpers)
# ── Originals: _load_name_cache, _save_name_cache, _enrich_name_cache, _kname,
# ──   _notify_buy, _notify_sell, _notify_trail, _file_hash, setup_logging,
# ──   is_weekday, is_trading_day, is_market_hours, _resolve_trading_mode,
# ──   validate_trading_mode — all imported from lifecycle/utils and notify/helpers above.


def _try_fast_reentry(state_mgr, portfolio, config, executor, provider,
                      trade_logger, logger, mode_label, tracker=None,
                      name_cache=None, guard=None) -> bool:
    """paper_test only: execute pending buys after delay instead of T+1.

    Returns True if reentry executed (or permanently skipped), False to retry next cycle.
    """
    rt = state_mgr.load_runtime()
    ready_at_str = rt.get("test_reentry_ready_at", "")
    cycle_id = rt.get("test_cycle_id", "")
    sell_status = rt.get("rebal_sell_status", "")
    pending_buys = rt.get("pending_buys", [])
    dd_blocked_buys = rt.get("dd_blocked_buys", [])

    # ── Diagnostic: re-evaluate DD_GUARD with current equity ──
    daily_pnl = portfolio.get_daily_pnl_pct()
    monthly_dd = portfolio.get_monthly_dd_pct()
    can_buy_now = True
    buy_scale_now = 1.0
    risk_level = "UNKNOWN"
    if guard:
        risk_action = guard.get_risk_action(
            daily_pnl, monthly_dd,
            dd_levels=config.DD_LEVELS,
            safe_mode_release=config.SAFE_MODE_RELEASE_THRESHOLD)
        skip_buys, _ = guard.should_skip_rebalance(daily_pnl, monthly_dd)
        buy_scale_now = risk_action["buy_scale"]
        if skip_buys:
            buy_scale_now = 0.0
        can_buy_now = buy_scale_now > 0
        risk_level = risk_action["level"]

    # Use dd_blocked_buys as fallback when pending_buys is empty due to DD block
    effective_buys = pending_buys if pending_buys else dd_blocked_buys

    logger.info(
        f"[FAST_REENTRY_CHECK] ready_at={ready_at_str or 'NONE'} "
        f"sell_status={sell_status or 'NONE'} "
        f"pending_buys={len(pending_buys)} dd_blocked_buys={len(dd_blocked_buys)} "
        f"monthly_dd={monthly_dd:.2%} daily_pnl={daily_pnl:.2%} "
        f"risk={risk_level} buy_scale={buy_scale_now:.0%} can_buy={can_buy_now}")

    if not ready_at_str:
        logger.info("[FAST_REENTRY_BLOCKED] reason=NO_READY_AT (reentry not scheduled)")
        return False

    if not effective_buys:
        logger.info("[FAST_REENTRY_BLOCKED] reason=NO_PENDING_BUYS "
                    "(pending_buys and dd_blocked_buys both empty)")
        return False

    # Check sell_status
    if sell_status not in ("COMPLETE",):
        logger.info(f"[FAST_REENTRY_BLOCKED] reason=SELL_STATUS_{sell_status} "
                    f"(waiting for ghost fills to settle)")
        return False

    # Check time
    try:
        ready_at = datetime.fromisoformat(ready_at_str)
    except (ValueError, TypeError):
        logger.warning(f"[FAST_REENTRY_BLOCKED] reason=INVALID_READY_AT "
                       f"value={ready_at_str} — skipping permanently")
        return True  # skip permanently

    now = datetime.now()
    remaining = (ready_at - now).total_seconds()
    if remaining > 0:
        logger.info(f"[FAST_REENTRY_BLOCKED] reason=NOT_YET "
                    f"remaining={remaining:.0f}s ready_at={ready_at_str}")
        return False  # not yet

    # Re-evaluate DD_GUARD at execution time
    if not can_buy_now:
        logger.warning(
            f"[FAST_REENTRY_BLOCKED] reason=DD_GUARD "
            f"level={risk_level} monthly_dd={monthly_dd:.2%} "
            f"daily_pnl={daily_pnl:.2%} buy_scale={buy_scale_now:.0%} "
            f"— will retry next cycle")
        return False  # retry — DD may improve

    # If original pending_buys was empty but dd_blocked_buys available,
    # validate metadata, filter stale/duplicate, then restore
    if not pending_buys and dd_blocked_buys:
        dd_meta = rt.get("dd_blocked_meta", {})
        blocked_cycle = dd_meta.get("cycle_id", "")
        blocked_created = dd_meta.get("created_at", "")

        # ── Validate: cycle_id must match current session ──
        if blocked_cycle and cycle_id and blocked_cycle != cycle_id:
            logger.warning(
                f"[DD_BLOCKED_RESTORE] DISCARD_CYCLE_MISMATCH "
                f"blocked_cycle={blocked_cycle} current_cycle={cycle_id} "
                f"— {len(dd_blocked_buys)} buys discarded")
            rt["dd_blocked_buys"] = []
            rt.pop("dd_blocked_meta", None)
            state_mgr.save_runtime(rt)
            return True  # permanently skip — wrong cycle

        # ── Validate: created_at must not be stale (max 1 calendar day) ──
        if blocked_created:
            try:
                created_dt = datetime.fromisoformat(blocked_created)
                age_hours = (datetime.now() - created_dt).total_seconds() / 3600
                if age_hours > 24:
                    logger.warning(
                        f"[DD_BLOCKED_RESTORE] DISCARD_STALE "
                        f"created_at={blocked_created} age={age_hours:.1f}h "
                        f"— {len(dd_blocked_buys)} buys discarded")
                    rt["dd_blocked_buys"] = []
                    rt.pop("dd_blocked_meta", None)
                    state_mgr.save_runtime(rt)
                    return True  # permanently skip — too old
            except (ValueError, TypeError):
                pass  # invalid timestamp — proceed with caution

        # ── Filter: skip tickers already in portfolio (EXISTING_POSITION) ──
        existing_codes = set(portfolio.positions.keys())
        restored = []
        for b in dd_blocked_buys:
            ticker = b["ticker"]
            if ticker in existing_codes:
                logger.info(f"[DD_BLOCKED_RESTORE] code={ticker} "
                            f"action=SKIP_EXISTING (already in portfolio)")
                continue
            b["target_amount"] = b.get("original_amount", b["target_amount"]) * buy_scale_now
            restored.append(b)
            logger.info(f"[DD_BLOCKED_RESTORE] code={ticker} action=RESTORE "
                        f"amount={b['target_amount']:,.0f} scale={buy_scale_now:.0%}")

        n_skip = len(dd_blocked_buys) - len(restored)
        logger.info(
            f"[DD_BLOCKED_RESTORE_SUMMARY] total={len(dd_blocked_buys)} "
            f"restored={len(restored)} skipped_existing={n_skip} "
            f"cycle={blocked_cycle} buy_scale={buy_scale_now:.0%}")

        if not restored:
            logger.info("[FAST_REENTRY_BLOCKED] reason=ALL_BLOCKED_BUYS_FILTERED")
            rt["dd_blocked_buys"] = []
            rt.pop("dd_blocked_meta", None)
            state_mgr.save_runtime(rt)
            return True  # nothing left to execute

        effective_buys = restored

    # All conditions met — execute
    logger.info(f"[FAST_REENTRY_EXECUTE] cycle={cycle_id} "
                f"buys={len(effective_buys)} sell_status={sell_status} "
                f"cash={portfolio.cash:,.0f} buy_scale={buy_scale_now:.0%}")
    logger.info(f"[TRACKER_WIRED] fast_reentry tracker={'yes' if tracker else 'no'}")

    _execute_pending_buys(
        portfolio, effective_buys, config, executor, provider,
        trade_logger, state_mgr, logger, mode_label, tracker=tracker,
        name_cache=name_cache)

    # Mark executed
    rt["pending_buys"] = []
    rt["dd_blocked_buys"] = []
    rt.pop("dd_blocked_meta", None)
    rt["rebal_sell_status"] = ""
    rt["test_reentry_executed"] = True
    state_mgr.save_runtime(rt)
    _safe_save(state_mgr, portfolio, context="paper_test_reentry_complete")
    logger.info(f"[PAPER_TEST_REENTRY_DONE] cycle={cycle_id}")
    return True


# [REMOVED] _save_test_reentry_meta — use lifecycle.utils (imported line 47)


# [REMOVED] _safe_save — use lifecycle.utils (imported line 47)


# [REMOVED] run_batch — use lifecycle.batch (imported line 50)


# [REMOVED] _count_trading_days — use lifecycle.utils (imported line 48)


# [REMOVED] _compute_regime_snapshot — use lifecycle.utils (imported line 48)


# ── Live Mode ────────────────────────────────────────────────────────────────
def run_live(config):
    """
    Live mode: Kiwoom login → broker sync → rebalance → trail stop monitor → EOD.
    """
    logger = logging.getLogger("gen4.live")
    logger.info("=" * 60)
    logger.info("  Gen4 Live Mode")
    logger.info("=" * 60)

    # ── Pre-flight ───────────────────────────────────────────────
    if not is_trading_day():
        logger.warning("Non-trading day (weekend/holiday). Exiting.")
        return
    if datetime.now().hour >= 16 and _resolve_trading_mode(config) != "shadow_test":
        logger.warning("After 16:00. Market closed. Exiting.")
        return

    # ── Kakao name cache (loaded once per session) ───────────────
    _name_cache = _load_name_cache(config.BASE_DIR)

    # ── Regime Snapshot (once per session, observation only) ──────
    session_regime, session_kospi_ma200, session_breadth = _compute_regime_snapshot(config)

    # ── Phase 0: QApplication + Kiwoom Login ─────────────────────
    from PyQt5.QtWidgets import QApplication
    app = QApplication.instance() or QApplication(sys.argv)

    from api.kiwoom_connector import create_loggedin_kiwoom
    from data.kiwoom_provider import Gen4KiwoomProvider
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from risk.exposure_guard import ExposureGuard, BuyPermission
    from strategy.trail_stop import check_trail_stop, calc_trail_stop_price
    from strategy.factor_ranker import load_target_portfolio
    from report.reporter import TradeLogger, save_forensic_snapshot

    kiwoom, server_type = create_loggedin_kiwoom()

    # ── Trading mode resolution ──────────────────────────────────
    # Derive trading_mode from server_type (auto-detect):
    #   MOCK server → paper,  REAL server → live
    # Then validate against config.TRADING_MODE intent.
    intended_mode = _resolve_trading_mode(config)
    actual_mode = "paper" if server_type == "MOCK" else "live"

    # If config says "paper" but server is REAL (or vice versa), abort
    try:
        validate_trading_mode(intended_mode, server_type, broker_connected=True)
    except RuntimeError as e:
        logger.critical(str(e))
        return

    # paper_test / shadow_test: preserve intended mode for separate state/signals/report paths
    trading_mode = intended_mode if intended_mode in ("paper_test", "shadow_test") else actual_mode
    logger.info(f"[TRADING_MODE] {trading_mode}  "
                f"(intended={intended_mode}, server={server_type})")

    # FORCE_* fault-injection flags must NEVER be active in live/paper mode
    if trading_mode in ("live", "paper"):
        _force_flags = {
            "FORCE_OPT10075_FAIL": getattr(config, "FORCE_OPT10075_FAIL", False),
            "FORCE_RECON_CORRECTIONS": getattr(config, "FORCE_RECON_CORRECTIONS", 0),
            "FORCE_PENDING_EXTERNAL_BLOCK": getattr(config, "FORCE_PENDING_EXTERNAL_BLOCK", False),
        }
        _active = {k: v for k, v in _force_flags.items() if v}
        if _active:
            logger.critical(f"[ABORT] FORCE_* flags active in {trading_mode} mode: {_active}")
            raise RuntimeError(f"FORCE_* flags must be False in {trading_mode} mode: {_active}")

    # LIVE protection: block if test residue detected
    if actual_mode == "live" and intended_mode not in ("paper_test", "shadow_test"):
        _test_state = config.STATE_DIR / "runtime_state_paper_test.json"
        if _test_state.exists():
            import json
            try:
                with open(_test_state, "r") as f:
                    _trt = json.load(f)
                if _trt.get("test_cycle_id") or _trt.get("test_reentry_ready_at"):
                    logger.critical(
                        "[LIVE_BLOCKED_TEST_RESIDUE] paper_test runtime fields "
                        "detected. Clean up before LIVE trading.")
                    return
            except Exception:
                pass  # file parse error — not blocking

    provider = Gen4KiwoomProvider(kiwoom, str(config.SECTOR_MAP))
    provider._server_type = server_type  # "MOCK" or "REAL" — for broker gate check

    tracker = OrderTracker(journal_dir=config.LOG_DIR, trading_mode=trading_mode)
    if trading_mode == "paper_test":
        _report_dir = config.REPORT_DIR_TEST
    elif trading_mode == "shadow_test":
        _report_dir = config.REPORT_DIR_SHADOW
    else:
        _report_dir = config.REPORT_DIR
    trade_logger = TradeLogger(_report_dir)
    # simulate=False: orders go via Kiwoom API. Server determines virtual/real.
    executor = OrderExecutor(provider, tracker, trade_logger,
                             simulate=False, trading_mode=trading_mode)

    mode_label = trading_mode.upper()  # "PAPER" / "PAPER_TEST" / "SHADOW_TEST" / "LIVE"
    logger.info(f"Mode: {mode_label}  (server={server_type})")

    # ── Phase 1: State Restore + Broker Sync ─────────────────────
    state_mgr = StateManager(config.STATE_DIR, trading_mode=trading_mode)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)
    logger.info("[RECOVERY_STATE_INIT] session-local, starts at NORMAL "
                "(stateless — re-evaluated each session from live signals)")

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)
        logger.info(f"Restored: {len(portfolio.positions)} positions, cash={portfolio.cash:,.0f}")

        # ── C1 FIX: Startup price validity guard ──────────────────
        # After restore, current_price may be stale (from last save) or zero
        # (legacy state without current_price).  Phantom DD can false-trigger
        # DD guard before RECON/real-time prices arrive.
        #
        # Composite condition — ALL must be true to neutralize:
        #   1) apparent daily DD exceeds -30% (extreme, not normal overnight)
        #   2) market_value is suspiciously low vs prev_close_equity
        #      (most positions have current_price=0 or near-zero)
        #   3) at least one position has current_price <= 0
        #
        # This ensures:
        #   - Normal overnight DD (-3%~-18%) is NEVER masked
        #   - Only structural price-data-loss (restart with stale/zero prices)
        #     triggers neutralization
        #   - RECON will sync broker truth; real-time ticks update prices
        _restored_equity = portfolio.get_current_equity()
        _saved_prev = portfolio.prev_close_equity
        _n_positions = len(portfolio.positions)
        if _saved_prev > 0 and _n_positions > 0:
            _startup_dd = (_restored_equity - _saved_prev) / _saved_prev
            _market_value = sum(p.market_value for p in portfolio.positions.values())
            _mv_ratio = _market_value / _saved_prev if _saved_prev > 0 else 1.0
            _zero_price_count = sum(
                1 for p in portfolio.positions.values() if p.current_price <= 0)

            _is_phantom = (
                _startup_dd < -0.30          # condition 1: extreme DD
                and _mv_ratio < 0.50         # condition 2: market value < 50% of prev equity
                and _zero_price_count > 0    # condition 3: at least one zero-price position
            )

            logger.info(
                f"[STARTUP_PRICE_CHECK] equity={_restored_equity:,.0f} "
                f"prev_close={_saved_prev:,.0f} dd={_startup_dd:.2%} "
                f"mv_ratio={_mv_ratio:.2%} zero_prices={_zero_price_count}/{_n_positions} "
                f"phantom={_is_phantom}")

            if _is_phantom:
                logger.warning(
                    f"[STARTUP_PRICE_GUARD] Phantom DD detected: "
                    f"dd={_startup_dd:.2%} mv_ratio={_mv_ratio:.2%} "
                    f"zero_prices={_zero_price_count}/{_n_positions} "
                    f"— neutralizing prev_close_equity until RECON/realtime")
                portfolio.prev_close_equity = _restored_equity
                portfolio.peak_equity = max(portfolio.peak_equity, _restored_equity)

    # Ghost fill sync: executor needs portfolio + state_mgr for immediate sync
    executor.set_ghost_fill_context(portfolio, state_mgr,
                                     buy_cost=config.BUY_COST)
    provider.set_ghost_fill_callback(executor.on_ghost_fill)

    # ── LIVE Guard: block if test paths are active ────────────────
    if trading_mode == "live":
        _live_signals_dir = config.SIGNALS_DIR  # live always uses production
        logger.info("[LIVE_GUARD] trading_mode=%s signals_dir=%s state_mode=%s",
                    trading_mode, _live_signals_dir, state_mgr.trading_mode)
        if _live_signals_dir == config.SIGNALS_DIR_TEST:
            logger.critical("[LIVE_BLOCKED_TEST_SIGNALS] "
                            "signals_dir points to test path. Aborting.")
            return
        if state_mgr.trading_mode == "paper_test":
            logger.critical("[LIVE_BLOCKED_TEST_STATE] "
                            "state manager uses paper_test mode. Aborting.")
            return

    # ── Dirty Exit Detection ─────────────────────────────────────
    _dirty = state_mgr.was_dirty_exit()
    _last_reason = state_mgr.get_last_shutdown_reason()

    # FRESH mode: intentional state deletion, not a real dirty exit
    if _dirty and getattr(config, "FRESH_START", False):
        _dirty = False
        logger.info("[FRESH_OVERRIDE] Dirty exit suppressed — FRESH intentional state deletion")

    if _dirty:
        logger.warning(f"[DIRTY_EXIT_DETECTED] last_shutdown_reason={_last_reason} "
                       f"— running recovery-first startup")
    else:
        logger.info(f"[CLEAN_STARTUP] last_shutdown_reason={_last_reason}")

    # Mark session as running (dirty) — will be cleared on clean shutdown
    state_mgr.mark_startup()

    # ── Discard stale PENDING_EXTERNAL orders (24h threshold) ──
    _pe_stale = state_mgr.load_pending_external()
    if _pe_stale:
        _now_ts = datetime.now()
        _kept, _discarded = [], []
        for _pe in _pe_stale:
            _pe_ts_str = _pe.get("requested_at") or _pe.get("timestamp", "")
            try:
                _pe_ts = datetime.fromisoformat(_pe_ts_str)
                _age_h = (_now_ts - _pe_ts).total_seconds() / 3600
                if _age_h > 24:
                    _discarded.append((_pe, f"{_age_h:.1f}h"))
                else:
                    _kept.append(_pe)
            except (ValueError, TypeError):
                _discarded.append((_pe, "unparseable_ts"))
        if _discarded:
            logger.warning(f"[PENDING_STALE_DISCARD] discarded={len(_discarded)} "
                           f"kept={len(_kept)} total={len(_pe_stale)}")
            for _d, _reason in _discarded:
                logger.info(f"  discarded: order_id={_d.get('order_id','?')} "
                            f"code={_d.get('code','?')} side={_d.get('side','?')} "
                            f"ts={_d.get('requested_at', _d.get('timestamp','?'))} "
                            f"age={_reason}")
            if _kept:
                _saved = state_mgr.save_pending_external(_kept)
            else:
                _saved = state_mgr.clear_pending_external()
            if not _saved:
                logger.error("[PENDING_STALE_DISCARD] state save FAILED after discard")
            else:
                logger.info(f"[PENDING_STALE_DISCARD] state saved — "
                            f"remaining pending_external={len(_kept)}")
        else:
            logger.info(f"[PENDING_EXTERNAL_CHECK] {len(_pe_stale)} entries, all within 24h")

    # ── Cancel Stale Orders (recovery-first) ──────────────────
    # Always cancel open orders on startup — prevents ghost fills from
    # previous session's orphaned orders (root cause of OVERFILL)
    _recovery_ok = True  # assume clean until proven otherwise
    logger.info("[STARTUP_CANCEL] Querying open orders from previous session...")
    try:
        _cancelled = provider.cancel_all_open_orders()
        if _cancelled is None:
            # Query itself failed — cannot determine stale order state
            logger.critical("[STARTUP_CANCEL_FAIL] Open order query failed — "
                            "cannot confirm stale orders cleared")
            _recovery_ok = False
        elif _cancelled > 0:
            logger.warning(f"[STARTUP_CANCEL] {_cancelled} stale orders cancelled")
            time.sleep(3.0)  # Allow cancels to settle
            # Re-verify: single query (cancel_all already verified internally)
            # Total opt10075 calls: cancel_all(query+verify) + this = 3
            try:
                _remaining = provider.query_open_orders()
                logger.info(f"[STARTUP_CANCEL_RECHECK] source=startup_cancel_check "
                            f"remaining={'None' if _remaining is None else len(_remaining)} "
                            f"opt10075_call=3of3")
                if _remaining is None:
                    logger.warning("[OPEN_ORDERS_RECHECK_FAIL] query returned None")
                    _recovery_ok = False
                elif len(_remaining) > 0:
                    logger.critical(f"[CANCEL_INCOMPLETE] {len(_remaining)} orders still open "
                                    f"after cancel — SAFE_MODE L2 + rebalance blocked")
                    guard.force_safe_mode(
                        f"CANCEL_INCOMPLETE: {len(_remaining)} orders remain", level=2)
                    _recovery_ok = False
                else:
                    logger.info("[OPEN_ORDERS_AFTER_CANCEL] 0 — verified clean")
            except Exception as _recheck_err:
                logger.warning(f"[OPEN_ORDERS_RECHECK_FAIL] {_recheck_err}")
                _recovery_ok = False
            # Dirty exit + had stale orders → extra caution
            if _dirty:
                _recovery_ok = False
                logger.warning("[RECOVERY_PENDING] dirty exit + stale orders — "
                               "will verify after RECON before trading")
        else:
            logger.info("[STARTUP_CANCEL] No stale orders found")
    except Exception as _cancel_err:
        logger.critical(f"[STARTUP_CANCEL_FAIL] {_cancel_err} — proceeding with RECON")
        _recovery_ok = False

    # Record opt10075 result for buy permission
    # Fault injection (paper_test only)
    _opt_success = _recovery_ok
    if (getattr(config, "FORCE_OPT10075_FAIL", False)
            and trading_mode in ("paper_test", "shadow_test")):
        _opt_success = False
        logger.warning("[FAULT_INJECT] FORCE_OPT10075_FAIL=True → recording as failure")
    logger.info(f"[OPT10075_RECORD] source=startup_cancel success={_opt_success} "
                f"recovery_ok={_recovery_ok} streak_before={guard._opt10075_fail_streak}")
    guard.record_opt10075_result(success=_opt_success)

    # Wait for broker data to settle after login (mitigation, not root fix)
    logger.info("[RECON_WAIT] Waiting 3s for broker data to settle...")
    time.sleep(3.0)
    logger.info("[RECON_WAIT] Done — proceeding with reconciliation")

    # Broker reconciliation — broker is truth, engine state synced (with safety guards)
    recon = _reconcile_with_broker(portfolio, provider, logger, trade_logger,
                                    buy_cost=config.BUY_COST)
    reconcile_corrections = recon.get("corrections", 0) if recon else 0
    if recon and reconcile_corrections > 0:
        if trading_mode != "shadow_test":
            _safe_save(state_mgr, portfolio,
                       context=f"recon/{reconcile_corrections}corrections")
            logger.info(f"[RECON] State saved after {recon['corrections']} corrections")
        else:
            logger.info(f"[SHADOW_TEST] RECON found {reconcile_corrections} corrections (not saved)")
    if recon and not recon.get("ok", True):
        if trading_mode == "shadow_test":
            logger.warning("[SHADOW_TEST_EXIT] Broker sync failed — logged only, no abort needed")
            logger.info("[SHADOW_TEST_RESULT] RECON=FAIL, corrections=%d, error=%s",
                        reconcile_corrections, recon.get("error", "unknown"))
            return
        logger.critical("Broker sync FAILED — aborting LIVE to prevent stale-state trading")
        save_forensic_snapshot(
            config.STATE_DIR,
            portfolio_data=portfolio.to_dict(),
            error_msg=f"Broker sync failed: {recon.get('error', 'unknown')}",
            extra={"recon": recon})
        return

    # ── shadow_test: exit after RECON (read-only verification complete) ──
    if trading_mode == "shadow_test":
        _pos_count = len(portfolio.positions)
        _cash = portfolio.cash
        logger.info("[SHADOW_TEST_EXIT] RECON completed successfully")
        logger.info("[SHADOW_TEST_RESULT] RECON=OK, corrections=%d, "
                    "positions=%d, cash=%s, broker_ok=True",
                    reconcile_corrections, _pos_count, f"{_cash:,.0f}")
        return

    # FRESH start: reset equity baseline to current (post-RECON) equity
    # to prevent DD_GUARD false trigger from stale prev_close_equity
    if getattr(config, "FRESH_START", False) and reconcile_corrections > 0:
        cur_eq = portfolio.get_current_equity()
        portfolio.prev_close_equity = cur_eq
        portfolio.peak_equity = max(portfolio.peak_equity, cur_eq)
        logger.info(f"[FRESH_EQUITY_RESET] prev_close_equity={cur_eq:,.0f} "
                    f"(post-RECON baseline, DD_GUARD starts at 0%)")
    # ── Enrich name cache from Kiwoom master data ────────────────
    _all_codes = list(portfolio.positions.keys())
    if _enrich_name_cache(_name_cache, _all_codes, provider):
        _save_name_cache(config.BASE_DIR, _name_cache)
        logger.info(f"[NAME_CACHE] Updated: {len(_name_cache)} entries")

    # ── Recovery Verification (dirty exit + stale orders) ────────
    if not _recovery_ok:
        # Single re-check after RECON (opt10075 call count control)
        _recheck_ok = False
        try:
            _remaining = provider.query_open_orders()
            logger.info(f"[RECOVERY_RECHECK] source=recovery_recheck "
                        f"remaining={'None' if _remaining is None else len(_remaining)}")
            if _remaining is None:
                logger.critical("[RECOVERY_CHECK_FAIL] query returned None — staying cautious")
            elif len(_remaining) > 0:
                logger.critical(f"[RECOVERY_FAILED] {len(_remaining)} orders still open "
                                f"after cancel — forcing SAFE_MODE L2")
                guard.force_safe_mode("RECOVERY_FAILED: stale orders remain", level=2)
            else:
                logger.info("[RECOVERY_OK] No stale orders remain — verified clean")
                _recovery_ok = True
                _recheck_ok = True
        except Exception as _re:
            logger.warning(f"[RECOVERY_CHECK_FAIL] {_re} — staying cautious")
        # Fault injection (paper_test only)
        _recheck_success = _recheck_ok
        if (getattr(config, "FORCE_OPT10075_FAIL", False)
                and trading_mode in ("paper_test", "shadow_test")):
            _recheck_success = False
            logger.warning("[FAULT_INJECT] FORCE_OPT10075_FAIL=True → recheck as failure")
        logger.info(f"[OPT10075_RECORD] source=recovery_recheck success={_recheck_success} "
                    f"streak_before={guard._opt10075_fail_streak}")
        guard.record_opt10075_result(success=_recheck_success)

    # Fault injection: force 2nd opt10075 failure even when recovery_ok=True
    if (getattr(config, "FORCE_OPT10075_FAIL", False)
            and trading_mode in ("paper_test", "shadow_test")
            and _recovery_ok):
        logger.warning(f"[OPT10075_RECORD] source=fault_inject_test success=False "
                       f"streak_before={guard._opt10075_fail_streak}")
        guard.record_opt10075_result(success=False)

    # Session-level context for equity log (initialized early for safe_mode path)
    session_rebalance_executed = False
    session_price_fail_count = 0
    session_monitor_only = False

    # Record RECON result for recovery state machine
    _recon_ok = not (recon and recon.get("safe_mode"))
    guard.record_recon_result(ok=_recon_ok)

    if recon and recon.get("safe_mode"):
        reason = recon.get("safe_mode_reason", "excessive corrections")
        corrections = recon.get("corrections", 0)
        cash_spike = recon.get("cash_spike", False)
        # Determine safe_mode level from RECON severity
        if corrections > 10 and cash_spike:
            _sm_level = 3
        elif corrections > 10:
            _sm_level = 2
        else:
            _sm_level = 1
        # Persist recon_unreliable flag in runtime state
        rt = state_mgr.load_runtime()
        rt["recon_unreliable"] = True
        state_mgr.save_runtime(rt)
        if mode_label.lower() in ("paper_test", "shadow_test"):
            logger.critical(f"[RECON_SAFE_MODE_DETECTED] {reason} "
                            f"L{_sm_level} — ⚠ WOULD BLOCK in live!")
            logger.warning(f"[RECON_SAFE_MODE_SKIP] {mode_label}: proceeding despite safe mode")
        else:
            level_changed = guard.force_safe_mode(reason, level=_sm_level)
            if level_changed and _KAKAO_OK:
                try: _kakao_safe_mode(_sm_level, reason)
                except Exception: pass
        # holdings_unreliable → full monitor-only (block rebalance sells too)
        if "holdings_unreliable" in reason:
            session_monitor_only = True
            logger.critical("[BROKER_STATE_UNRELIABLE] Session forced to MONITOR-ONLY. "
                            "No rebalance, no buy, no sell until next session with "
                            "reliable holdings.")
    else:
        # RECON clean (or no safe_mode) — clear stale recon_unreliable flag
        rt = state_mgr.load_runtime()
        if rt.get("recon_unreliable"):
            rt["recon_unreliable"] = False
            state_mgr.save_runtime(rt)

    # ── Phase 1.5: Pending Buy Execution (T+1 model) ────────────
    _is_paper_test = mode_label.lower() in ("paper_test", "shadow_test")
    _cycle = config.PAPER_TEST_CYCLE if _is_paper_test else "full"

    # sell_only: skip buy entirely (Phase 1.5 + monitor reentry)
    # full with fast_reentry: defer to monitor loop
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
            # Force immediate execution — override sell_status to COMPLETE
            _skip_phase15 = False
            logger.info("[PAPER_TEST] buy_only mode — executing pending buys immediately")

    pending_buys, pb_sell_status = state_mgr.load_pending_buys()
    logger.info(f"[PENDING_BUY_LOAD] {len(pending_buys)} buys, "
                f"sell_status={pb_sell_status or 'NONE'}")

    # buy_only: force sell_status to COMPLETE (broker already settled)
    if _cycle == "buy_only" and pb_sell_status not in ("COMPLETE",):
        logger.warning(f"[PAPER_TEST_BUY_ONLY] Forcing sell_status: "
                       f"{pb_sell_status} -> COMPLETE")
        pb_sell_status = "COMPLETE"
        state_mgr.save_pending_buys(pending_buys, "COMPLETE")

    # Guard: block pending buys if recovery failed or safe_mode active
    if pending_buys and not _recovery_ok:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_RECOVERY] "
            f"{len(pending_buys)} buys blocked — recovery_ok=False")
        pending_buys = []  # prevent fall-through to execution

    if pending_buys and guard.safe_mode_reason:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_SAFE_MODE] "
            f"{len(pending_buys)} buys blocked — {guard.safe_mode_reason}")
        pending_buys = []  # prevent fall-through to execution

    # GAP-2 FIX: BuyPermission 검사 (BLOCKED/RECOVERING이면 매수 차단)
    if pending_buys:
        _pb_perm, _pb_reason = guard.get_buy_permission()
        if _pb_perm in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
            logger.warning(
                f"[PENDING_BUY_BLOCKED_PERMISSION] "
                f"{len(pending_buys)} buys blocked — {_pb_perm.value}: {_pb_reason}")
            pending_buys = []

    if pending_buys and not session_monitor_only and not _skip_phase15:
        if pb_sell_status not in ("COMPLETE",):
            # Safety: after RECON sync, engine matches broker.
            # BROKER_ONLY = broker has positions engine doesn't know about
            # → risky to proceed (unexpected holdings).
            # ENGINE_ONLY = engine had leftover from partial sells, RECON removed
            # → broker already fully sold, safe to upgrade.
            # QTY_MISMATCH = quantity difference → ambiguous, keep PARTIAL.
            recon_details = recon.get("correction_details", []) if recon else []
            dangerous_mismatches = [d for d in recon_details
                                    if d[0] in ("BROKER_ONLY", "QTY_MISMATCH")]
            if not dangerous_mismatches and pb_sell_status == "PARTIAL":
                engine_only = [d for d in recon_details if d[0] == "ENGINE_ONLY"]
                logger.warning(
                    f"[SELL_STATUS_AUTO_UPGRADE] {pb_sell_status} -> COMPLETE "
                    f"(RECON settled: engine_only_removed={len(engine_only)}, "
                    f"no dangerous mismatches)")
                pb_sell_status = "COMPLETE"
                state_mgr.save_pending_buys(pending_buys, "COMPLETE")

        if pb_sell_status not in ("COMPLETE",):
            # PARTIAL/UNCERTAIN/FAILED → buy 차단 (보수적)
            logger.warning(
                f"[PENDING_BUY_BLOCKED_UNSETTLED_REBAL] "
                f"sell_status={pb_sell_status}, "
                f"{len(pending_buys)} buys blocked. "
                f"Manual intervention or next successful rebal required.")
            logger.info(f"[PENDING_BUY_SKIP] reason=unsettled_sell_status:{pb_sell_status}")
        else:
            # COMPLETE → signal_date 유효성 체크
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
                _pb_ok, _pb_fail = _execute_pending_buys(
                    portfolio, pending_buys, config, executor, provider,
                    trade_logger, state_mgr, logger, mode_label,
                    tracker=tracker, name_cache=_name_cache)

                # Conditional clear — single authority for pending_buys state
                if _pb_fail == 0:
                    state_mgr.clear_pending_buys()
                elif _pb_ok == 0:
                    logger.critical(
                        "[PENDING_BUY_ALL_FAILED] %d buys all failed — "
                        "pending_buys RETAINED for next session retry", _pb_fail)
                else:
                    # Partial success: clear all, rely on RECON + EXISTING_POSITION guard
                    logger.warning(
                        "[PENDING_BUY_PARTIAL_CLEAR] success=%d fail=%d total=%d "
                        "— clearing all, relying on RECON + EXISTING_POSITION guard",
                        _pb_ok, _pb_fail, _pb_ok + _pb_fail)
                    state_mgr.clear_pending_buys()
            else:
                logger.info(f"[PENDING_BUY_SKIP] reason=expired signal_date={signal_date}")
                state_mgr.clear_pending_buys()
    elif pending_buys and session_monitor_only:
        logger.warning(
            f"[PENDING_BUY_BLOCKED_MONITOR_ONLY] "
            f"{len(pending_buys)} buys blocked — session is monitor-only")

    # ── Phase 2: Rebalance Check ─────────────────────────────────
    # buy_only: skip rebalance entirely
    if _cycle == "buy_only":
        logger.info("[PAPER_TEST] buy_only mode — skipping rebalance")
        need_rebalance = False
    else:
        need_rebalance = None  # will be set below

    runtime = state_mgr.load_runtime()
    last_rebal = runtime.get("last_rebalance_date", "")
    today_str = date.today().strftime("%Y%m%d")

    # Trading-day based rebalance check (matches backtest REBAL_DAYS)
    if need_rebalance is not None:
        pass  # already set (buy_only)
    elif not last_rebal:
        need_rebalance = True
        logger.info("No previous rebalance record — will rebalance.")
    else:
        try:
            last_dt = datetime.strptime(last_rebal, "%Y%m%d").date()
            # Count actual trading days using KOSPI calendar
            trading_days = _count_trading_days(last_dt, date.today(), config)
            need_rebalance = (trading_days >= config.REBAL_DAYS)
            logger.info(f"Trading days since last rebalance: {trading_days} "
                        f"(threshold: {config.REBAL_DAYS}, "
                        f"calendar days: {(date.today() - last_dt).days})")
        except (ValueError, TypeError):
            need_rebalance = True
            logger.warning(f"Failed to parse last_rebalance_date='{last_rebal}' "
                           f"— will rebalance as safety fallback.")

    # paper_test / shadow_test: force rebalance regardless of last_rebalance_date
    if not need_rebalance and mode_label.lower() in ("paper_test", "shadow_test") \
       and config.PAPER_TEST_FORCE_REBALANCE:
        test_cycle_id = runtime.get("test_cycle_id", "")
        current_cycle = f"{today_str}_force"
        if test_cycle_id != current_cycle:
            need_rebalance = True
            _force_reason = "force_rebalance" if config.FORCE_REBALANCE_CONFIRMED else "auto"
            logger.warning(f"[FORCE_REBALANCE] mode={mode_label}, "
                           f"last={last_rebal}, cycle={current_cycle}, "
                           f"reason={_force_reason}, "
                           f"operator_confirmed={config.FORCE_REBALANCE_CONFIRMED}")
            # Record in runtime_state
            rt = state_mgr.load_runtime()
            rt["force_rebalance_log"] = {
                "date": today_str,
                "mode": mode_label,
                "confirmed": config.FORCE_REBALANCE_CONFIRMED,
                "previous_rebal": last_rebal,
            }
            state_mgr.save_runtime(rt)

    # Monitor-only guard: skip rebalance entirely if session is monitor-only
    if need_rebalance and session_monitor_only:
        logger.critical("[MONITOR_ONLY] Rebalance day but session is MONITOR-ONLY "
                        "(holdings unreliable or forced safe mode). Skipping rebalance.")
        need_rebalance = False

    # Recovery guard: dirty exit + recovery incomplete → block rebalance
    if need_rebalance and _dirty and not _recovery_ok:
        logger.critical("[RECOVERY_BLOCK] Rebalance blocked — dirty exit recovery "
                        "incomplete (stale orders may remain). Monitor-only this session.")
        need_rebalance = False
        session_monitor_only = True

    if need_rebalance:
        logger.info("=" * 40)
        logger.info("  REBALANCE DAY")
        logger.info("=" * 40)

        if trading_mode == "paper_test":
            signals_dir = config.SIGNALS_DIR_TEST
            logger.info(f"[PAPER_TEST] Using test signals: {signals_dir}")
        elif trading_mode == "shadow_test":
            signals_dir = config.SIGNALS_DIR  # shadow reads production signals
            logger.info(f"[SHADOW_TEST] Using production signals (read-only): {signals_dir}")
        else:
            signals_dir = config.SIGNALS_DIR
        target = load_target_portfolio(signals_dir)
        if not target:
            logger.error("No target portfolio! Skipping rebalance, "
                         "monitor-only mode. Run: python main.py --batch")
            session_monitor_only = True
        else:
            data_date = target.get("date", "?")
            logger.info(f"Target loaded: {len(target['target_tickers'])} stocks (data: {data_date})")

            # Enrich name cache with target tickers
            try:
                if _enrich_name_cache(_name_cache, target["target_tickers"], provider):
                    _save_name_cache(config.BASE_DIR, _name_cache)
            except Exception:
                pass  # name cache is cosmetic — never block trading

            # Stale/future target check
            target_ok = True
            try:
                td = datetime.strptime(today_str, "%Y%m%d").date()
                dd = datetime.strptime(data_date, "%Y%m%d").date()
                if dd > td:
                    logger.error(f"Target date is in the FUTURE ({data_date} > {today_str}). "
                                 f"Rejecting — possible data corruption.")
                    target_ok = False
                    session_monitor_only = True
                stale_days = (td - dd).days  # no abs() — future already rejected
                if target_ok:
                    logger.info(f"Target age: {stale_days} calendar day(s) "
                                f"(max allowed: {config.TARGET_MAX_STALE_DAYS})")
                if target_ok and stale_days > config.TARGET_MAX_STALE_DAYS:
                    logger.warning(
                        f"Target is STALE ({data_date}, {stale_days}d old, "
                        f"limit={config.TARGET_MAX_STALE_DAYS}d). "
                        f"Rebalance SKIPPED — monitor-only. "
                        f"Run --batch to refresh.")
                    target_ok = False
                    session_monitor_only = True
            except (ValueError, TypeError):
                logger.warning(f"Cannot parse target date '{data_date}' — "
                               f"treating as stale. Rebalance skipped.")
                target_ok = False
                session_monitor_only = True

            if target_ok:
                # Pass pending_external to guard for buy permission check
                _pe_list = state_mgr.load_pending_external()
                if (getattr(config, "FORCE_PENDING_EXTERNAL_BLOCK", False)
                        and trading_mode in ("paper_test", "shadow_test")):
                    _pe_list = [{"requested_at": "2000-01-01T00:00:00",
                                 "code": "FAKE1"}, {"code": "FAKE2"}]
                    logger.warning("[FAULT_INJECT] FORCE_PENDING_EXTERNAL_BLOCK=True")
                guard.set_pending_external(_pe_list)

                # Risk check — graduated DD response
                daily_pnl = portfolio.get_daily_pnl_pct()
                monthly_dd = portfolio.get_monthly_dd_pct()
                risk_action = guard.get_risk_action(
                    daily_pnl, monthly_dd,
                    dd_levels=config.DD_LEVELS,
                    safe_mode_release=config.SAFE_MODE_RELEASE_THRESHOLD)

                # Legacy guard (min with graduated)
                skip_buys, legacy_reason = guard.should_skip_rebalance(
                    daily_pnl, monthly_dd)

                buy_scale = risk_action["buy_scale"]
                if skip_buys:
                    buy_scale = 0.0  # legacy guard overrides

                # Final risk action log
                logger.info(
                    f"[RISK_ACTION] level={risk_action['level']} "
                    f"daily_block={skip_buys} buy_scale={buy_scale:.0%} "
                    f"trim={risk_action['trim_ratio']:.0%} "
                    f"safe_mode={risk_action['safe_mode']}")

                # ── Trail pre-check: HWM update only (no trigger) ────
                # Matches backtest order: trail evaluated before rebalance.
                # Live does NOT trigger here — EOD evaluates on official close.
                # Note: collector may not exist yet (initialized after rebalance)
                try:
                    rt_prices = collector.get_last_prices()
                except NameError:
                    rt_prices = {}  # collector not yet initialized (pre-rebalance)
                for _pc_code in list(portfolio.positions.keys()):
                    _pc_pos = portfolio.positions[_pc_code]
                    _pc_price = rt_prices.get(_pc_code, 0)
                    if _pc_price <= 0:
                        _pc_price = executor.get_live_price(_pc_code)
                    if _pc_price <= 0:
                        continue
                    # HWM update only
                    if _pc_price > _pc_pos.high_watermark:
                        _pc_pos.high_watermark = _pc_price
                    _pc_pos.trail_stop_price = calc_trail_stop_price(
                        _pc_pos.high_watermark, config.TRAIL_PCT)
                    # Warn if near trail trigger (EOD will decide)
                    if _pc_pos.high_watermark > 0:
                        _pc_dd = (_pc_price - _pc_pos.high_watermark) / _pc_pos.high_watermark
                        if _pc_dd <= -config.TRAIL_PCT:
                            logger.warning(
                                f"[TRAIL_PRECHECK_NEAR] {_pc_code}: dd={_pc_dd:.2%} "
                                f"price={_pc_price:,.0f} hwm={_pc_pos.high_watermark:,.0f} "
                                f"— will evaluate at EOD close (no intraday trigger)")

                # ── BuyPermission check (Phase 1 방어) ──
                guard.advance_recovery_state()  # 최대 1단계 전이
                permission, perm_reason = guard.get_buy_permission()
                guard.update_blocked_tracking(permission)
                logger.info(f"[BUY_PERMISSION] {permission.value}: {perm_reason or 'OK'}")

                if permission in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
                    # 리밸 전체 보류 — 포지션 유지, 주문 0건
                    logger.critical(f"[REBAL_{permission.value}] {perm_reason} — "
                                    f"리밸 보류, 포지션 유지, trail 금지")
                    need_rebalance = False
                    rt = state_mgr.load_runtime()
                    rt["rebal_deferred_reason"] = perm_reason
                    rt["rebal_deferred_date"] = today_str
                    state_mgr.save_runtime(rt)
                    if _KAKAO_OK and permission == BuyPermission.BLOCKED:
                        try: _kakao_buy_blocked(perm_reason)
                        except Exception: pass

                elif permission == BuyPermission.REDUCED:
                    buy_scale = buy_scale * 0.5
                    logger.warning(f"[BUY_REDUCED] {perm_reason} — "
                                   f"buy_scale={buy_scale:.0%}")

                # ── Wait for market open (09:00) before sending orders ──
                if need_rebalance and not is_market_hours():
                    logger.info("[MARKET_WAIT] Market not open yet — waiting for 09:00...")
                    while not is_market_hours():
                        time.sleep(5)
                    logger.info("[MARKET_WAIT] Market open — proceeding with rebalance orders")

                if not need_rebalance:
                    pass  # BLOCKED — skip to monitor
                else:
                    try:
                        pfail, pending_buys_list, sell_status = _execute_rebalance_live(
                            portfolio, target, config, executor, provider,
                            trade_logger, skip_buys, logger,
                            state_mgr=state_mgr, today_str=today_str,
                            buy_scale=buy_scale, risk_action=risk_action,
                            regime=session_regime,
                            mode_str=mode_label, tracker=tracker,
                            name_cache=_name_cache)
                        session_rebalance_executed = True
                        session_price_fail_count = pfail

                        # Mark trim executed (prevents same-day repeat)
                        if risk_action.get("trim_ratio", 0) > 0:
                            guard.mark_trim_executed(risk_action["level"])

                        # Commit order: sell_status determines commit behavior
                        # T+1 model: sells today, buys pending for next session
                        if sell_status == "COMPLETE":
                            portfolio_saved = _safe_save(
                                state_mgr, portfolio, context="rebalance_commit/portfolio")
                            if portfolio_saved:
                                state_mgr.save_pending_buys(pending_buys_list, sell_status)
                                state_mgr.set_last_rebalance_date(today_str)
                                logger.info(
                                    "[REBALANCE_COMMIT_OK] Portfolio saved, "
                                    "rebalance date marked: %s, "
                                    "pending buys: %d", today_str, len(pending_buys_list))
                                # Clear deferred state (BLOCKED → NORMAL recovery)
                                _rt = state_mgr.load_runtime()
                                if _rt.get("rebal_deferred_reason"):
                                    _rt.pop("rebal_deferred_reason", None)
                                    _rt.pop("rebal_deferred_date", None)
                                    state_mgr.save_runtime(_rt)
                                    logger.info("[DEFERRED_CLEARED] rebal deferred state cleaned up")
                                # paper_test: save fast reentry metadata
                                # shadow_test: skip — pending_buys_list is empty (dry-run)
                                if (mode_label.lower() in ("paper_test", "shadow_test")
                                        and config.PAPER_TEST_FAST_REENTRY
                                        and not config.SHADOW_MODE):
                                    _save_test_reentry_meta(
                                        state_mgr, config, today_str, logger)
                            else:
                                logger.critical(
                                    "[REBALANCE_COMMIT_PARTIAL_FAIL] Portfolio save failed! "
                                    "Rebalance date NOT marked — will retry next session.")
                        elif sell_status in ("PARTIAL", "UNCERTAIN"):
                            _safe_save(state_mgr, portfolio, context="rebalance_partial")
                            state_mgr.save_pending_buys(pending_buys_list, sell_status)
                            logger.warning(
                                f"[REBAL_COMMIT_DEFERRED] sell_status={sell_status} — "
                                f"rebal date NOT marked, "
                                f"pending buys saved but blocked until COMPLETE")
                            # paper_test: save reentry meta even for PARTIAL
                            if (mode_label.lower() in ("paper_test", "shadow_test")
                                    and config.PAPER_TEST_FAST_REENTRY
                                    and not config.SHADOW_MODE):
                                _save_test_reentry_meta(
                                    state_mgr, config, today_str, logger)
                        elif sell_status == "FAILED":
                            _safe_save(state_mgr, portfolio, context="rebalance_sell_failed")
                            logger.critical(
                                "[REBAL_SELL_FAILED] no sells filled — "
                                "rebalance aborted, no pending buys")
                    except Exception as e:
                        logger.error(f"Rebalance crashed: {e}", exc_info=True)
                        # Save portfolio (preserve sell results) but do NOT mark date
                        # → next session will retry rebalance
                        _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")
                        logger.info("Crash recovery: portfolio saved, "
                                    "rebalance date NOT marked (will retry)")
    else:
        logger.info(f"Not rebalance day (last: {last_rebal})")

    # ── Phase 2A: Emergency Rebalance Check (Ver.02 Strategy A) ──
    if config.EMERGENCY_REBAL_ENABLED and not session_monitor_only:
        try:
            emergency_executed = _check_emergency_rebalance(
                portfolio, config, executor, provider, trade_logger,
                state_mgr, guard, mode_label, logger,
                session_rebalance_executed=session_rebalance_executed)
            if emergency_executed:
                _safe_save(state_mgr, portfolio, context="emergency_rebal/commit")
                logger.info("[EMERGENCY_REBAL_COMMIT_OK] Portfolio saved after emergency trim")
        except Exception as e:
            logger.error(f"[EMERGENCY_REBAL_ERROR] {e}", exc_info=True)
            _safe_save(state_mgr, portfolio, context="emergency_rebal/crash_recovery")

    # ── Phase 2.5: Intraday Collector Setup ──────────────────────
    from data.intraday_collector import IntradayCollector
    if trading_mode == "paper_test":
        _intraday_dir = config.INTRADAY_DIR_TEST
    elif trading_mode == "shadow_test":
        _intraday_dir = config.INTRADAY_DIR_SHADOW
    else:
        _intraday_dir = config.INTRADAY_DIR
    collector = IntradayCollector(_intraday_dir,
                                  date.today().strftime("%Y-%m-%d"))
    collector.set_active_codes(list(portfolio.positions.keys()))

    # KOSPI index collector — uses polling (SetRealReg doesn't work for indices)
    _kospi_intraday_dir = _intraday_dir / "indices"
    kospi_collector = IntradayCollector(_kospi_intraday_dir,
                                        date.today().strftime("%Y-%m-%d"))
    kospi_collector.set_active_codes(["001"])  # KOSPI index code

    if portfolio.positions:
        provider.set_real_data_callback(collector.on_tick)
        provider.register_real(list(portfolio.positions.keys()), fids="10;27")
        logger.info("[Intraday] Real-time tick collection started "
                    f"for {len(portfolio.positions)} positions + KOSPI polling")

    # ── Snapshot replay data collectors (non-critical) ──────────────
    swing_collector = None
    micro_collector = None
    try:
        from data.microstructure_collector import MicrostructureCollector
        from data.swing_collector import SwingRankingCollector
        _swing_dir = config.BASE_DIR / "data" / "swing"
        _micro_dir = config.BASE_DIR / "data" / "micro"
        micro_collector = MicrostructureCollector(
            _micro_dir, date.today().strftime("%Y%m%d"), provider)
        swing_collector = SwingRankingCollector(
            _swing_dir, date.today().strftime("%Y%m%d"),
            provider, collector, micro_collector)
        provider.set_micro_callback(micro_collector.on_real_data)
        logger.info("[SWING] snapshot replay data collectors initialized")
        # FIX-001: Pre-market seed — force first ranking snapshot immediately
        try:
            swing_collector.seed_pre_market()
        except Exception as seed_err:
            logger.warning(f"[SWING] pre-market seed failed (non-critical): {seed_err}")
    except Exception as e:
        logger.warning(f"[SWING] init failed (non-critical): {e}")
        swing_collector = None
        micro_collector = None

    # ── Phase 3: Monitor Loop (HWM update + trail warning only) ────
    #    Trail stop EXECUTION happens at EOD (Phase 4) to match backtest
    #    (backtest uses daily close; live must also use EOD close).
    #    Intraday: update HWM, warn if near trigger, but do NOT execute.
    #    Monitor ends at MONITOR_END (15:20). EOD evaluates at EOD_EVAL (15:30).
    MONITOR_END_HOUR, MONITOR_END_MIN = 15, 20
    EOD_EVAL_HOUR, EOD_EVAL_MIN = 15, 30

    trail_warnings = set()  # codes warned during intraday
    monitor_price_fail_count = 0  # track price fetch failures during monitoring
    _prev_equity = 0.0           # equity stale detection
    _equity_stale_count = 0      # consecutive same-equity count

    # -- Ctrl+C graceful shutdown (FIX 6) --
    _stop_requested = False

    def _sigint_handler(sig, frame):
        nonlocal _stop_requested
        _stop_requested = True
        logger.info("[SIGINT] Stop requested — will exit after current cycle")

    prev_handler = signal.signal(signal.SIGINT, _sigint_handler)

    n_pos = len(portfolio.positions)
    if n_pos == 0:
        logger.info("No positions. Skipping monitor.")
    else:
        logger.info(f"Monitor: {n_pos} positions, 60s interval. Ctrl+C to stop.")
        logger.info("Trail stops evaluated at EOD close (matching backtest).")

        from PyQt5.QtCore import QEventLoop, QTimer

        # paper_test: one-shot fast reentry flag
        # sell_only: no reentry; buy_only: already done in Phase 1.5
        _test_reentry_done = (_cycle in ("sell_only", "buy_only"))

        try:
            cycle = 0
            while True:
                if _stop_requested:
                    logger.info("[MONITOR_STOP] Ctrl+C received — exiting loop")
                    break
                now = datetime.now()
                if now.hour > MONITOR_END_HOUR or (
                        now.hour == MONITOR_END_HOUR and now.minute >= MONITOR_END_MIN):
                    break

                # ── paper_test fast reentry: execute pending buys after delay ──
                # Check dirty flag (set by ghost settle sell_status upgrade)
                if not _test_reentry_done:
                    _rt_check = state_mgr.load_runtime()
                    if _rt_check.get("fast_reentry_dirty"):
                        _rt_check.pop("fast_reentry_dirty", None)
                        state_mgr.save_runtime(_rt_check)
                        logger.info("[FAST_REENTRY_DIRTY] sell_status upgrade "
                                    "detected — forcing immediate reentry check")

                if (mode_label.lower() in ("paper_test", "shadow_test")
                        and config.PAPER_TEST_FAST_REENTRY
                        and not _test_reentry_done):
                    _pre_reentry_codes = set(portfolio.positions.keys())
                    _test_reentry_done = _try_fast_reentry(
                        state_mgr, portfolio, config, executor, provider,
                        trade_logger, logger, mode_label, tracker=tracker,
                        name_cache=_name_cache, guard=guard)
                    # FIX-A8: fast_reentry로 추가된 종목을 collector에 등록
                    if _test_reentry_done:
                        _new_codes = set(portfolio.positions.keys()) - _pre_reentry_codes
                        if _new_codes and collector is not None:
                            collector.set_active_codes(list(portfolio.positions.keys()))
                            for _nc in _new_codes:
                                logger.info(f"[FAST_REENTRY_REGISTER] code={_nc}")

                if now.hour < 9:
                    # Qt-aware wait (allows real-time tick events to process)
                    pre_wait = QEventLoop()
                    QTimer.singleShot(60000, pre_wait.quit)
                    # Check stop_requested every 1s during pre-market wait
                    pre_check = QTimer()
                    pre_check.timeout.connect(
                        lambda lp=pre_wait: lp.quit() if _stop_requested else None)
                    pre_check.start(1000)
                    pre_wait.exec_()
                    pre_check.stop()
                    continue

                # Get live prices: prefer real-time ticks, fallback to TR
                rt_tick_prices = collector.get_last_prices()
                prices = {}
                n_realtime = 0
                n_fallback = 0
                cycle_fails = 0
                for code in list(portfolio.positions.keys()):
                    # 1st: real-time tick (most fresh)
                    if code in rt_tick_prices and rt_tick_prices[code] > 0:
                        prices[code] = rt_tick_prices[code]
                        n_realtime += 1
                    else:
                        # 2nd: TR fallback (GetMasterLastPrice)
                        p = executor.get_live_price(code)
                        if p > 0:
                            prices[code] = p
                            n_fallback += 1
                        else:
                            cycle_fails += 1
                if cycle_fails > 0:
                    monitor_price_fail_count += cycle_fails
                portfolio.update_prices(prices)

                # Intraday: HWM update + trail proximity warning
                for code in list(portfolio.positions.keys()):
                    pos = portfolio.positions.get(code)
                    if not pos or pos.current_price <= 0:
                        continue

                    # Update HWM only (no trigger)
                    if pos.current_price > pos.high_watermark:
                        pos.high_watermark = pos.current_price
                    pos.trail_stop_price = calc_trail_stop_price(
                        pos.high_watermark, config.TRAIL_PCT)

                    # Warn if within 2% of trail trigger
                    if pos.high_watermark > 0:
                        dd = (pos.current_price - pos.high_watermark) / pos.high_watermark
                        if dd <= -(config.TRAIL_PCT - 0.02) and code not in trail_warnings:
                            trail_warnings.add(code)
                            logger.warning(
                                f"TRAIL WARNING {code}: dd={dd:.2%}, "
                                f"hwm={pos.high_watermark:,.0f}, "
                                f"price={pos.current_price:,.0f}, "
                                f"trigger={pos.trail_stop_price:,.0f}")

                # Periodic logging + save + stale check
                cycle += 1
                if cycle % 5 == 0:
                    portfolio.check_stale_prices(threshold_sec=600)
                    summary = portfolio.summary()
                    cur_equity = summary['equity']
                    logger.info(
                        f"Monitor: equity={cur_equity:,.0f}, "
                        f"pos={summary['n_positions']}, "
                        f"daily={summary['daily_pnl']:.2%}, "
                        f"risk={summary['risk_mode']} "
                        f"[price: rt={n_realtime} fb={n_fallback} fail={cycle_fails}]")

                    # Equity stale detection
                    if _prev_equity > 0 and abs(cur_equity - _prev_equity) < 1.0:
                        _equity_stale_count += 1
                        if _equity_stale_count >= 3:
                            logger.warning(
                                f"[EQUITY_STALE] equity={cur_equity:,.0f} unchanged "
                                f"for {_equity_stale_count} cycles — "
                                f"price sources: rt={n_realtime} fb={n_fallback} fail={cycle_fails}")
                    else:
                        _equity_stale_count = 0
                    _prev_equity = cur_equity

                    trade_logger.log_equity(
                        summary["equity"], summary["cash"],
                        summary["n_positions"],
                        summary["daily_pnl"], summary["monthly_dd"],
                        risk_mode=summary["risk_mode"],
                        rebalance_executed=session_rebalance_executed,
                        price_fail_count=session_price_fail_count,
                        reconcile_corrections=reconcile_corrections,
                        monitor_only=session_monitor_only)
                    _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")

                # Qt-aware wait: allows real-time tick events to process
                # Check stop_requested every 1s to enable Ctrl+C during wait
                wait_loop = QEventLoop()
                QTimer.singleShot(60000, wait_loop.quit)
                check_timer = QTimer()
                check_timer.timeout.connect(
                    lambda lp=wait_loop: lp.quit() if _stop_requested else None)
                check_timer.start(1000)
                wait_loop.exec_()
                check_timer.stop()

                # Flush minute bars
                collector.check_and_flush()

                # KOSPI index polling (SetRealReg doesn't support indices)
                try:
                    _kospi_price = provider.get_kospi_close()
                    if _kospi_price and _kospi_price > 0:
                        kospi_collector.on_tick("001", _kospi_price, 0)
                except Exception:
                    pass
                kospi_collector.check_and_flush()

                # Swing/Micro snapshot replay collectors (fully isolated)
                if swing_collector:
                    try:
                        swing_collector.check_and_snapshot()
                    except Exception as _sw_e:
                        logger.warning(f"[SWING] snapshot error (ignored): {_sw_e}")
                if micro_collector:
                    try:
                        micro_collector.check_and_sample()
                    except Exception as _mc_e:
                        logger.warning(f"[MICRO] sample error (ignored): {_mc_e}")

        except KeyboardInterrupt:
            logger.info("Interrupted (Ctrl+C)")
        except Exception as e:
            logger.error(f"Monitor loop crashed: {e}", exc_info=True)
        finally:
            # Always save state + flush intraday + cleanup on monitor exit
            try:
                collector.flush_all()
                kospi_collector.flush_all()
                if swing_collector:
                    try:
                        swing_collector.flush()
                    except Exception as _e:
                        logger.warning(f"[SWING] flush error (ignored): {_e}")
                if micro_collector:
                    try:
                        micro_collector.flush()
                    except Exception as _e:
                        logger.warning(f"[MICRO] flush error (ignored): {_e}")
                provider.unregister_real()
                _safe_save(state_mgr, portfolio, context="monitor_exit")
                logger.info("[MONITOR_CLEANUP] state saved, realtime cleared, exit complete")
            except Exception as cleanup_err:
                logger.error(f"Monitor cleanup failed: {cleanup_err}")

    # Restore previous signal handler
    signal.signal(signal.SIGINT, prev_handler)

    # Merge monitor price failures into session total
    session_price_fail_count += monitor_price_fail_count
    if monitor_price_fail_count > 0:
        logger.warning(f"Monitor price failures: {monitor_price_fail_count} "
                       f"(total session: {session_price_fail_count})")

    # ── Phase 4: EOD — Trail Stop Execution (close-based) ────────
    #    Wait until 15:30 so closing prices are settled.
    now = datetime.now()
    eod_target = now.replace(hour=EOD_EVAL_HOUR, minute=EOD_EVAL_MIN, second=0)
    if now < eod_target and not _stop_requested:
        wait_ms = int(max(0, (eod_target - now).total_seconds()) * 1000)
        logger.info(f"Monitor ended. EOD pending — waiting until "
                    f"{EOD_EVAL_HOUR}:{EOD_EVAL_MIN:02d} ({wait_ms/1000:.0f}s)..."
                    f" (Ctrl+C to skip)")
        # Qt-aware wait with stop_requested check
        from PyQt5.QtCore import QEventLoop, QTimer
        eod_wait = QEventLoop()
        QTimer.singleShot(wait_ms, eod_wait.quit)
        eod_check = QTimer()
        eod_check.timeout.connect(
            lambda: eod_wait.quit() if _stop_requested else None)
        eod_check.start(1000)
        eod_wait.exec_()
        eod_check.stop()
    if _stop_requested:
        # Ctrl+C during market hours → no reliable close prices, skip EOD
        now_check = datetime.now()
        if now_check.hour < EOD_EVAL_HOUR or (
                now_check.hour == EOD_EVAL_HOUR and now_check.minute < EOD_EVAL_MIN):
            logger.info("[EOD_SKIP] Ctrl+C during market hours — skipping trail stop + EOD evaluation")
            _safe_save(state_mgr, portfolio, context="early_exit")
            state_mgr.mark_shutdown("sigint")
            provider.shutdown()
            signal.signal(signal.SIGINT, prev_handler)
            try:
                app.quit()
            except Exception:
                pass
            return
        else:
            logger.info("[EOD_SIGINT_LATE] Ctrl+C after market close — proceeding with trail stop evaluation (no new orders)")
    logger.info("EOD: evaluating trail stops on close prices...")

    # ── EOD close price resolution ──────────────────────────
    # Realtime feed is already disconnected (15:20).
    # Use intraday collector's last bar as close price source.
    logger.info("[EOD_PRICE_CONTEXT] realtime updates ended; "
                "using close-price evaluation for %d positions",
                len(portfolio.positions))

    eod_bars = {}
    try:
        eod_bars = collector.load_all_today()
    except Exception as e:
        logger.warning(f"[EOD_PRICE_CONTEXT] intraday load failed: {e}")

    today_str = date.today().strftime("%Y-%m-%d")
    eod_price_src = {}  # code -> (price, source)
    for code in list(portfolio.positions.keys()):
        # Priority 1: intraday collector last close (with date validation)
        if code in eod_bars and not eod_bars[code].empty:
            try:
                bar_df = eod_bars[code]
                last_close = float(bar_df["close"].iloc[-1])
                # Source date validation: intraday data must be from today
                bar_date_ok = True
                if "datetime" in bar_df.columns:
                    last_bar_date = str(bar_df["datetime"].iloc[-1])[:10]
                    if last_bar_date != today_str:
                        logger.warning(
                            f"[EOD_DATE_MISMATCH] {code}: bar_date={last_bar_date} "
                            f"!= today={today_str}, skipping intraday source")
                        bar_date_ok = False
                if bar_date_ok and last_close > 0:
                    eod_price_src[code] = (last_close, "intraday_last_close")
                    portfolio.positions[code].current_price = last_close
                    continue
            except Exception:
                pass
        # Priority 2: provider last cached price
        try:
            p = executor.get_live_price(code)
            if p > 0:
                eod_price_src[code] = (p, "provider_cached")
                portfolio.positions[code].current_price = p
                continue
        except Exception:
            pass
        # Priority 3: existing position price
        pos = portfolio.positions.get(code)
        if pos and pos.current_price > 0:
            eod_price_src[code] = (pos.current_price, "position_fallback")
        else:
            eod_price_src[code] = (0, "unavailable")

    # ── EOD Prefetch: re-query fb positions via GetMasterLastPrice ──
    # After market close (15:30+), GetMasterLastPrice returns official close.
    # Upgrade provider_cached/position_fallback → eod_master_close.
    prefetch_upgraded = 0
    prefetch_codes = [c for c, (_, src) in eod_price_src.items()
                      if src in ("provider_cached", "position_fallback", "unavailable")]
    if prefetch_codes:
        logger.info(f"[EOD_PREFETCH] Re-querying {len(prefetch_codes)} "
                    f"non-intraday positions for official close...")
        for code in prefetch_codes:
            try:
                p = provider.get_current_price(code)
                if p > 0:
                    eod_price_src[code] = (p, "eod_master_close")
                    portfolio.positions[code].current_price = p
                    prefetch_upgraded += 1
            except Exception as e:
                logger.warning(f"[EOD_PREFETCH_FAIL] {code}: {e}")
        if prefetch_upgraded:
            logger.info(f"[EOD_PREFETCH_OK] {prefetch_upgraded}/{len(prefetch_codes)} "
                        f"upgraded to eod_master_close")

    src_counts = {}
    for _, src in eod_price_src.values():
        src_counts[src] = src_counts.get(src, 0) + 1
    logger.info("[EOD_PRICE_SOURCE] %s", src_counts)

    # Trail stop check (close-based, same as backtest)
    # Result tracking: TRIGGERED → ORDER_SENT → FILLED / FAILED
    from report.reporter import make_event_id
    trail_triggered = 0
    trail_sent = 0
    trail_filled = 0
    trail_failed = 0

    for code in list(portfolio.positions.keys()):
        pos = portfolio.positions.get(code)
        price_info = eod_price_src.get(code, (0, "unavailable"))
        close_price, price_source = price_info

        if not pos or close_price is None or close_price <= 0:
            # Phase 1-A: unavailable도 trail_skip_days 누적 (감시 사각 방지)
            if pos:
                pos.trail_skip_days += 1
                if pos.trail_skip_days >= 3:
                    logger.critical(
                        f"[TRAIL_SKIP_UNAVAIL] {code}: {pos.trail_skip_days}d "
                        f"— price unavailable, trail stop unmonitored")
                else:
                    logger.warning(
                        f"[EOD_PRICE_MISSING] {code}: decision=SKIP_NO_PRICE "
                        f"(source={price_source}, skip_days={pos.trail_skip_days})")
            else:
                logger.warning(f"[EOD_PRICE_MISSING] {code}: pos=None")
            continue

        # [BEHAVIOR CHANGE] Only execute trail stop on verified close prices.
        # Cached/fallback prices may be stale → skip execution, log warning.
        if price_source in ("provider_cached", "position_fallback"):
            # Escalation: track consecutive skip days
            pos.trail_skip_days += 1
            if pos.trail_skip_days == 1:
                logger.warning(
                    f"[TRAIL_SKIP_1D] {code}: no official close "
                    f"(source={price_source}, price={close_price:,.0f})")
            elif pos.trail_skip_days == 2:
                logger.warning(
                    f"[TRAIL_SKIP_2D] {code}: ELEVATED — 2 consecutive days "
                    f"without official close")
            elif pos.trail_skip_days >= 3:
                logger.critical(
                    f"[TRAIL_DISABLED_BY_DATA] {code}: {pos.trail_skip_days}d "
                    f"— trail stop protection inactive")
            # Still update HWM for observability, but do NOT trigger exit
            if close_price > pos.high_watermark:
                pos.high_watermark = close_price
            continue

        triggered, new_hwm, exit_price = check_trail_stop(
            pos.high_watermark, close_price, config.TRAIL_PCT)
        pos.high_watermark = new_hwm
        pos.trail_skip_days = 0  # reset: official close received

        stop_price = calc_trail_stop_price(new_hwm, config.TRAIL_PCT)
        decision = "EXIT" if triggered else "HOLD"
        logger.info(f"[EOD_TRAIL_CHECK] {code}: close={close_price:,.0f}, "
                     f"hwm={new_hwm:,.0f}, stop={stop_price:,.0f}, "
                     f"decision={decision}, source={price_source}")

        if triggered:
            trail_triggered += 1
            logger.warning(f"[EOD_TRAIL_EXIT] {code}: close={close_price:,.0f} "
                            f"<= stop={stop_price:,.0f} (source={price_source})")
            eid = make_event_id(code, "TRAIL_STOP")

            # Decision log: trail stop context
            hold_days = 0
            if pos.entry_date and "-" in pos.entry_date:
                try:
                    hold_days = (date.today() - datetime.strptime(
                        pos.entry_date, "%Y-%m-%d").date()).days
                except ValueError:
                    pass
            trade_logger.log_decision_sell(
                code, "TRAIL_STOP_TRIGGERED",
                price=pos.current_price,
                high_watermark=new_hwm,
                trail_stop_price=calc_trail_stop_price(new_hwm, config.TRAIL_PCT),
                pnl_pct=pos.unrealized_pnl_pct,
                hold_days=hold_days,
                event_id=eid,
                regime=session_regime)

            # Order attempt — skip if Ctrl+C (evaluate-only mode)
            if _stop_requested:
                logger.warning(f"[EOD_TRAIL_EXIT_DEFERRED] {code}: Ctrl+C — "
                               f"trail stop triggered but order deferred to next session")
                continue
            trail_sent += 1
            result = executor.execute_sell(code, pos.quantity, "TRAIL_STOP")
            if not result.get("error"):
                trail_filled += 1
                fill_price = result["exec_price"] or pos.current_price
                exec_qty = result.get("exec_qty", pos.quantity)
                logger.info(f"[PORTFOLIO] TRAIL_STOP {code} requested={pos.quantity} "
                            f"exec_qty={exec_qty} fill_price={fill_price:,.0f}")
                trade = portfolio.remove_position(code, fill_price, config.SELL_COST,
                                                  qty=exec_qty)
                if trade:
                    trade["exit_reason"] = "TRAIL_STOP"
                    _hwm_pct = (pos.high_watermark / pos.avg_price - 1) if pos.avg_price > 0 else 0
                    trade_logger.log_close(trade, "TRAIL_STOP_FILLED",
                                            mode_label,
                                            event_id=eid,
                                            entry_rank=pos.entry_rank,
                                            score_mom=pos.score_mom,
                                            max_hwm_pct=_hwm_pct)
                logger.info(f"TRAIL_STOP_FILLED {code}: price={fill_price:,.0f}")
                _drop = (fill_price / pos.high_watermark - 1) if pos.high_watermark > 0 else 0
                _notify_trail(code, _name_cache, fill_price, pos.high_watermark, _drop,
                             qty=exec_qty, avg_price=pos.avg_price)
                collector.mark_sold(code)
                _safe_save(state_mgr, portfolio, context=f"trail_stop/{code}")
            else:
                trail_failed += 1
                logger.error(f"TRAIL_STOP_FAILED {code}: {result['error']}")

    if trail_triggered > 0:
        logger.info(f"Trail stop summary: triggered={trail_triggered}, "
                    f"sent={trail_sent}, filled={trail_filled}, "
                    f"failed={trail_failed}")

    # ── EOD Intraday Cleanup ──────────────────────────────────
    collector.flush_all()
    kospi_collector.flush_all()
    if swing_collector:
        try:
            swing_collector.flush()
        except Exception as _e:
            logger.warning(f"[SWING] EOD flush error (ignored): {_e}")
    if micro_collector:
        try:
            micro_collector.flush()
        except Exception as _e:
            logger.warning(f"[MICRO] EOD flush error (ignored): {_e}")
    provider.unregister_real()
    provider.set_real_data_callback(None)

    # ── EOD KOSPI/KOSDAQ close fetch (before log_equity) ──────────
    kospi_close_val = 0.0
    kosdaq_close_val = 0.0
    try:
        kospi_close_val = provider.get_kospi_close()
        if kospi_close_val > 0:
            from report.kospi_utils import inject_kospi_close
            inject_kospi_close(config.INDEX_FILE,
                               date.today().strftime("%Y-%m-%d"), kospi_close_val)
            logger.info(f"KOSPI close injected: {kospi_close_val:.2f}")
    except Exception as e:
        logger.warning(f"KOSPI fetch failed: {e} (non-critical)")

    try:
        kosdaq_close_val = provider.get_kosdaq_close()
        if kosdaq_close_val > 0:
            logger.info(f"KOSDAQ close fetched: {kosdaq_close_val:.2f}")
    except Exception as e:
        logger.warning(f"KOSDAQ fetch failed: {e} (non-critical)")

    # ── EOD KOSPI minute bars (for intraday chart overlay) ──────────
    try:
        import json as _json
        kospi_bars = provider.get_index_minute_bars("001")
        if kospi_bars:
            today_compact = date.today().strftime("%Y%m%d")
            kb_path = _report_dir / f"kospi_minute_{today_compact}.json"
            kb_path.write_text(_json.dumps(kospi_bars, ensure_ascii=False), encoding="utf-8")
            logger.info(f"KOSPI minute bars saved: {len(kospi_bars)} bars -> {kb_path}")
    except Exception as e:
        logger.warning(f"KOSPI minute bars fetch failed: {e} (non-critical)")

    # ── EOD equity snapshot (BEFORE end_of_day to capture correct daily_pnl) ──
    summary = portfolio.summary()
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"],
        risk_mode=summary["risk_mode"],
        rebalance_executed=session_rebalance_executed,
        price_fail_count=session_price_fail_count,
        reconcile_corrections=reconcile_corrections,
        monitor_only=session_monitor_only,
        kospi_close=kospi_close_val,
        kosdaq_close=kosdaq_close_val,
        regime=session_regime,
        kospi_ma200=session_kospi_ma200,
        breadth=session_breadth)

    portfolio.end_of_day()
    _safe_save(state_mgr, portfolio, context="EOD")

    # ── EOD Reports, Analysis, Settlement ──────────────────────
    # Wrapped in try/finally so shutdown cleanup (mark_shutdown,
    # provider.shutdown, app.quit) runs even if reports/settlement fail.
    try:
        # ── EOD Daily Report: open positions snapshot ──────────────
        trade_logger.log_daily_positions(
            portfolio.positions,
            buy_cost=config.BUY_COST,
            sell_cost=config.SELL_COST)

        # ── EOD Intraday Analysis + Daily HTML Report ────────────
        # Use same report_dir as TradeLogger (output_test/ for paper_test)
        _eod_report_dir = _report_dir
        intraday_summary = None
        try:
            from report.intraday_analyzer import (
                analyze_all as ia_analyze_all,
                generate_summary as ia_generate_summary,
                save_json as ia_save_json,
                save_csv as ia_save_csv,
                extract_prev_closes as ia_prev_closes,
            )
            from data.intraday_collector import IntradayCollector
            today_date_str = date.today().strftime("%Y-%m-%d")
            ia_bars = IntradayCollector.load_all_for_date(
                _intraday_dir, today_date_str)
            if ia_bars:
                prev_closes = ia_prev_closes(
                    _intraday_dir, today_date_str, list(ia_bars.keys()))
                ia_results = ia_analyze_all(ia_bars, prev_closes)
                intraday_summary = ia_generate_summary(ia_results, today_date_str)
                ia_save_json(intraday_summary, _eod_report_dir, today_date_str)
                ia_save_csv(intraday_summary, _eod_report_dir, today_date_str)
                logger.info(f"[INTRADAY_ANALYSIS] {intraday_summary['n_stocks']} stocks, "
                            f"risk_score={intraday_summary.get('risk_score', 'N/A')}, "
                            f"worst_dd={intraday_summary.get('worst_dd_pct', 0):.2f}%")
            else:
                logger.info("[INTRADAY_ANALYSIS] No intraday data for today")
        except Exception as e:
            logger.warning(f"Intraday analysis failed: {e} (non-critical)")

        try:
            from report.daily_report import generate_daily_report as gen_daily
            rpt_path = gen_daily(_eod_report_dir, config,
                                  intraday_dir=_intraday_dir,
                                  intraday_summary=intraday_summary,
                                  eod_price_src=eod_price_src)
            if rpt_path:
                logger.info(f"Daily report: {rpt_path}")
        except Exception as e:
            logger.warning(f"Daily report generation failed: {e} (non-critical)")

        # ── Weekly Report (Friday EOD) ─────────────────────────
        if date.today().weekday() == 4:  # Friday
            try:
                from report.weekly_report import generate_weekly_report
                wrpt = generate_weekly_report(_eod_report_dir, config)
                if wrpt:
                    logger.info(f"Weekly report: {wrpt}")
            except Exception as e:
                logger.warning(f"Weekly report generation failed: {e} (non-critical)")

        # ── Monthly Report (last trading day of month) ─────────
        tomorrow = date.today() + timedelta(days=1)
        if tomorrow.month != date.today().month:
            try:
                from report.monthly_report import generate_monthly_report
                mrpt = generate_monthly_report(_eod_report_dir, config)
                if mrpt:
                    logger.info(f"Monthly report: {mrpt}")
            except Exception as e:
                logger.warning(f"Monthly report generation failed: {e} (non-critical)")

        # Order summary
        order_sum = tracker.summary()
        logger.info(f"Orders: {order_sum}")

        # Ghost check
        ghosts = provider.get_ghost_orders()
        if ghosts:
            logger.critical(f"GHOST ORDERS: {len(ghosts)} unresolved! Check HTS!")
            for g in ghosts:
                logger.critical(f"  {g['side']} {g['code']} qty={g['requested_qty']} status={g['status']}")

        # ── EOD: Settle PENDING_EXTERNAL orders via broker snapshot ────
        if tracker:
            from runtime.order_tracker import OrderStatus
            pending_ext = [r for r in tracker._orders.values()
                           if r.status == OrderStatus.PENDING_EXTERNAL]
            if pending_ext:
                logger.info(f"[EOD_SETTLE] {len(pending_ext)} PENDING_EXTERNAL orders to settle")
                try:
                    snap = provider.query_account_summary()
                    broker_holdings = {h["code"]: h for h in snap.get("holdings", [])}
                    for rec in pending_ext:
                        broker_pos = broker_holdings.get(rec.code)
                        broker_qty = broker_pos.get("qty", broker_pos.get("quantity", 0)) if broker_pos else 0
                        broker_avg = broker_pos.get("avg_price", 0) if broker_pos else 0
                        logger.info(
                            f"[EOD_SETTLE_INPUT] {rec.side} {rec.code}: "
                            f"broker_pos={'found' if broker_pos else 'MISSING'} "
                            f"broker_qty={broker_qty} broker_avg={broker_avg} "
                            f"requested={rec.quantity} base_qty={getattr(rec, 'base_qty', 'N/A')}")

                        if rec.side == "BUY":
                            base_qty = getattr(rec, 'base_qty', None)
                            if base_qty is not None:
                                delta_qty = max(0, broker_qty - base_qty)
                                if broker_qty < base_qty:
                                    logger.warning(
                                        f"[RECON_NEGATIVE_DELTA] BUY {rec.code}: "
                                        f"broker={broker_qty} < base={base_qty} — clamped to 0")
                            else:
                                delta_qty = broker_qty
                                logger.warning(
                                    f"[RECON_NO_BASE_QTY] BUY {rec.code}: "
                                    f"base_qty missing, using broker_qty as-is")

                            if delta_qty > rec.quantity * 2:
                                logger.error(
                                    f"[RECON_ANOMALY] BUY {rec.code}: "
                                    f"delta={delta_qty} > 2x requested={rec.quantity}")

                            if delta_qty >= rec.quantity:
                                terminal, final_qty = "FILLED", rec.quantity
                            elif 0 < delta_qty < rec.quantity:
                                terminal, final_qty = "FILLED", delta_qty
                                logger.warning(
                                    f"[RECON_PARTIAL_BUY] {rec.code}: "
                                    f"delta={delta_qty} < requested={rec.quantity}")
                            else:
                                terminal, final_qty = "CANCELLED", 0

                            logger.info(
                                f"[RECON_DECISION] BUY {rec.code} broker_qty={broker_qty} "
                                f"base_qty={base_qty} delta={delta_qty} "
                                f"expected={rec.quantity} → {terminal} (qty={final_qty})")
                            tracker.mark_reconcile_settled(
                                rec.order_id, final_qty, broker_avg, terminal)

                        elif rec.side == "SELL":
                            # EOD SELL settle: use base_qty (pre-order holdings)
                            # to compute how many shares were actually sold.
                            #   delta = base_qty - broker_qty_after
                            # Mirrors BUY logic symmetrically:
                            #   BUY:  delta = broker_qty_after - base_qty  (increase)
                            #   SELL: delta = base_qty - broker_qty_after  (decrease)
                            base_qty = getattr(rec, 'base_qty', None)
                            if base_qty is not None:
                                sold_qty = max(0, base_qty - broker_qty)
                                if broker_qty > base_qty:
                                    logger.warning(
                                        f"[RECON_NEGATIVE_DELTA] SELL {rec.code}: "
                                        f"broker={broker_qty} > base={base_qty} "
                                        f"— clamped to 0 (sell may not have executed)")
                            else:
                                # Fallback: legacy path (no base_qty)
                                sold_qty = max(0, rec.quantity - broker_qty)
                                logger.warning(
                                    f"[RECON_NO_BASE_QTY] SELL {rec.code}: "
                                    f"base_qty missing, using requested-broker fallback")

                            if broker_qty < 0:
                                logger.error(
                                    f"[RECON_ANOMALY] SELL {rec.code}: "
                                    f"broker_qty={broker_qty} < 0 — data error")
                            if sold_qty <= 0:
                                terminal, final_qty = "CANCELLED", 0
                                logger.warning(
                                    f"[RECON_DECISION] SELL {rec.code} "
                                    f"base_qty={base_qty} broker_qty={broker_qty} "
                                    f"delta=0 → CANCELLED")
                            elif sold_qty >= rec.quantity:
                                terminal, final_qty = "FILLED", rec.quantity
                                logger.info(
                                    f"[RECON_DECISION] SELL {rec.code} "
                                    f"base_qty={base_qty} broker_qty={broker_qty} "
                                    f"delta={sold_qty} requested={rec.quantity} → FILLED")
                            else:
                                terminal, final_qty = "FILLED", sold_qty
                                logger.info(
                                    f"[RECON_DECISION] SELL {rec.code} "
                                    f"base_qty={base_qty} broker_qty={broker_qty} "
                                    f"delta={sold_qty} requested={rec.quantity} "
                                    f"→ FILLED (partial)")
                            tracker.mark_reconcile_settled(
                                rec.order_id, final_qty, rec.exec_price, terminal)

                    executor._try_upgrade_sell_status()
                    if state_mgr:
                        state_mgr.clear_pending_external()
                    logger.info("[EOD_SETTLE] PENDING_EXTERNAL settlement complete")
                except Exception as e:
                    logger.error(f"[EOD_SETTLE_ERROR] {e}", exc_info=True)

    finally:
        # Shutdown cleanup: each step individually protected
        # so one failure does not skip subsequent steps.
        try:
            state_mgr.mark_shutdown("eod_complete")
        except Exception as e:
            logger.error(f"[SHUTDOWN_CLEANUP_FAIL] mark_shutdown: {e}")

        try:
            provider.shutdown()
        except Exception as e:
            logger.error(f"[SHUTDOWN_CLEANUP_FAIL] provider.shutdown: {e}")

        logger.info("=" * 40)
        logger.info("  EOD complete.")
        logger.info("=" * 40)

        try:
            app.quit()
        except Exception:
            pass


# [REMOVED] _reconcile_with_broker — use lifecycle.reconcile (imported line 52)


# [REMOVED] _execute_dd_trim — moved to risk.risk_management
from risk.risk_management import _execute_dd_trim
# [REMOVED] _check_emergency_rebalance — moved to risk.risk_management
from risk.risk_management import _check_emergency_rebalance


def _execute_pending_buys(portfolio, pending_buys, config, executor,
                          provider, trade_logger, state_mgr, logger,
                          mode_label, tracker=None, name_cache=None):
    """Execute pending buy orders from previous session (T+1 model).

    Broker cash sync first, then each buy with cash hard cap.
    Returns: (success_count, fail_count)
    """
    # Shadow mode: dry-run only — log intent, never send to executor
    if config.SHADOW_MODE:
        for pb in pending_buys:
            logger.info(
                f"[SHADOW_PENDING_BUY] {pb['ticker']}: "
                f"amount={pb.get('target_amount', 0):,.0f} — DRY RUN (not executed)"
            )
        logger.info(f"[SHADOW_PENDING_BUY] {len(pending_buys)} buys skipped (shadow mode)")
        return (0, 0)

    # Broker cash sync: provider is truth
    cash_source = "LOCAL"
    extra_buffer = 1.0
    try:
        summary = provider.query_account_summary()
        if not summary.get("error"):
            broker_cash = summary.get("available_cash", 0)
            if broker_cash > 0:
                cash_source = "BROKER_SYNC"
                if abs(broker_cash - portfolio.cash) / max(portfolio.cash, 1) > 0.05:
                    logger.warning(
                        f"[CASH_DIVERGENCE] broker={broker_cash:,.0f} "
                        f"local={portfolio.cash:,.0f} "
                        f"diff={broker_cash - portfolio.cash:,.0f}")
                    portfolio.cash = broker_cash  # broker is truth
    except Exception as e:
        logger.warning(f"[BROKER_CASH_FETCH_FAIL] {e} — using local cash")

    if cash_source != "BROKER_SYNC":
        logger.warning(
            f"[BUY_REDUCED_STALE_CASH] cash_source={cash_source} "
            f"— applying 80% extra buffer")
        extra_buffer = 0.80

    _pb_success = 0
    _pb_fail = 0
    _pb_failed_tickers = []

    logger.info(
        f"[PENDING_BUY_EXEC] {len(pending_buys)} buys, "
        f"cash={portfolio.cash:,.0f}, source={cash_source}")
    logger.info(f"[TRACKER_WIRED] pending_buys tracker={'yes' if tracker else 'no'}")

    # BUY 재진입 보호: PENDING_EXTERNAL BUY 존재 시 스킵
    _pending_ext_buy_codes = set()
    if tracker:
        from runtime.order_tracker import OrderStatus
        _pending_ext_buy_codes = {r.code for r in tracker._orders.values()
                                  if r.status == OrderStatus.PENDING_EXTERNAL
                                  and r.side == "BUY"}
        if _pending_ext_buy_codes:
            logger.info(f"[PENDING_EXT_BUY_GUARD] blocking codes: {_pending_ext_buy_codes}")

    for pb in pending_buys:
        ticker = pb["ticker"]
        if ticker in _pending_ext_buy_codes:
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: reason=PENDING_EXTERNAL — "
                           f"ghost/reconcile pending, skip to avoid duplicate")
            continue
        # Guard: RECON may have added this position via BROKER_ONLY
        if ticker in portfolio.positions:
            existing_qty = portfolio.positions[ticker].quantity
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: reason=EXISTING_POSITION "
                           f"qty={existing_qty} — likely RECON BROKER_ONLY, skip")
            continue
        live_price = executor.get_live_price(ticker)
        if live_price <= 0:
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: no price")
            continue

        # Cash hard cap
        available = max(0, portfolio.cash * config.CASH_BUFFER_RATIO * extra_buffer)
        max_qty = int(available / (live_price * (1 + config.BUY_COST)))

        # Target qty
        target_qty = int(pb["target_amount"] / (live_price * (1 + config.BUY_COST)))
        final_qty = min(target_qty, max_qty)

        if final_qty <= 0:
            reason = "INSUFFICIENT_CASH" if max_qty <= 0 else "REPRICE_REDUCED_QTY"
            logger.warning(
                f"[PENDING_BUY_SKIP] {ticker}: {reason} "
                f"available={available:,.0f} price={live_price:,.0f} "
                f"target_qty={target_qty} max_qty={max_qty}")
            continue

        if final_qty < target_qty:
            logger.info(
                f"[PENDING_BUY_CAPPED] {ticker}: {target_qty} -> {final_qty}")

        result = executor.execute_buy(ticker, final_qty, "REBALANCE_ENTRY")
        if not result.get("error"):
            fill_price = result["exec_price"] or live_price
            applied_qty = result["exec_qty"]
            logger.info(f"[PORTFOLIO] PENDING_BUY {ticker} requested={final_qty} "
                        f"exec_qty={applied_qty} fill_price={fill_price:,.0f}")
            portfolio.add_position(
                ticker, applied_qty, fill_price,
                entry_date=str(date.today()),
                buy_cost=config.BUY_COST)
            # Inject entry-time metadata (observation only)
            _new_pos = portfolio.positions.get(ticker)
            if _new_pos:
                _new_pos.entry_rank = pb.get("rank", 0)
                _new_pos.score_mom = pb.get("score_mom", 0.0)
            _notify_buy(ticker, name_cache or {}, applied_qty, fill_price)
            _safe_save(state_mgr, portfolio, context=f"pending_buy/{ticker}")
            _pb_success += 1
        else:
            error_str = result.get("error", "")
            _pb_fail += 1
            _pb_failed_tickers.append(ticker)
            if "TIMEOUT_UNCERTAIN" in error_str:
                logger.warning(f"[PENDING_BUY_PENDING_EXTERNAL] {ticker}: "
                               f"timeout — ghost/reconcile will resolve")
            else:
                logger.error(f"[PENDING_BUY_FAILED] {ticker}: {error_str}")

    logger.info(f"[PENDING_BUY_COMPLETE] success={_pb_success} fail={_pb_fail} "
                f"cash_remaining={portfolio.cash:,.0f}")
    if _pb_failed_tickers:
        logger.info(f"[PENDING_BUY_FAILED_TICKERS] {_pb_failed_tickers}")
    return (_pb_success, _pb_fail)


def _execute_rebalance_live(portfolio, target, config, executor, provider,
                             trade_logger, skip_buys, logger,
                             state_mgr=None, today_str="",
                             buy_scale: float = 1.0,
                             risk_action: dict = None,
                             regime: str = "",
                             mode_str: str = "LIVE",
                             tracker=None,
                             name_cache=None) -> tuple:
    """Execute rebalance sells, generate pending buys for T+1.

    Returns: (price_fail_count, pending_buys_list, sell_status)
      - price_fail_count: int
      - pending_buys_list: list of dicts for next-session execution
      - sell_status: "COMPLETE" | "PARTIAL" | "FAILED"
    """
    from strategy.rebalancer import compute_orders
    from report.reporter import make_event_id

    # -- Rebalance dedup: reject if already executed today --
    # paper_test: skip dedup (force_rebalance allows same-day repeat)
    if state_mgr and today_str and mode_str not in ("paper_test", "shadow_test"):
        runtime_check = state_mgr.load_runtime()
        if runtime_check.get("last_rebalance_date") == today_str:
            logger.warning("Rebalance already recorded today (%s) — SKIP", today_str)
            return 0, [], "COMPLETE"

    target_tickers = target["target_tickers"]
    scores = target.get("scores", {})

    # Get live prices for all involved stocks (track failures)
    all_codes = set(portfolio.positions.keys()) | set(target_tickers)
    prices = {}
    price_fail_codes = []
    for code in all_codes:
        p = executor.get_live_price(code)
        if p > 0:
            prices[code] = p
        else:
            price_fail_codes.append(code)
            logger.warning(f"Price fetch failed: {code} -> {p} (will skip if buy)")
    if price_fail_codes:
        logger.warning(f"Price failures: {len(price_fail_codes)}/{len(all_codes)} "
                       f"codes: {price_fail_codes}")
    portfolio.update_prices(prices)

    # mode_str passed from caller (PAPER or LIVE)

    sell_orders, buy_orders = compute_orders(
        current_positions={code: {"quantity": pos.quantity, "avg_price": pos.avg_price}
                           for code, pos in portfolio.positions.items()},
        target_tickers=target_tickers,
        total_equity=portfolio.get_current_equity(),
        current_cash=portfolio.cash,
        buy_cost=config.BUY_COST,
        sell_cost=config.SELL_COST,
        prices=prices,
        cash_buffer=config.CASH_BUFFER_RATIO)

    # ── Execute Sells First ──────────────────────────────────────
    logger.info(f"Sells: {len(sell_orders)} orders")
    sell_results = []  # track per-order status for sell_status
    for order in sell_orders:
        pos = portfolio.positions.get(order.ticker)
        eid = make_event_id(order.ticker, "SELL")

        # Decision log: sell context
        if pos:
            trade_logger.log_decision_sell(
                order.ticker, "REBALANCE_EXIT",
                price=prices.get(order.ticker, 0),
                high_watermark=pos.high_watermark,
                trail_stop_price=pos.trail_stop_price,
                pnl_pct=pos.unrealized_pnl_pct,
                hold_days=(date.today() - datetime.strptime(
                    pos.entry_date, "%Y-%m-%d").date()).days
                    if pos.entry_date and "-" in pos.entry_date else 0,
                event_id=eid,
                regime=regime)

        # Shadow mode: dry-run only (no orders)
        if config.SHADOW_MODE:
            _price = prices.get(order.ticker, 0)
            logger.info(f"[SHADOW_SELL] {order.ticker}: qty={order.quantity}, "
                        f"price={_price:,.0f}, "
                        f"value={order.quantity * _price:,.0f} — DRY RUN")
            sell_results.append("SHADOW")
            continue

        result = executor.execute_sell(order.ticker, order.quantity, "REBALANCE_EXIT")
        if not result.get("error"):
            fill_price = result["exec_price"] or prices.get(order.ticker, 0)
            exec_qty = result.get("exec_qty", order.quantity)
            logger.info(f"[PORTFOLIO] SELL {order.ticker} requested={order.quantity} "
                        f"exec_qty={exec_qty} fill_price={fill_price:,.0f}")
            # Capture entry metadata before remove_position (may delete from dict)
            _pre_pos = portfolio.positions.get(order.ticker)
            _er = _pre_pos.entry_rank if _pre_pos else 0
            _sm = _pre_pos.score_mom if _pre_pos else 0.0
            _hwm_p = ((_pre_pos.high_watermark / _pre_pos.avg_price - 1)
                      if _pre_pos and _pre_pos.avg_price > 0 else 0)
            trade = portfolio.remove_position(order.ticker, fill_price, config.SELL_COST,
                                              qty=exec_qty)
            if trade:
                trade["exit_reason"] = "REBALANCE_EXIT"
                trade_logger.log_close(trade, "REBALANCE_EXIT", mode_str,
                                       event_id=eid,
                                       entry_rank=_er,
                                       score_mom=_sm,
                                       max_hwm_pct=_hwm_p)
            _pnl = trade.get("pnl_pct", 0.0) if trade else 0.0
            _avg = _pre_pos.avg_price if _pre_pos else 0.0
            _notify_sell(order.ticker, name_cache or {}, exec_qty, fill_price, _pnl, "REBALANCE",
                         avg_price=_avg)
            _sell_status = "FILLED" if exec_qty >= order.quantity else "PARTIAL"
            sell_results.append(_sell_status)
            # Crash-recovery checkpoint: save known state so far.
            # This is NOT a fill confirmation — it records the portfolio
            # as understood at this moment for restart recovery purposes.
            if state_mgr:
                _safe_save(state_mgr, portfolio,
                           context=f"rebalance_sell/{order.ticker}/{_sell_status}")
        else:
            error_str = result.get("error", "")
            if "TIMEOUT_UNCERTAIN" in error_str:
                logger.warning(f"[SELL_PENDING_EXTERNAL] {order.ticker}: "
                               f"timeout — ghost/reconcile will resolve")
                sell_results.append("PENDING")
                if state_mgr:
                    _safe_save(state_mgr, portfolio,
                               context=f"rebalance_sell/{order.ticker}/PENDING")
            else:
                logger.error(f"SELL failed {order.ticker}: {error_str}")
                sell_results.append("FAILED")

    # Post-sell checkpoint: save portfolio but NOT rebalance date
    if state_mgr:
        _safe_save(state_mgr, portfolio, context="rebalance_post_sell_checkpoint")
        logger.info("Post-sell checkpoint saved (rebalance date NOT yet marked)")

    # ── Sell status determination (tracker-based) ─────────────────
    pending_sells = []
    if tracker:
        pending_sells = [r for r in tracker.pending_today()
                         if r.side == "SELL" and r.reason == "REBALANCE_EXIT"]
    if pending_sells:
        sell_status = "PARTIAL"  # PENDING_EXTERNAL → PARTIAL until settled
    elif not sell_orders:
        sell_status = "COMPLETE"  # no sells needed
    elif all(s in ("FILLED", "SHADOW") for s in sell_results):
        sell_status = "COMPLETE"
    elif any(s in ("FILLED", "PENDING", "PARTIAL") for s in sell_results):
        # PARTIAL = partial fill (exec_qty < order.quantity) → treat as PARTIAL,
        # not FAILED. Pending buys will be generated and committed as deferred.
        sell_status = "PARTIAL"
    else:
        sell_status = "FAILED"

    logger.info(f"[REBAL_SELL_STATUS] {sell_status} "
                f"(filled={sell_results.count('FILLED')}, "
                f"partial={sell_results.count('PARTIAL')}, "
                f"pending={sell_results.count('PENDING')}, "
                f"failed={sell_results.count('FAILED')}, "
                f"pending_ext={len(pending_sells)})")

    time.sleep(2)  # Brief pause after sells

    # ── DD Graduated: Position Trim (before pending buy generation) ──
    if risk_action and risk_action.get("trim_ratio", 0) > 0:
        trim_ratio = risk_action["trim_ratio"]
        level = risk_action["level"]
        logger.warning(f"[DD_TRIM_START] {level}: trimming {trim_ratio:.0%} of all positions")
        _execute_dd_trim(portfolio, trim_ratio, executor, config,
                         trade_logger, mode_str, logger)

    # ── Generate Pending Buys (T+1 model) ─────────────────────────
    # Buys are NOT executed today — saved for next session open
    pending_buys_list = []

    if skip_buys or buy_scale <= 0:
        reason = risk_action["level"] if risk_action else "DD_GUARD"
        logger.warning(f"[DD_GUARD_TRIGGERED] {reason}: buys BLOCKED (no pending buys)")
        # Save blocked buys for later reentry (DD may clear during session)
        dd_blocked_list = []
        for rank_idx, order in enumerate(buy_orders, 1):
            dd_blocked_list.append({
                "ticker": order.ticker,
                "target_amount": order.target_amount,
                "original_amount": order.target_amount,
                "score_vol": scores.get(order.ticker, {}).get("vol_12m", 0),
                "score_mom": scores.get(order.ticker, {}).get("mom_12_1", 0),
                "rank": rank_idx,
                "signal_date": today_str,
            })
        if dd_blocked_list:
            _rt = state_mgr.load_runtime()
            _rt["dd_blocked_buys"] = dd_blocked_list
            _rt["dd_blocked_meta"] = {
                "cycle_id": _rt.get("test_cycle_id", today_str),
                "created_at": datetime.now().isoformat(),
                "sell_status": sell_status,
            }
            state_mgr.save_runtime(_rt)
            logger.info(f"[DD_BLOCKED_BUYS_SAVED] {len(dd_blocked_list)} buys "
                        f"saved for reentry if DD clears "
                        f"(cycle={_rt['dd_blocked_meta']['cycle_id']})")
    else:
        if buy_scale < 1.0:
            logger.info(f"[DD_BUY_SCALED] buy allocation * {buy_scale:.0%}")

        for rank_idx, order in enumerate(buy_orders, 1):
            scaled_amount = order.target_amount * buy_scale
            if config.SHADOW_MODE:
                _price = prices.get(order.ticker, 0)
                _qty = int(scaled_amount / _price) if _price > 0 else 0
                logger.info(f"[SHADOW_BUY] {order.ticker}: amount={scaled_amount:,.0f}, "
                            f"est_qty={_qty}, price={_price:,.0f} — DRY RUN")
                continue  # shadow: do not queue for execution
            pending_buys_list.append({
                "ticker": order.ticker,
                "target_amount": scaled_amount,
                "score_vol": scores.get(order.ticker, {}).get("vol_12m", 0),
                "score_mom": scores.get(order.ticker, {}).get("mom_12_1", 0),
                "rank": rank_idx,
                "signal_date": today_str,
            })

        _label = "SHADOW — no orders will be sent" if config.SHADOW_MODE else "T+1 model"
        logger.info(f"[PENDING_BUYS_GENERATED] {len(pending_buys_list)} buys "
                    f"queued ({_label})")

    # Summary
    buy_skipped = [o.ticker for o in buy_orders
                   if prices.get(o.ticker, 0) <= 0]
    if buy_skipped or price_fail_codes:
        logger.warning(f"Rebalance summary — price-failed codes: {price_fail_codes}, "
                       f"buy-skipped: {buy_skipped}")

    trade_logger.log_rebalance_summary(len(sell_orders), len(pending_buys_list),
                                        portfolio.get_current_equity())
    return len(price_fail_codes), pending_buys_list, sell_status


# run_mock → lifecycle/mock.py (Phase 1 extracted)

MAX_RESTART_ATTEMPTS = 3
MIN_RESTART_INTERVAL_SEC = 60  # prevent crash loop


def _run_live_with_restart(config):
    """Run live with auto-restart on crash (max N attempts)."""
    _logger = logging.getLogger("gen4.live")
    last_crash_time = None

    for attempt in range(1, MAX_RESTART_ATTEMPTS + 1):
        try:
            _logger.info(f"[LIVE_START] attempt {attempt}/{MAX_RESTART_ATTEMPTS}")
            run_live(config)
            _logger.info("[LIVE_END] normal exit")
            return  # normal exit, no restart needed
        except KeyboardInterrupt:
            _logger.info("[LIVE_END] user interrupt (Ctrl+C)")
            return  # user wanted to stop
        except SystemExit:
            _logger.info("[LIVE_END] system exit")
            return
        except Exception as e:
            _logger.error(f"[LIVE_CRASH] attempt {attempt}: {e}", exc_info=True)

            # Crash loop guard: if last crash was too recent, stop
            now = datetime.now()
            if last_crash_time:
                gap = (now - last_crash_time).total_seconds()
                if gap < MIN_RESTART_INTERVAL_SEC:
                    _logger.error(
                        f"[LIVE_CRASH_LOOP] {gap:.0f}s since last crash "
                        f"(< {MIN_RESTART_INTERVAL_SEC}s). Stopping.")
                    return
            last_crash_time = now

            if attempt < MAX_RESTART_ATTEMPTS:
                _logger.info(f"[LIVE_RESTART] waiting 10s before attempt "
                             f"{attempt+1}/{MAX_RESTART_ATTEMPTS}...")
                time.sleep(10)

    _logger.error(f"[LIVE_GIVE_UP] {MAX_RESTART_ATTEMPTS} attempts exhausted. "
                  f"Manual intervention required.")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gen4 Core Trading System")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", action="store_true", help="Batch: update data + scoring")
    group.add_argument("--live", action="store_true", help="Live: Kiwoom trading")
    group.add_argument("--rebalance", action="store_true",
                       help="[DEPRECATED] Use --paper-test --force-rebalance")
    group.add_argument("--backtest", action="store_true", help="Run backtester")
    group.add_argument("--mock", action="store_true", help="Mock mode (test)")
    group.add_argument("--paper-test", action="store_true",
                       help="Paper test: separate state + test signals")
    group.add_argument("--shadow-test", action="store_true",
                       help="Shadow test: compute only, no orders (dry run)")
    parser.add_argument("--start", default="2019-01-02")
    parser.add_argument("--end", default=str(date.today()))
    parser.add_argument("--cycle", choices=["full", "sell_only", "buy_only"],
                        default="full",
                        help="paper-test cycle mode (default: full)")
    parser.add_argument("--fresh", action="store_true",
                        help="paper-test/shadow-test: delete state files, start from broker")
    parser.add_argument("--force-rebalance", action="store_true",
                        help="Force rebalance (paper_test/shadow_test only)")
    parser.add_argument("--confirm-force-rebalance", action="store_true",
                        help="Confirm force rebalance intent (required with --force-rebalance)")
    parser.add_argument("--fast", action="store_true",
                        help="Batch: skip reports + fundamental (signal only)")
    args = parser.parse_args()

    # ── Force-rebalance safety checks ────────────────────────────
    if args.force_rebalance:
        if args.live:
            print("ERROR: --force-rebalance is NOT allowed in live mode.")
            print("  Use: --paper-test --force-rebalance --confirm-force-rebalance")
            print("   Or: --shadow-test --force-rebalance --confirm-force-rebalance")
            sys.exit(1)
        if not (args.paper_test or args.shadow_test):
            print("ERROR: --force-rebalance requires --paper-test or --shadow-test")
            sys.exit(1)
        if not args.confirm_force_rebalance:
            print("ERROR: --force-rebalance requires --confirm-force-rebalance")
            print("  This is a safety check to prevent accidental force rebalance.")
            sys.exit(1)

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from config import Gen4Config
    config = Gen4Config()
    config.ensure_dirs()

    if args.backtest:
        setup_logging(config.LOG_DIR, "backtest")
        from backtest.backtester import main as bt_main
        sys.argv = ["backtester", "--start", args.start, "--end", args.end]
        bt_main()
    elif args.batch:
        setup_logging(config.LOG_DIR, "batch")
        run_batch(config, fast=getattr(args, "fast", False))
    elif args.live:
        setup_logging(config.LOG_DIR, "live")
        _run_live_with_restart(config)
    elif args.paper_test:
        config.TRADING_MODE = "paper_test"
        config.PAPER_TEST_CYCLE = args.cycle
        if args.force_rebalance:
            config.PAPER_TEST_FORCE_REBALANCE = True
            config.FORCE_REBALANCE_CONFIRMED = True
        setup_logging(config.LOG_DIR, "paper_test")
        logger = logging.getLogger("gen4.live")
        if args.fresh:
            config.FRESH_START = True
            for suffix in (".json", ".bak"):
                for prefix in ("portfolio_state_paper_test",
                                "runtime_state_paper_test"):
                    f = config.STATE_DIR / f"{prefix}{suffix}"
                    if f.exists():
                        f.unlink()
                        logger.info(f"[FRESH] Deleted {f.name}")
            logger.info("[FRESH] Clean start — state will sync from broker")
        logger.info(f"[PAPER_TEST_CYCLE] mode={args.cycle}")
        _run_live_with_restart(config)
    elif args.shadow_test:
        config.TRADING_MODE = "shadow_test"
        config.PAPER_TEST_CYCLE = args.cycle
        config.SHADOW_MODE = True
        if args.force_rebalance:
            config.PAPER_TEST_FORCE_REBALANCE = True
            config.FORCE_REBALANCE_CONFIRMED = True
        setup_logging(config.LOG_DIR, "shadow_test")
        logger = logging.getLogger("gen4.live")
        if args.fresh:
            config.FRESH_START = True
            for suffix in (".json", ".bak"):
                for prefix in ("portfolio_state_shadow_test",
                                "runtime_state_shadow_test"):
                    f = config.STATE_DIR / f"{prefix}{suffix}"
                    if f.exists():
                        f.unlink()
                        logger.info(f"[FRESH] Deleted {f.name}")
            logger.info("[FRESH] Clean start — state will sync from broker")
        # Snapshot live state hash for post-test verification
        _live_state_hash = _file_hash(config.STATE_DIR / "portfolio_state_paper.json")
        logger.info(f"[SHADOW_START] Live state hash: {_live_state_hash}")
        logger.info(f"[SHADOW_START] mode=shadow_test, force_rebalance={args.force_rebalance}")
        _run_live_with_restart(config)
        # Post-test: verify live state unchanged
        _post_hash = _file_hash(config.STATE_DIR / "portfolio_state_paper.json")
        if _live_state_hash == _post_hash:
            logger.info("[ISOLATION_OK] Live state unchanged after shadow test")
        else:
            logger.critical("[ISOLATION_BREACH] Live state modified during shadow test!")
    elif args.mock:
        setup_logging(config.LOG_DIR, "mock")
        run_mock(config)
    elif args.rebalance:
        print("[DEPRECATED] --rebalance is deprecated and unsafe.")
        print("  Use: --paper-test --force-rebalance --confirm-force-rebalance --fresh")
        print("   Or: --shadow-test --force-rebalance --confirm-force-rebalance --fresh")
        sys.exit(1)


if __name__ == "__main__":
    main()
