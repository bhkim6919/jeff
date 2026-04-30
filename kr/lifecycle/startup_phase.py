"""
lifecycle/startup_phase.py — Phase 0+1: Startup & Reconciliation
================================================================
Init provider, restore state, reconcile with broker.
Returns fully initialized LiveContext.

Replaces Phase 0 (QApplication + Kiwoom Login) and Phase 1 (State Restore +
Broker Sync) from the monolithic run_live().

Changes from kr-legacy original:
  - QApplication/QEventLoop removed (REST doesn't need Qt)
  - Gen4KiwoomProvider → KiwoomRestProvider (dynamic based on config)
  - time.sleep replaces QEventLoop waits
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from lifecycle.context import LiveContext
from lifecycle.phase_skeleton import Phase
from lifecycle.utils import (
    _safe_save, is_trading_day, _resolve_trading_mode,
    validate_trading_mode, _compute_regime_snapshot,
)
from lifecycle.reconcile import _reconcile_with_broker
from notify.helpers import (
    _load_name_cache, _save_name_cache, _enrich_name_cache,
)

logger = logging.getLogger("gen4.live")

# Telegram notifier (failure never blocks trading)
try:
    from notify.telegram_bot import send as _tg_send
    def _kakao_safe_mode(level, reason=""):
        _tg_send(f"<b>SAFE MODE L{level}</b>\n{reason}", "CRITICAL")
    _KAKAO_OK = True
except Exception:
    _KAKAO_OK = False


def run_startup(config) -> LiveContext:
    """Phase 0+1: Init provider, restore state, reconcile with broker.

    Returns fully initialized LiveContext ready for Phase 1.5+.
    """
    logger.info("=" * 60)
    logger.info("  Gen4 Live Mode")
    logger.info("=" * 60)

    # ── Pre-flight ───────────────────────────────────────────────
    if not is_trading_day():
        logger.warning("Non-trading day (weekend/holiday). Exiting.")
        raise SystemExit("Non-trading day")
    if datetime.now().hour >= 16 and _resolve_trading_mode(config) != "shadow_test":
        logger.warning("After 16:00. Market closed. Exiting.")
        raise SystemExit("After market hours")

    # ── Kakao name cache (loaded once per session) ───────────────
    name_cache = _load_name_cache(config.BASE_DIR)

    # ── Regime Snapshot (once per session, observation only) ──────
    session_regime, session_kospi_ma200, session_breadth = _compute_regime_snapshot(config)

    # ── Phase 0: Provider Init (REST — no Qt needed) ─────────────
    from data.rest_provider import KiwoomRestProvider
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from risk.exposure_guard import ExposureGuard, BuyPermission
    from report.reporter import TradeLogger, save_forensic_snapshot

    # REST provider: no Kiwoom COM login needed
    provider = KiwoomRestProvider()
    server_type = getattr(provider, '_server_type', 'REAL')

    # ── Trading mode resolution ──────────────────────────────────
    intended_mode = _resolve_trading_mode(config)
    actual_mode = "paper" if server_type == "MOCK" else "live"

    try:
        validate_trading_mode(intended_mode, server_type, broker_connected=True)
    except RuntimeError as e:
        logger.critical(str(e))
        raise SystemExit(str(e))

    # paper_test / shadow_test: preserve intended mode
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
                    raise SystemExit("Test residue detected")
            except Exception:
                pass

    tracker = OrderTracker(journal_dir=config.LOG_DIR, trading_mode=trading_mode)
    if trading_mode == "paper_test":
        _report_dir = config.REPORT_DIR_TEST
    elif trading_mode == "shadow_test":
        _report_dir = config.REPORT_DIR_SHADOW
    else:
        _report_dir = config.REPORT_DIR
    trade_logger = TradeLogger(_report_dir)
    executor = OrderExecutor(provider, tracker, trade_logger,
                             simulate=False, trading_mode=trading_mode)

    mode_label = trading_mode.upper()
    logger.info(f"Mode: {mode_label}  (server={server_type})")

    # ── Phase 0.5: State Canary (Jeff 2026-04-30) ───────────────
    # Detect external deletion of critical files (kr/state, OHLCV
    # cache, KOSPI index). 04-30 incident: portfolio_state_live.json
    # + runtime_state_live.json + backtest/data_full/ohlcv/ all
    # vanished overnight; root cause unidentified, recurrence HIGH.
    # Read-only — never blocks startup. Forensic snapshot + Telegram
    # CRITICAL on any failure so the next event is captured before
    # RECON masks it.
    try:
        from lifecycle.state_canary import run_state_canary
        run_state_canary(config, logger)
    except Exception as _canary_err:  # noqa: BLE001
        logger.warning(f"[STATE_CANARY] init failed: {_canary_err}")

    # ── Phase 1: State Restore + Broker Sync ─────────────────────
    state_mgr = StateManager(config.STATE_DIR, trading_mode=trading_mode)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)

    # Recovery state 복원 (이전 세션에서 BLOCKED → 재시작 시 유지)
    _saved_guard = state_mgr.load_guard_state()
    if _saved_guard and _saved_guard.get("recovery_state", "NORMAL") != "NORMAL":
        guard.restore_guard_state(_saved_guard)
        logger.warning(
            f"[GUARD_RESTORED] Loaded persisted state: "
            f"recovery={_saved_guard.get('recovery_state')} "
            f"safe_mode_level={_saved_guard.get('safe_mode_level', 0)}")
    else:
        logger.info("[RECOVERY_STATE_INIT] starts at NORMAL (no persisted state)")

    # Guard 상태 변경 시 자동 영속화 콜백 등록
    guard.set_state_change_callback(state_mgr.save_guard_state)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)
        logger.info(f"Restored: {len(portfolio.positions)} positions, cash={portfolio.cash:,.0f}")

        # ── C1 FIX: Startup price validity guard ──────────────────
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
                _startup_dd < -0.30
                and _mv_ratio < 0.50
                and _zero_price_count > 0
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

    # Ghost fill sync
    executor.set_ghost_fill_context(portfolio, state_mgr,
                                     buy_cost=config.BUY_COST)
    provider.set_ghost_fill_callback(executor.on_ghost_fill)

    # ── LIVE Guard: block if test paths are active ────────────────
    if trading_mode == "live":
        _live_signals_dir = config.SIGNALS_DIR
        logger.info("[LIVE_GUARD] trading_mode=%s signals_dir=%s state_mode=%s",
                    trading_mode, _live_signals_dir, state_mgr.trading_mode)
        if _live_signals_dir == config.SIGNALS_DIR_TEST:
            logger.critical("[LIVE_BLOCKED_TEST_SIGNALS] "
                            "signals_dir points to test path. Aborting.")
            raise SystemExit("Test signals in live mode")
        if state_mgr.trading_mode == "paper_test":
            logger.critical("[LIVE_BLOCKED_TEST_STATE] "
                            "state manager uses paper_test mode. Aborting.")
            raise SystemExit("Test state in live mode")

    # ── Dirty Exit Detection ─────────────────────────────────────
    _dirty = state_mgr.was_dirty_exit()
    _last_reason = state_mgr.get_last_shutdown_reason()

    if _dirty and getattr(config, "FRESH_START", False):
        _dirty = False
        logger.info("[FRESH_OVERRIDE] Dirty exit suppressed — FRESH intentional state deletion")

    if _dirty:
        logger.warning(f"[DIRTY_EXIT_DETECTED] last_shutdown_reason={_last_reason} "
                       f"— running recovery-first startup")
    else:
        logger.info(f"[CLEAN_STARTUP] last_shutdown_reason={_last_reason}")

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
                _saved_ok = state_mgr.save_pending_external(_kept)
            else:
                _saved_ok = state_mgr.clear_pending_external()
            if not _saved_ok:
                logger.error("[PENDING_STALE_DISCARD] state save FAILED after discard")
            else:
                logger.info(f"[PENDING_STALE_DISCARD] state saved — "
                            f"remaining pending_external={len(_kept)}")
        else:
            logger.info(f"[PENDING_EXTERNAL_CHECK] {len(_pe_stale)} entries, all within 24h")

    # ── Cancel Stale Orders (recovery-first) ──────────────────
    _recovery_ok = True
    logger.info("[STARTUP_CANCEL] Querying open orders from previous session...")
    try:
        _cancelled = provider.cancel_all_open_orders()
        if _cancelled is None:
            logger.critical("[STARTUP_CANCEL_FAIL] Open order query failed — "
                            "cannot confirm stale orders cleared")
            _recovery_ok = False
        elif _cancelled > 0:
            logger.warning(f"[STARTUP_CANCEL] {_cancelled} stale orders cancelled")
            time.sleep(3.0)
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
    _opt_success = _recovery_ok
    if (getattr(config, "FORCE_OPT10075_FAIL", False)
            and trading_mode in ("paper_test", "shadow_test")):
        _opt_success = False
        logger.warning("[FAULT_INJECT] FORCE_OPT10075_FAIL=True → recording as failure")
    logger.info(f"[OPT10075_RECORD] source=startup_cancel success={_opt_success} "
                f"recovery_ok={_recovery_ok} streak_before={guard._opt10075_fail_streak}")
    guard.record_opt10075_result(success=_opt_success)

    # Wait for broker data to settle
    logger.info("[RECON_WAIT] Waiting 3s for broker data to settle...")
    time.sleep(3.0)
    logger.info("[RECON_WAIT] Done — proceeding with reconciliation")

    # Broker reconciliation (P1-1: PARTIAL auto-retry 3x 30s before monitor-only)
    _RECON_MAX_RETRY = 3
    _RECON_RETRY_SLEEP = 30.0
    _recon_attempt = 0
    recon = None
    while _recon_attempt <= _RECON_MAX_RETRY:
        recon = _reconcile_with_broker(portfolio, provider, logger, trade_logger,
                                        buy_cost=config.BUY_COST, guard=guard)
        # Only retry on holdings_unreliable (PARTIAL snapshot). Other errors
        # (e.g. cash_spike, broker_fail) should fall through to existing handlers.
        _is_partial = bool(recon and not recon.get("ok", True)
                            and recon.get("error") == "holdings_unreliable")
        if not _is_partial:
            break
        if _recon_attempt >= _RECON_MAX_RETRY:
            logger.critical(
                f"[RECON_PARTIAL_RETRY] exhausted={_recon_attempt}/{_RECON_MAX_RETRY} "
                f"— falling through to monitor-only")
            break
        _recon_attempt += 1
        logger.warning(
            f"[RECON_PARTIAL_RETRY] attempt={_recon_attempt}/{_RECON_MAX_RETRY} "
            f"sleep={_RECON_RETRY_SLEEP:.0f}s — PARTIAL snapshot, retrying")
        time.sleep(_RECON_RETRY_SLEEP)
    if _recon_attempt > 0 and recon and recon.get("ok", True):
        logger.info(f"[RECON_PARTIAL_RETRY] recovered after attempt={_recon_attempt}")
    reconcile_corrections = recon.get("corrections", 0) if recon else 0
    if recon and reconcile_corrections > 0:
        if trading_mode != "shadow_test":
            _safe_save(state_mgr, portfolio,
                       context=f"recon/{reconcile_corrections}corrections")
            logger.info(f"[RECON] State saved after {recon['corrections']} corrections")
        else:
            logger.info(f"[SHADOW_TEST] RECON found {reconcile_corrections} corrections (not saved)")
    if recon and not recon.get("ok", True):
        _err = recon.get("error", "unknown")
        # PARTIAL holdings → do NOT abort; force session monitor-only.
        # (KR-P0-002: was ok=True+safe_mode which often proceeded silently.)
        if _err == "holdings_unreliable":
            if trading_mode == "shadow_test":
                logger.warning("[SHADOW_TEST_EXIT] PARTIAL snapshot — logged only")
                raise SystemExit("Shadow test PARTIAL")
            logger.critical(
                "[RECON_HOLDINGS_UNRELIABLE] PARTIAL snapshot detected — "
                "session forced to MONITOR-ONLY. No rebal/buy/sell until next session.")
            try:
                rt = state_mgr.load_runtime()
                rt["recon_unreliable"] = True
                rt["monitor_only_reason"] = "holdings_unreliable"
                state_mgr.save_runtime(rt)
            except Exception as _e:
                logger.warning(f"[RECON_PARTIAL_SAVE_FAIL] {_e}")
            # guard.force_safe_mode on L2 so downstream gates see SAFE_MODE
            try:
                guard.force_safe_mode("RECON_PARTIAL: holdings_unreliable", level=2)
            except Exception:
                pass
            if _KAKAO_OK:
                try:
                    _kakao_safe_mode(2, "PARTIAL snapshot — monitor-only")
                except Exception:
                    pass
            # Allow startup to continue but sentinel flag propagates to phases.
            # session_monitor_only is set below at line ~441 via safe_mode path.
            recon = dict(recon)
            recon["safe_mode"] = True
            recon["safe_mode_reason"] = "holdings_unreliable — monitor-only session"
        else:
            if trading_mode == "shadow_test":
                logger.warning("[SHADOW_TEST_EXIT] Broker sync failed — logged only, no abort needed")
                logger.info("[SHADOW_TEST_RESULT] RECON=FAIL, corrections=%d, error=%s",
                            reconcile_corrections, _err)
                raise SystemExit("Shadow test RECON fail")
            logger.critical("Broker sync FAILED — aborting LIVE to prevent stale-state trading")
            save_forensic_snapshot(
                config.STATE_DIR,
                portfolio_data=portfolio.to_dict(),
                error_msg=f"Broker sync failed: {_err}",
                extra={"recon": recon})
            raise SystemExit("Broker sync failed")

    # shadow_test: exit after RECON
    if trading_mode == "shadow_test":
        _pos_count = len(portfolio.positions)
        _cash = portfolio.cash
        logger.info("[SHADOW_TEST_EXIT] RECON completed successfully")
        logger.info("[SHADOW_TEST_RESULT] RECON=OK, corrections=%d, "
                    "positions=%d, cash=%s, broker_ok=True",
                    reconcile_corrections, _pos_count, f"{_cash:,.0f}")
        raise SystemExit("Shadow test complete")

    # FRESH start: reset equity baseline
    if getattr(config, "FRESH_START", False) and reconcile_corrections > 0:
        cur_eq = portfolio.get_current_equity()
        portfolio.prev_close_equity = cur_eq
        portfolio.peak_equity = max(portfolio.peak_equity, cur_eq)
        logger.info(f"[FRESH_EQUITY_RESET] prev_close_equity={cur_eq:,.0f} "
                    f"(post-RECON baseline, DD_GUARD starts at 0%)")

    # Enrich name cache from broker master data
    _all_codes = list(portfolio.positions.keys())
    if _enrich_name_cache(name_cache, _all_codes, provider):
        _save_name_cache(config.BASE_DIR, name_cache)
        logger.info(f"[NAME_CACHE] Updated: {len(name_cache)} entries")

    # ── Recovery Verification (dirty exit + stale orders) ────────
    if not _recovery_ok:
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
        _recheck_success = _recheck_ok
        if (getattr(config, "FORCE_OPT10075_FAIL", False)
                and trading_mode in ("paper_test", "shadow_test")):
            _recheck_success = False
            logger.warning("[FAULT_INJECT] FORCE_OPT10075_FAIL=True → recheck as failure")
        logger.info(f"[OPT10075_RECORD] source=recovery_recheck success={_recheck_success} "
                    f"streak_before={guard._opt10075_fail_streak}")
        guard.record_opt10075_result(success=_recheck_success)

    # Fault injection: force 2nd opt10075 failure
    if (getattr(config, "FORCE_OPT10075_FAIL", False)
            and trading_mode in ("paper_test", "shadow_test")
            and _recovery_ok):
        logger.warning(f"[OPT10075_RECORD] source=fault_inject_test success=False "
                       f"streak_before={guard._opt10075_fail_streak}")
        guard.record_opt10075_result(success=False)

    # Session-level flags
    session_monitor_only = False

    # Record RECON result for recovery state machine
    _recon_ok = not (recon and recon.get("safe_mode"))
    guard.record_recon_result(ok=_recon_ok)

    if recon and recon.get("safe_mode"):
        reason = recon.get("safe_mode_reason", "excessive corrections")
        corrections = recon.get("corrections", 0)
        cash_spike = recon.get("cash_spike", False)
        if corrections > 10 and cash_spike:
            _sm_level = 3
        elif corrections > 10:
            _sm_level = 2
        else:
            _sm_level = 1
        rt = state_mgr.load_runtime()
        rt["recon_unreliable"] = True
        state_mgr.save_runtime(rt)
        if mode_label.lower() in ("paper_test", "shadow_test"):
            logger.critical(f"[RECON_SAFE_MODE_DETECTED] {reason} "
                            f"L{_sm_level} — WOULD BLOCK in live!")
            logger.warning(f"[RECON_SAFE_MODE_SKIP] {mode_label}: proceeding despite safe mode")
        else:
            level_changed = guard.force_safe_mode(reason, level=_sm_level)
            if level_changed and _KAKAO_OK:
                try:
                    _kakao_safe_mode(_sm_level, reason)
                except Exception:
                    pass
        if "holdings_unreliable" in reason:
            session_monitor_only = True
            logger.critical("[BROKER_STATE_UNRELIABLE] Session forced to MONITOR-ONLY. "
                            "No rebalance, no buy, no sell until next session with "
                            "reliable holdings.")
    else:
        rt = state_mgr.load_runtime()
        if rt.get("recon_unreliable"):
            rt["recon_unreliable"] = False
            state_mgr.save_runtime(rt)

    # ── XVAL Observer (P2: observer-only, no state writes) ──────
    from web.cross_validator import CrossValidationObserver
    xval_log_dir = config.BASE_DIR / "data" / "xval"
    xval_observer = CrossValidationObserver(log_dir=xval_log_dir)
    logger.info(f"[XVAL_INIT] CrossValidationObserver created, log_dir={xval_log_dir}")

    # ── Build LiveContext ─────────────────────────────────────────
    ctx = LiveContext(
        config=config,
        trading_mode=trading_mode,
        mode_label=mode_label,
        server_type=server_type,
        provider=provider,
        portfolio=portfolio,
        state_mgr=state_mgr,
        executor=executor,
        tracker=tracker,
        guard=guard,
        trade_logger=trade_logger,
        name_cache=name_cache,
        session_regime=session_regime,
        session_kospi_ma200=session_kospi_ma200,
        session_breadth=session_breadth,
        recovery_ok=_recovery_ok,
        monitor_only=session_monitor_only,
        reconcile_corrections=reconcile_corrections,
        dirty_exit=_dirty,
        xval_observer=xval_observer,
        current_phase=Phase.RECON.value,
        recon_complete=True,
    )

    return ctx
