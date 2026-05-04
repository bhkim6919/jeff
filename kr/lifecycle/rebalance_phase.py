"""
lifecycle/rebalance_phase.py — Phase 2+2A: Rebalance
=====================================================
Check if rebalance day, execute rebalance, emergency rebalance.

Extracted from run_live() Phase 2 + Phase 2A blocks.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime

from lifecycle.context import LiveContext
from lifecycle.phase_skeleton import Phase, is_order_allowed
from lifecycle.utils import (
    _safe_save, _count_trading_days, _save_test_reentry_meta,
    is_market_hours,
)
from notify.helpers import _enrich_name_cache, _save_name_cache

logger = logging.getLogger("gen4.live")

# Telegram notifier (optional)
try:
    from notify.telegram_bot import send as _tg_send
    def _kakao_buy_blocked(reason):
        _tg_send(f"<b>BUY BLOCKED</b>\n{reason}", "WARN")
    _KAKAO_OK = True
except Exception:
    _KAKAO_OK = False


def run_rebalance(ctx: LiveContext) -> None:
    """Phase 2+2A: Check and execute rebalance."""
    from risk.exposure_guard import BuyPermission
    from strategy.trail_stop import calc_trail_stop_price
    from strategy.factor_ranker import load_target_portfolio
    from risk.risk_management import _check_emergency_rebalance

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
    trading_mode = ctx.trading_mode

    _is_paper_test = mode_label.lower() in ("paper_test", "shadow_test")
    _cycle = config.PAPER_TEST_CYCLE if _is_paper_test else "full"

    # buy_only: skip rebalance entirely
    if _cycle == "buy_only":
        logger.info("[PAPER_TEST] buy_only mode — skipping rebalance")
        return

    need_rebalance = None
    runtime = state_mgr.load_runtime()
    last_rebal = runtime.get("last_rebalance_date", "")
    today_str = date.today().strftime("%Y%m%d")

    # Trading-day based rebalance check
    if not last_rebal:
        need_rebalance = True
        logger.info("No previous rebalance record — will rebalance.")
    else:
        try:
            last_dt = datetime.strptime(last_rebal, "%Y%m%d").date()
            trading_days = _count_trading_days(last_dt, date.today(), config)
            need_rebalance = (trading_days >= config.REBAL_DAYS)
            logger.info(f"Trading days since last rebalance: {trading_days} "
                        f"(threshold: {config.REBAL_DAYS}, "
                        f"calendar days: {(date.today() - last_dt).days})")
        except (ValueError, TypeError):
            need_rebalance = True
            logger.warning(f"Failed to parse last_rebalance_date='{last_rebal}' "
                           f"— will rebalance as safety fallback.")

    # paper_test / shadow_test: force rebalance
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
            rt = state_mgr.load_runtime()
            rt["force_rebalance_log"] = {
                "date": today_str,
                "mode": mode_label,
                "confirmed": config.FORCE_REBALANCE_CONFIRMED,
                "previous_rebal": last_rebal,
            }
            state_mgr.save_runtime(rt)

    # Monitor-only guard
    if need_rebalance and ctx.monitor_only:
        logger.critical("[MONITOR_ONLY] Rebalance day but session is MONITOR-ONLY "
                        "(holdings unreliable or forced safe mode). Skipping rebalance.")
        need_rebalance = False

    # Recovery guard
    if need_rebalance and ctx.dirty_exit and not ctx.recovery_ok:
        logger.critical("[RECOVERY_BLOCK] Rebalance blocked — dirty exit recovery "
                        "incomplete (stale orders may remain). Monitor-only this session.")
        need_rebalance = False
        ctx.monitor_only = True

    if need_rebalance:
        logger.info("=" * 40)
        logger.info("  REBALANCE DAY")
        logger.info("=" * 40)

        if trading_mode == "paper_test":
            signals_dir = config.SIGNALS_DIR_TEST
            logger.info(f"[PAPER_TEST] Using test signals: {signals_dir}")
        elif trading_mode == "shadow_test":
            signals_dir = config.SIGNALS_DIR
            logger.info(f"[SHADOW_TEST] Using production signals (read-only): {signals_dir}")
        else:
            signals_dir = config.SIGNALS_DIR
        target = load_target_portfolio(signals_dir)
        if not target:
            logger.error("No target portfolio! Skipping rebalance, "
                         "monitor-only mode. Run: python main.py --batch")
            ctx.monitor_only = True
        else:
            data_date = target.get("date", "?")
            # KR-P0-004: log snapshot_version for traceability and re-run detection
            _sv = target.get("snapshot_version", "")
            _src = target.get("selected_source", "?")
            _dl = target.get("data_last_date", "?")
            _uc = target.get("universe_count", "?")
            if _sv:
                logger.info(
                    f"Target loaded: {len(target['target_tickers'])} stocks "
                    f"(data: {data_date}, sv={_sv})")
                logger.info(
                    f"[REBAL_SNAPSHOT_VERSION] source={_src} data_last={_dl} "
                    f"universe={_uc} — skip if matches last_rebal_snapshot_version")
                # Persist to runtime for post-mortem and idempotency
                try:
                    _rt = state_mgr.load_runtime()
                    _last_sv = _rt.get("last_rebal_snapshot_version", "")
                    if _last_sv == _sv and _rt.get("last_rebal_date", "") == today_str:
                        logger.critical(
                            f"[REBAL_DUPLICATE_SNAPSHOT] skip: sv={_sv} "
                            f"already applied today ({today_str})")
                        ctx.monitor_only = True
                        target = None  # force skip below
                except Exception as _e:
                    logger.warning(f"[REBAL_SV_CHECK_FAIL] {_e} — proceeding without gate")
            else:
                logger.warning(
                    f"Target loaded: {len(target['target_tickers'])} stocks "
                    f"(data: {data_date}) — NO snapshot_version (pre-P0-004 batch?)")
        # Re-check after possible force-skip above
        if target is None:
            pass
        else:

            # Enrich name cache with target tickers
            try:
                if _enrich_name_cache(name_cache, target["target_tickers"], provider):
                    _save_name_cache(config.BASE_DIR, name_cache)
            except Exception:
                pass

            # Stale/future target check
            target_ok = True
            try:
                td = datetime.strptime(today_str, "%Y%m%d").date()
                dd = datetime.strptime(data_date, "%Y%m%d").date()
                if dd > td:
                    logger.error(f"Target date is in the FUTURE ({data_date} > {today_str}). "
                                 f"Rejecting — possible data corruption.")
                    target_ok = False
                    ctx.monitor_only = True
                stale_days = (td - dd).days
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
                    ctx.monitor_only = True
            except (ValueError, TypeError):
                logger.warning(f"Cannot parse target date '{data_date}' — "
                               f"treating as stale. Rebalance skipped.")
                target_ok = False
                ctx.monitor_only = True

            if target_ok:
                # Pass pending_external to guard
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

                skip_buys, legacy_reason = guard.should_skip_rebalance(
                    daily_pnl, monthly_dd)

                buy_scale = risk_action["buy_scale"]
                if skip_buys:
                    buy_scale = 0.0

                logger.info(
                    f"[RISK_ACTION] level={risk_action['level']} "
                    f"daily_block={skip_buys} buy_scale={buy_scale:.0%} "
                    f"trim={risk_action['trim_ratio']:.0%} "
                    f"safe_mode={risk_action['safe_mode']}")

                # Trail pre-check: HWM update only (no trigger)
                rt_prices = {}
                if ctx.collector:
                    try:
                        rt_prices = ctx.collector.get_last_prices()
                    except Exception:
                        pass
                for _pc_code in list(portfolio.positions.keys()):
                    _pc_pos = portfolio.positions[_pc_code]
                    _pc_price = rt_prices.get(_pc_code, 0)
                    if _pc_price <= 0:
                        _pc_price = executor.get_live_price(_pc_code)
                    if _pc_price <= 0:
                        continue
                    if _pc_price > _pc_pos.high_watermark:
                        _pc_pos.high_watermark = _pc_price
                    _pc_pos.trail_stop_price = calc_trail_stop_price(
                        _pc_pos.high_watermark, config.TRAIL_PCT)
                    if _pc_pos.high_watermark > 0:
                        _pc_dd = (_pc_price - _pc_pos.high_watermark) / _pc_pos.high_watermark
                        if _pc_dd <= -config.TRAIL_PCT:
                            logger.warning(
                                f"[TRAIL_PRECHECK_NEAR] {_pc_code}: dd={_pc_dd:.2%} "
                                f"price={_pc_price:,.0f} hwm={_pc_pos.high_watermark:,.0f} "
                                f"— will evaluate at EOD close (no intraday trigger)")

                # BuyPermission check
                guard.advance_recovery_state()
                permission, perm_reason = guard.get_buy_permission()
                guard.update_blocked_tracking(permission)
                logger.info(f"[BUY_PERMISSION] {permission.value}: {perm_reason or 'OK'}")

                if permission in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
                    logger.critical(f"[REBAL_{permission.value}] {perm_reason} — "
                                    f"rebal deferred, positions held, trail disabled")
                    need_rebalance = False
                    rt = state_mgr.load_runtime()
                    rt["rebal_deferred_reason"] = perm_reason
                    rt["rebal_deferred_date"] = today_str
                    state_mgr.save_runtime(rt)
                    if _KAKAO_OK and permission == BuyPermission.BLOCKED:
                        try:
                            _kakao_buy_blocked(perm_reason)
                        except Exception:
                            pass

                elif permission == BuyPermission.REDUCED:
                    buy_scale = buy_scale * 0.5
                    logger.warning(f"[BUY_REDUCED] {perm_reason} — "
                                   f"buy_scale={buy_scale:.0%}")

                # ── P2.4: Auto Trading Gate (BUY-only enforcement) ──────────
                # SELL/RECON/EOD/trail는 영향 없음. BUY skip 만 토글.
                try:
                    from risk.execution_guard_hook import guard_buy_execution
                    from risk.strategy_health import compute_strategy_health
                    _rt_for_gate = state_mgr.load_runtime() or {}
                    _equity_dd = float(_rt_for_gate.get("equity_dd_pct", 0.0) or 0.0)
                    _health = compute_strategy_health(equity_dd_pct=_equity_dd)
                    _decision = guard_buy_execution(
                        guard=guard, runtime=_rt_for_gate,
                        portfolio=portfolio, strategy_health=_health,
                    )
                    _req_id = f"rebal-{today_str}"
                    if _decision.block_buy:
                        logger.critical(
                            f"[BUY_BLOCKED_BY_AUTO_GATE] market=KR "
                            f"req={_req_id} mode={_decision.mode} "
                            f"top={_decision.highest_blocker} "
                            f"reason={_decision.reason}"
                        )
                        skip_buys = True
                        buy_scale = 0.0
                    elif not _decision.enabled:
                        # advisory: enforce OFF, BUY 통과하지만 "차단되었을 것" 기록
                        logger.info(
                            f"[BUY_ADVISORY] market=KR req={_req_id} "
                            f"mode={_decision.mode} "
                            f"top={_decision.highest_blocker} "
                            f"reason={_decision.reason}"
                        )
                    else:
                        logger.info(
                            f"[BUY_GATE_ALLOWED] market=KR req={_req_id} "
                            f"mode={_decision.mode} "
                            f"buy_scale={_decision.buy_scale:.2f}"
                        )
                except Exception as _ge:
                    # fail-safe: gate 평가 자체 실패 시 advisory 로그만, BUY 진행
                    logger.error(f"[BUY_GATE_EVAL_ERROR] {type(_ge).__name__}: {_ge}")

                # Wait for market open (09:00)
                if need_rebalance and not is_market_hours():
                    logger.info("[MARKET_WAIT] Market not open yet — waiting for 09:00...")
                    while not is_market_hours():
                        time.sleep(5)
                    logger.info("[MARKET_WAIT] Market open — proceeding with rebalance orders")

                # ── AUTO defer gates (before execution) ──
                if need_rebalance:
                    _defer_reason = None

                    # Gate 1: Manual rebalance busy
                    try:
                        from web.rebalance_api import is_busy as _rebal_is_busy
                        if _rebal_is_busy():
                            _defer_reason = "manual_busy"
                    except ImportError:
                        pass

                    # Gate 2: Open orders on broker
                    if not _defer_reason:
                        try:
                            _open = provider.query_open_orders()
                            if _open is None:
                                _defer_reason = "open_orders_query_fail"
                            elif len(_open) > 0:
                                _defer_reason = f"open_orders={len(_open)}"
                        except Exception:
                            _defer_reason = "open_orders_query_error"

                    # Gate 3: Pending external orders
                    if not _defer_reason:
                        _pe = state_mgr.load_pending_external()
                        if _pe:
                            _defer_reason = f"pending_external={len(_pe)}"

                    if _defer_reason:
                        logger.warning(f"[REBAL_DEFER] reason={_defer_reason} — "
                                       f"auto rebalance deferred to next session")
                        need_rebalance = False
                        rt = state_mgr.load_runtime()
                        rt["rebal_deferred_reason"] = _defer_reason
                        rt["rebal_deferred_date"] = today_str
                        # Clear stale preview from previous cycle
                        rt.pop("rebal_preview_hash", None)
                        state_mgr.save_runtime(rt)
                        try:
                            from web.rebalance_api import get_phase as _gp
                            logger.info(f"[REBAL_DEFER_STATE] "
                                        f"manual_phase={_gp()} auto_deferred=True")
                        except ImportError:
                            pass

                if not need_rebalance:
                    pass  # BLOCKED/DEFERRED — skip to monitor
                else:
                    try:
                        from main import _execute_rebalance_live
                        pfail, pending_buys_list, sell_status = _execute_rebalance_live(
                            portfolio, target, config, executor, provider,
                            trade_logger, skip_buys, logger,
                            state_mgr=state_mgr, today_str=today_str,
                            buy_scale=buy_scale, risk_action=risk_action,
                            regime=ctx.session_regime,
                            mode_str=mode_label, tracker=tracker,
                            name_cache=name_cache,
                            guard=guard)  # PR 5 / G5-a: trim mark moved inside
                        ctx.rebalance_executed = True
                        ctx.price_fail_count = pfail

                        # PR 5 / G5-a: mark_trim_executed now lives inside
                        # _execute_dd_trim() and is conditional on
                        # trimmed > 0. The unconditional call here was the
                        # bug — even when no positions had qty * trim_ratio
                        # >= 1 (no actual trades), this marked the level
                        # as executed and blocked same-day re-trim.

                        # Commit order
                        if sell_status == "COMPLETE":
                            portfolio_saved = _safe_save(
                                state_mgr, portfolio, context="rebalance_commit/portfolio")
                            if portfolio_saved:
                                state_mgr.save_pending_buys(pending_buys_list, sell_status)
                                state_mgr.set_last_rebalance_date(today_str)
                                # KR-P0-004: persist snapshot_version for duplicate-gate
                                try:
                                    __rt = state_mgr.load_runtime()
                                    __rt["last_rebal_snapshot_version"] = target.get(
                                        "snapshot_version", "")
                                    __rt["last_rebal_date"] = today_str
                                    state_mgr.save_runtime(__rt)
                                except Exception as __e:
                                    logger.warning(f"[REBAL_SV_PERSIST_FAIL] {__e}")
                                logger.info(
                                    "[REBALANCE_COMMIT_OK] Portfolio saved, "
                                    "rebalance date marked: %s, "
                                    "pending buys: %d", today_str, len(pending_buys_list))
                                _rt = state_mgr.load_runtime()
                                if _rt.get("rebal_deferred_reason"):
                                    _rt.pop("rebal_deferred_reason", None)
                                    _rt.pop("rebal_deferred_date", None)
                                    state_mgr.save_runtime(_rt)
                                    logger.info("[DEFERRED_CLEARED] rebal deferred state cleaned up")
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
                        _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")
                        logger.info("Crash recovery: portfolio saved, "
                                    "rebalance date NOT marked (will retry)")
    else:
        logger.info(f"Not rebalance day (last: {last_rebal})")

    # ── Phase 2A: Emergency Rebalance Check ──
    if config.EMERGENCY_REBAL_ENABLED and not ctx.monitor_only:
        try:
            emergency_executed = _check_emergency_rebalance(
                portfolio, config, executor, provider, trade_logger,
                state_mgr, guard, mode_label, logger,
                session_rebalance_executed=ctx.rebalance_executed)
            if emergency_executed:
                _safe_save(state_mgr, portfolio, context="emergency_rebal/commit")
                logger.info("[EMERGENCY_REBAL_COMMIT_OK] Portfolio saved after emergency trim")
        except Exception as e:
            logger.error(f"[EMERGENCY_REBAL_ERROR] {e}", exc_info=True)
            _safe_save(state_mgr, portfolio, context="emergency_rebal/crash_recovery")
