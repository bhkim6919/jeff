"""
lifecycle/monitor_phase.py — Phase 3: Monitor Loop
====================================================
Register real-time data (WebSocket), 60-second monitor loop,
trail stop warnings, price updates, HWM tracking, fast reentry.

Extracted from run_live() Phase 3 block.

Changes from kr-legacy original:
  - QEventLoop/QTimer replaced with time.sleep (1s granularity)
  - Ctrl+C handled via ctx.stop_requested
"""
from __future__ import annotations

import logging
import signal
import time
from datetime import date, datetime

from lifecycle.context import LiveContext
from lifecycle.phase_skeleton import Phase
from lifecycle.utils import _safe_save

logger = logging.getLogger("gen4.live")


def run_monitor(ctx: LiveContext) -> None:
    """Phase 3: Monitor loop until EOD."""
    from strategy.trail_stop import calc_trail_stop_price
    from data.intraday_collector import IntradayCollector

    config = ctx.config
    portfolio = ctx.portfolio
    state_mgr = ctx.state_mgr
    executor = ctx.executor
    provider = ctx.provider
    trade_logger = ctx.trade_logger
    tracker = ctx.tracker
    guard = ctx.guard
    mode_label = ctx.mode_label
    name_cache = ctx.name_cache
    trading_mode = ctx.trading_mode

    _is_paper_test = mode_label.lower() in ("paper_test", "shadow_test")
    _cycle = config.PAPER_TEST_CYCLE if _is_paper_test else "full"

    MONITOR_END_HOUR, MONITOR_END_MIN = 15, 20

    # ── Phase 2.5: Intraday Collector Setup ──────────────────────
    if trading_mode == "paper_test":
        _intraday_dir = config.INTRADAY_DIR_TEST
    elif trading_mode == "shadow_test":
        _intraday_dir = config.INTRADAY_DIR_SHADOW
    else:
        _intraday_dir = config.INTRADAY_DIR
    collector = IntradayCollector(_intraday_dir,
                                  date.today().strftime("%Y-%m-%d"))
    collector.set_active_codes(list(portfolio.positions.keys()))

    # KOSPI index collector
    _kospi_intraday_dir = _intraday_dir / "indices"
    kospi_collector = IntradayCollector(_kospi_intraday_dir,
                                        date.today().strftime("%Y-%m-%d"))
    kospi_collector.set_active_codes(["001"])

    if portfolio.positions:
        provider.set_real_data_callback(collector.on_tick)
        provider.register_real(list(portfolio.positions.keys()), fids="10;27")
        logger.info("[Intraday] Real-time tick collection started "
                    f"for {len(portfolio.positions)} positions + KOSPI polling")

    # Snapshot replay data collectors (non-critical)
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
        try:
            swing_collector.seed_pre_market()
        except Exception as seed_err:
            logger.warning(f"[SWING] pre-market seed failed (non-critical): {seed_err}")
    except Exception as e:
        logger.warning(f"[SWING] init failed (non-critical): {e}")
        swing_collector = None
        micro_collector = None

    # Store collectors in context for EOD phase
    ctx.collector = collector
    ctx.kospi_collector = kospi_collector
    ctx.swing_collector = swing_collector
    ctx.micro_collector = micro_collector

    # ── Monitor state ────────────────────────────────────────────
    trail_warnings = set()
    monitor_price_fail_count = 0
    _prev_equity = 0.0
    _equity_stale_count = 0

    # -- Ctrl+C graceful shutdown --
    def _sigint_handler(sig, frame):
        ctx.stop_requested = True
        logger.info("[SIGINT] Stop requested — will exit after current cycle")

    prev_handler = signal.signal(signal.SIGINT, _sigint_handler)

    n_pos = len(portfolio.positions)
    if n_pos == 0:
        logger.info("No positions. Skipping monitor.")
    else:
        logger.info(f"Monitor: {n_pos} positions, 60s interval. Ctrl+C to stop.")
        logger.info("Trail stops evaluated at EOD close (matching backtest).")

        # paper_test: one-shot fast reentry flag
        _test_reentry_done = (_cycle in ("sell_only", "buy_only"))

        try:
            cycle = 0
            while True:
                if ctx.stop_requested:
                    logger.info("[MONITOR_STOP] Ctrl+C received — exiting loop")
                    break
                now = datetime.now()
                if now.hour > MONITOR_END_HOUR or (
                        now.hour == MONITOR_END_HOUR and now.minute >= MONITOR_END_MIN):
                    break

                # R10 (2026-04-23): idle heartbeat to runtime_state_live.
                # Refreshes timestamp so dashboard/monitoring doesn't flag
                # ENGINE_OFFLINE during quiet market periods when no events
                # naturally trigger a state write. Errors swallowed to never
                # break the monitor loop.
                try:
                    state_mgr.heartbeat()
                except Exception as _hb_e:
                    logger.warning(f"[HEARTBEAT_FAIL] {_hb_e}")

                # paper_test fast reentry
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
                    from main import _try_fast_reentry
                    _test_reentry_done = _try_fast_reentry(
                        state_mgr, portfolio, config, executor, provider,
                        trade_logger, logger, mode_label, tracker=tracker,
                        name_cache=name_cache, guard=guard)
                    if _test_reentry_done:
                        _new_codes = set(portfolio.positions.keys()) - _pre_reentry_codes
                        if _new_codes and collector is not None:
                            collector.set_active_codes(list(portfolio.positions.keys()))
                            for _nc in _new_codes:
                                logger.info(f"[FAST_REENTRY_REGISTER] code={_nc}")

                if now.hour < 9:
                    # Pre-market wait with 1s granularity for Ctrl+C
                    for _ in range(60):
                        if ctx.stop_requested:
                            break
                        time.sleep(1)
                    continue

                # Get live prices: prefer real-time ticks, fallback to TR
                rt_tick_prices = collector.get_last_prices()
                prices = {}
                n_realtime = 0
                n_fallback = 0
                cycle_fails = 0
                for code in list(portfolio.positions.keys()):
                    if code in rt_tick_prices and rt_tick_prices[code] > 0:
                        prices[code] = rt_tick_prices[code]
                        n_realtime += 1
                    else:
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
                    if pos.current_price > pos.high_watermark:
                        pos.high_watermark = pos.current_price
                    pos.trail_stop_price = calc_trail_stop_price(
                        pos.high_watermark, config.TRAIL_PCT)
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
                        rebalance_executed=ctx.rebalance_executed,
                        price_fail_count=ctx.price_fail_count,
                        reconcile_corrections=ctx.reconcile_corrections,
                        monitor_only=ctx.monitor_only)
                    _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")

                    # ── XVAL: observer-only cross validation (P2) ──
                    if ctx.xval_observer:
                        try:
                            _xval_broker = provider.query_account_summary()
                            _xval_file = portfolio.to_dict()
                            _xval_file["timestamp"] = state_mgr.load_runtime().get("timestamp", "")
                            _xval_file["_version_seq"] = state_mgr._version_seq
                            ctx.xval_observer.observe(
                                broker_summary=_xval_broker,
                                file_state=_xval_file,
                            )
                            if cycle % 60 == 0:  # ~1시간마다 summary
                                ctx.xval_observer.log_summary()
                        except Exception as _xval_err:
                            logger.debug(f"[XVAL_ERR] {_xval_err}")

                # Sleep with 1s granularity for Ctrl+C responsiveness
                for _ in range(60):
                    if ctx.stop_requested:
                        break
                    time.sleep(1)

                # Flush minute bars
                collector.check_and_flush()

                # KOSPI index polling
                try:
                    _kospi_price = provider.get_kospi_close()
                    if _kospi_price and _kospi_price > 0:
                        kospi_collector.on_tick("001", _kospi_price, 0)
                except Exception:
                    pass
                kospi_collector.check_and_flush()

                # Swing/Micro snapshot collectors
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

    # Merge monitor price failures
    ctx.price_fail_count += monitor_price_fail_count
    if monitor_price_fail_count > 0:
        logger.warning(f"Monitor price failures: {monitor_price_fail_count} "
                       f"(total session: {ctx.price_fail_count})")
