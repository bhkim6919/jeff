"""
lifecycle/eod_phase.py — Phase 4: End of Day
==============================================
Wait until 15:30, trail stop execution, EOD reports, settlement, shutdown.

Extracted from run_live() Phase 4 block.

Changes from kr-legacy original:
  - QEventLoop/QTimer replaced with time.sleep (1s granularity)
  - QApplication.quit() removed (REST doesn't need Qt)
"""
from __future__ import annotations

import json as _json
import logging
import time
from datetime import date, datetime, timedelta

from lifecycle.context import LiveContext
from lifecycle.phase_skeleton import Phase
from lifecycle.utils import _safe_save
from notify.helpers import _notify_trail

logger = logging.getLogger("gen4.live")


def run_eod(ctx: LiveContext) -> None:
    """Phase 4: EOD trail stops, reports, shutdown."""
    from strategy.trail_stop import check_trail_stop, calc_trail_stop_price
    from report.reporter import make_event_id

    config = ctx.config
    portfolio = ctx.portfolio
    state_mgr = ctx.state_mgr
    executor = ctx.executor
    provider = ctx.provider
    trade_logger = ctx.trade_logger
    tracker = ctx.tracker
    mode_label = ctx.mode_label
    name_cache = ctx.name_cache
    trading_mode = ctx.trading_mode
    collector = ctx.collector
    kospi_collector = ctx.kospi_collector
    swing_collector = ctx.swing_collector
    micro_collector = ctx.micro_collector

    EOD_EVAL_HOUR, EOD_EVAL_MIN = 15, 30

    # Determine report dir
    if trading_mode == "paper_test":
        _report_dir = config.REPORT_DIR_TEST
    elif trading_mode == "shadow_test":
        _report_dir = config.REPORT_DIR_SHADOW
    else:
        _report_dir = config.REPORT_DIR

    # Determine intraday dir
    if trading_mode == "paper_test":
        _intraday_dir = config.INTRADAY_DIR_TEST
    elif trading_mode == "shadow_test":
        _intraday_dir = config.INTRADAY_DIR_SHADOW
    else:
        _intraday_dir = config.INTRADAY_DIR

    # ── Wait until 15:30 for closing prices ──────────────────────
    now = datetime.now()
    eod_target = now.replace(hour=EOD_EVAL_HOUR, minute=EOD_EVAL_MIN, second=0)
    if now < eod_target and not ctx.stop_requested:
        wait_sec = max(0, (eod_target - now).total_seconds())
        logger.info(f"Monitor ended. EOD pending — waiting until "
                    f"{EOD_EVAL_HOUR}:{EOD_EVAL_MIN:02d} ({wait_sec:.0f}s)..."
                    f" (Ctrl+C to skip)")
        # time.sleep with 1s granularity for stop_requested check
        for _ in range(int(wait_sec)):
            if ctx.stop_requested:
                break
            time.sleep(1)

    if ctx.stop_requested:
        now_check = datetime.now()
        if now_check.hour < EOD_EVAL_HOUR or (
                now_check.hour == EOD_EVAL_HOUR and now_check.minute < EOD_EVAL_MIN):
            logger.info("[EOD_SKIP] Ctrl+C during market hours — skipping trail stop + EOD evaluation")
            _safe_save(state_mgr, portfolio, context="early_exit")
            state_mgr.mark_shutdown("sigint")
            provider.shutdown()
            return
        else:
            logger.info("[EOD_SIGINT_LATE] Ctrl+C after market close — proceeding with trail stop evaluation (no new orders)")

    logger.info("EOD: evaluating trail stops on close prices...")

    # ── EOD close price resolution ──────────────────────────
    logger.info("[EOD_PRICE_CONTEXT] realtime updates ended; "
                "using close-price evaluation for %d positions",
                len(portfolio.positions))

    eod_bars = {}
    try:
        if collector:
            eod_bars = collector.load_all_today()
    except Exception as e:
        logger.warning(f"[EOD_PRICE_CONTEXT] intraday load failed: {e}")

    today_str = date.today().strftime("%Y-%m-%d")
    eod_price_src = {}
    for code in list(portfolio.positions.keys()):
        if code in eod_bars and not eod_bars[code].empty:
            try:
                bar_df = eod_bars[code]
                last_close = float(bar_df["close"].iloc[-1])
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
        try:
            p = executor.get_live_price(code)
            if p > 0:
                eod_price_src[code] = (p, "provider_cached")
                portfolio.positions[code].current_price = p
                continue
        except Exception:
            pass
        pos = portfolio.positions.get(code)
        if pos and pos.current_price > 0:
            eod_price_src[code] = (pos.current_price, "position_fallback")
        else:
            eod_price_src[code] = (0, "unavailable")

    # EOD Prefetch: re-query for official close
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

    # Trail stop check
    trail_triggered = 0
    trail_sent = 0
    trail_filled = 0
    trail_failed = 0

    for code in list(portfolio.positions.keys()):
        pos = portfolio.positions.get(code)
        # KR-P0-003: skip trail stop for RECON-isolated symbols (qty uncertain)
        if pos and getattr(pos, "reconcile_pending", False):
            logger.critical(
                f"[TRAIL_SKIP_RECONCILE_PENDING] {code}: reconcile_pending=True, "
                f"trail stop disabled pending manual review")
            continue
        price_info = eod_price_src.get(code, (0, "unavailable"))
        close_price, price_source = price_info

        if not pos or close_price is None or close_price <= 0:
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

        if price_source in ("provider_cached", "position_fallback"):
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
            if close_price > pos.high_watermark:
                pos.high_watermark = close_price
            continue

        triggered, new_hwm, exit_price = check_trail_stop(
            pos.high_watermark, close_price, config.TRAIL_PCT)
        pos.high_watermark = new_hwm
        pos.trail_skip_days = 0

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
                regime=ctx.session_regime)

            if ctx.stop_requested:
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
                _pre_pos = portfolio.positions.get(code)
                _hwm_pct = (pos.high_watermark / pos.avg_price - 1) if pos.avg_price > 0 else 0
                trade = portfolio.remove_position(code, fill_price, config.SELL_COST,
                                                  qty=exec_qty)
                if trade:
                    trade["exit_reason"] = "TRAIL_STOP"
                    trade_logger.log_close(trade, "TRAIL_STOP_FILLED",
                                            mode_label,
                                            event_id=eid,
                                            entry_rank=pos.entry_rank,
                                            score_mom=pos.score_mom,
                                            max_hwm_pct=_hwm_pct)
                logger.info(f"TRAIL_STOP_FILLED {code}: price={fill_price:,.0f}")
                _drop = (fill_price / pos.high_watermark - 1) if pos.high_watermark > 0 else 0
                _notify_trail(code, name_cache, fill_price, pos.high_watermark, _drop,
                             qty=exec_qty, avg_price=pos.avg_price)
                if collector:
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
    if collector:
        collector.flush_all()
    if kospi_collector:
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

    # ── EOD KOSPI/KOSDAQ close fetch ──────────
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

    # KOSPI minute bars
    try:
        kospi_bars = provider.get_index_minute_bars("001")
        if kospi_bars:
            today_compact = date.today().strftime("%Y%m%d")
            kb_path = _report_dir / f"kospi_minute_{today_compact}.json"
            kb_path.write_text(_json.dumps(kospi_bars, ensure_ascii=False), encoding="utf-8")
            logger.info(f"KOSPI minute bars saved: {len(kospi_bars)} bars -> {kb_path}")
    except Exception as e:
        logger.warning(f"KOSPI minute bars fetch failed: {e} (non-critical)")

    # ── EOD equity snapshot ──
    summary = portfolio.summary()
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"],
        risk_mode=summary["risk_mode"],
        rebalance_executed=ctx.rebalance_executed,
        price_fail_count=ctx.price_fail_count,
        reconcile_corrections=ctx.reconcile_corrections,
        monitor_only=ctx.monitor_only,
        kospi_close=kospi_close_val,
        kosdaq_close=kosdaq_close_val,
        regime=ctx.session_regime,
        kospi_ma200=ctx.session_kospi_ma200,
        breadth=ctx.session_breadth)

    portfolio.end_of_day()
    _safe_save(state_mgr, portfolio, context="EOD")

    # ── EOD Reports, Analysis, Settlement ──────────────────────
    try:
        # Daily positions snapshot
        trade_logger.log_daily_positions(
            portfolio.positions,
            buy_cost=config.BUY_COST,
            sell_cost=config.SELL_COST)

        # Intraday Analysis + Daily HTML Report
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

        # Weekly Report (Friday EOD)
        if date.today().weekday() == 4:
            try:
                from report.weekly_report import generate_weekly_report
                wrpt = generate_weekly_report(_eod_report_dir, config)
                if wrpt:
                    logger.info(f"Weekly report: {wrpt}")
            except Exception as e:
                logger.warning(f"Weekly report generation failed: {e} (non-critical)")

        # Monthly Report (last trading day of month)
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

        # ── EOD: Settle PENDING_EXTERNAL orders ────
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
                                f"expected={rec.quantity} -> {terminal} (qty={final_qty})")
                            tracker.mark_reconcile_settled(
                                rec.order_id, final_qty, broker_avg, terminal)

                        elif rec.side == "SELL":
                            base_qty = getattr(rec, 'base_qty', None)
                            if base_qty is not None:
                                sold_qty = max(0, base_qty - broker_qty)
                                if broker_qty > base_qty:
                                    logger.warning(
                                        f"[RECON_NEGATIVE_DELTA] SELL {rec.code}: "
                                        f"broker={broker_qty} > base={base_qty} "
                                        f"— clamped to 0 (sell may not have executed)")
                            else:
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
                                    f"delta=0 -> CANCELLED")
                            elif sold_qty >= rec.quantity:
                                terminal, final_qty = "FILLED", rec.quantity
                                logger.info(
                                    f"[RECON_DECISION] SELL {rec.code} "
                                    f"base_qty={base_qty} broker_qty={broker_qty} "
                                    f"delta={sold_qty} requested={rec.quantity} -> FILLED")
                            else:
                                terminal, final_qty = "FILLED", sold_qty
                                logger.info(
                                    f"[RECON_DECISION] SELL {rec.code} "
                                    f"base_qty={base_qty} broker_qty={broker_qty} "
                                    f"delta={sold_qty} requested={rec.quantity} "
                                    f"-> FILLED (partial)")
                            tracker.mark_reconcile_settled(
                                rec.order_id, final_qty, rec.exec_price, terminal)

                    executor._try_upgrade_sell_status()
                    if state_mgr:
                        state_mgr.clear_pending_external()
                    logger.info("[EOD_SETTLE] PENDING_EXTERNAL settlement complete")
                except Exception as e:
                    logger.error(f"[EOD_SETTLE_ERROR] {e}", exc_info=True)

    finally:
        # XVAL: save daily summary before shutdown
        if ctx.xval_observer:
            try:
                xval_dir = config.BASE_DIR / "data" / "xval"
                ctx.xval_observer.save_daily_summary(xval_dir)
                ctx.xval_observer.log_summary()
                p3_check = ctx.xval_observer.check_phase3_ready()
                logger.info(f"[XVAL_EOD] Phase3 ready={p3_check['ready']}")
            except Exception as _xval_err:
                logger.debug(f"[XVAL_EOD_ERR] {_xval_err}")

        # DB cleanup — retention 정책 적용
        try:
            from web.rest_state_db import cleanup_old_data as _rest_cleanup
            deleted = _rest_cleanup(keep_days=90)
            if deleted > 0:
                logger.info(f"[EOD_CLEANUP] rest_state: {deleted} old rows removed")
        except Exception as e:
            logger.warning(f"[EOD_CLEANUP] rest_state cleanup failed: {e}")

        try:
            from data.db_provider import get_db as _get_db
            _get_db().cleanup_report_tables(keep_days=365)
        except Exception as e:
            logger.warning(f"[EOD_CLEANUP] report_tables cleanup failed: {e}")

        # CSV → PG 적재 (intraday bars)
        try:
            from shared.db.csv_loader import load_csv_to_pg
            import pandas as _pd
            from pathlib import Path as _Path

            _today = date.today().strftime("%Y-%m-%d")
            _intraday_dir = config.BASE_DIR / "data" / "intraday"

            def _parse_intraday(f):
                df = _pd.read_csv(f, encoding="utf-8-sig")
                df = df.rename(columns={"datetime": "bar_datetime"})
                df["code"] = f.stem
                return df[["code", "bar_datetime", "open", "high", "low", "close", "volume", "status"]]

            if _intraday_dir.exists():
                result = load_csv_to_pg(
                    trade_date=_today,
                    dataset="intraday",
                    csv_dir=_intraday_dir,
                    pg_table="intraday_bars",
                    parse_fn=_parse_intraday,
                )
                logger.info(f"[EOD_CSV_TO_PG] intraday: {result.get('status')} ({result.get('rows', 0)} rows)")
        except Exception as e:
            logger.warning(f"[EOD_CSV_TO_PG] intraday failed: {e}")

        # Shutdown cleanup
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
