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
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Logging Setup ────────────────────────────────────────────────────────────
def setup_logging(log_dir: Path, mode: str):
    log_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    log_file = log_dir / f"gen4_{mode}_{today}.log"
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO, format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def is_weekday() -> bool:
    return date.today().weekday() < 5

def is_market_hours() -> bool:
    now = datetime.now()
    if now.hour < 9:
        return False
    if now.hour >= 15 and now.minute >= 20:
        return False
    return 9 <= now.hour <= 15


# ── Batch Mode ───────────────────────────────────────────────────────────────
def run_batch(config):
    """Batch: pykrx update → universe → scoring → target portfolio."""
    from data.pykrx_provider import update_ohlcv_incremental, get_stock_list
    from data.universe_builder import build_universe_from_ohlcv
    from strategy.factor_ranker import build_target_portfolio, save_target_portfolio
    import pandas as pd

    logger = logging.getLogger("gen4.batch")
    logger.info("=" * 60)
    logger.info("  Gen4 Batch Mode")
    logger.info("=" * 60)

    ohlcv_dir = config.OHLCV_DIR

    # Step 1: pykrx OHLCV update (existing + new listings)
    logger.info("[1/5] Updating OHLCV via pykrx...")
    if not is_weekday():
        logger.info("  Skipping pykrx update — weekend, using existing data")
    else:
        try:
            existing = set(f.stem for f in ohlcv_dir.glob("*.csv"))
            try:
                live_list = set(get_stock_list("KOSPI", ohlcv_dir=ohlcv_dir))
            except Exception as e:
                logger.warning(f"  pykrx ticker list failed: {e}")
                live_list = set()
            codes = sorted(existing | live_list)
            new_count = len(live_list - existing)
            if new_count > 0:
                logger.info(f"  New listings detected: {new_count} stocks")
            if codes:
                updated = update_ohlcv_incremental(ohlcv_dir, codes, days=30)
                logger.info(f"  Updated {updated}/{len(codes)} stocks")
        except Exception as e:
            logger.warning(f"  pykrx update failed: {e}. Using existing data.")

    # Step 2: Build universe
    logger.info("[2/5] Building universe...")
    universe = build_universe_from_ohlcv(
        ohlcv_dir, min_close=config.UNIV_MIN_CLOSE,
        min_amount=config.UNIV_MIN_AMOUNT,
        min_history=config.UNIV_MIN_HISTORY,
        min_count=config.UNIV_MIN_COUNT)
    logger.info(f"  Universe: {len(universe)} stocks")
    if not universe:
        logger.error("Empty universe!")
        return None

    # Step 3: Load OHLCV for scoring
    logger.info("[3/5] Loading OHLCV...")
    close_dict = {}
    for code in universe:
        path = ohlcv_dir / f"{code}.csv"
        if path.exists():
            df = pd.read_csv(path, parse_dates=["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
            if len(df) >= config.VOL_LOOKBACK:
                close_dict[code] = df.set_index("date")["close"]
    logger.info(f"  Loaded {len(close_dict)} stocks")

    # Step 4: Score and select
    logger.info("[4/5] Scoring and selecting...")
    target = build_target_portfolio(close_dict, config)
    path = save_target_portfolio(target, config.SIGNALS_DIR)
    logger.info(f"  Target: {len(target['target_tickers'])} stocks -> {path}")
    for i, tk in enumerate(target["target_tickers"], 1):
        s = target["scores"].get(tk, {})
        logger.info(f"    {i:2d}. {tk}  vol={s.get('vol_12m',0):.4f}  mom={s.get('mom_12_1',0):.4f}")

    # Step 5: Generate Top20 MA HTML report
    logger.info("[5/5] Generating Top20 MA report...")
    try:
        from report.top20_report import generate_top20_report
        html_path = generate_top20_report(target, ohlcv_dir, config.REPORT_DIR)
        if html_path:
            logger.info(f"  Report: {html_path}")
    except Exception as e:
        logger.warning(f"  Report generation failed: {e} (non-critical)")

    logger.info("Batch complete.")
    return target


def _count_trading_days(start_date, end_date, config) -> int:
    """Count trading days between two dates using KOSPI index calendar.
    Falls back to calendar days * 5/7 if KOSPI data unavailable."""
    try:
        from report.kospi_utils import load_kospi_close
        if hasattr(config, "INDEX_FILE") and config.INDEX_FILE.exists():
            kospi = load_kospi_close(config.INDEX_FILE)
            if not kospi.empty:
                s = start_date.strftime("%Y-%m-%d")
                e = end_date.strftime("%Y-%m-%d")
                trading = [d for d in kospi.index if s < d <= e]
                return len(trading)
    except Exception:
        pass
    # Fallback: approximate with weekdays
    cal_days = (end_date - start_date).days
    return int(cal_days * 5 / 7)


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
    if not is_weekday():
        logger.warning("Weekend. Market closed. Exiting.")
        return
    if datetime.now().hour >= 16:
        logger.warning("After 16:00. Market closed. Exiting.")
        return

    # ── Phase 0: QApplication + Kiwoom Login ─────────────────────
    from PyQt5.QtWidgets import QApplication
    app = QApplication.instance() or QApplication(sys.argv)

    from api.kiwoom_connector import create_loggedin_kiwoom
    from data.kiwoom_provider import Gen4KiwoomProvider
    from runtime.order_executor import OrderExecutor
    from runtime.order_tracker import OrderTracker
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from risk.exposure_guard import ExposureGuard
    from strategy.trail_stop import check_trail_stop, calc_trail_stop_price
    from strategy.factor_ranker import load_target_portfolio
    from report.reporter import TradeLogger, save_forensic_snapshot

    kiwoom, server_type = create_loggedin_kiwoom()
    # MOCK server: send real orders to mock server (not internal simulation)
    # REAL server: send real orders to live server
    is_paper = False
    if config.PAPER_TRADING and server_type == "REAL":
        logger.warning("config.PAPER_TRADING=True but server is REAL — "
                       "ignoring config, running LIVE. "
                       "Use --mock or MOCK server for paper trading.")
    provider = Gen4KiwoomProvider(kiwoom, str(config.SECTOR_MAP))

    tracker = OrderTracker()
    trade_logger = TradeLogger(config.REPORT_DIR)
    executor = OrderExecutor(provider, tracker, trade_logger, paper=is_paper)

    mode_label = "MOCK-LIVE" if server_type == "MOCK" else "REAL-LIVE"
    logger.info(f"Mode: {mode_label}  (server={server_type}, paper={is_paper})")

    # ── Phase 1: State Restore + Broker Sync ─────────────────────
    is_mock = (server_type == "MOCK")
    state_mgr = StateManager(config.STATE_DIR, paper=is_mock)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)
        logger.info(f"Restored: {len(portfolio.positions)} positions, cash={portfolio.cash:,.0f}")

    # Broker reconciliation — broker is truth, engine state synced
    recon = _reconcile_with_broker(portfolio, provider, logger, trade_logger)
    reconcile_corrections = recon.get("corrections", 0) if recon else 0
    if recon and reconcile_corrections > 0:
        state_mgr.save_portfolio(portfolio.to_dict())
        logger.info(f"[RECON] State saved after {recon['corrections']} corrections")
    if recon and not recon.get("ok", True):
        logger.critical("Broker sync FAILED — aborting LIVE to prevent stale-state trading")
        save_forensic_snapshot(
            config.STATE_DIR,
            portfolio_data=portfolio.to_dict(),
            error_msg=f"Broker sync failed: {recon.get('error', 'unknown')}",
            extra={"recon": recon})
        return

    # ── Phase 2: Rebalance Check ─────────────────────────────────
    runtime = state_mgr.load_runtime()
    last_rebal = runtime.get("last_rebalance_date", "")
    today_str = date.today().strftime("%Y%m%d")

    # Trading-day based rebalance check (matches backtest REBAL_DAYS)
    if not last_rebal:
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

    # Session-level context for equity log
    session_rebalance_executed = False
    session_price_fail_count = 0
    session_monitor_only = False

    if need_rebalance:
        logger.info("=" * 40)
        logger.info("  REBALANCE DAY")
        logger.info("=" * 40)

        target = load_target_portfolio(config.SIGNALS_DIR)
        if not target:
            logger.error("No target portfolio! Skipping rebalance, "
                         "monitor-only mode. Run: python main.py --batch")
            session_monitor_only = True
        else:
            data_date = target.get("date", "?")
            logger.info(f"Target loaded: {len(target['target_tickers'])} stocks (data: {data_date})")

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
                # Risk check
                skip_buys, reason = guard.should_skip_rebalance(
                    portfolio.get_daily_pnl_pct(), portfolio.get_monthly_dd_pct())

                pfail = _execute_rebalance_live(
                    portfolio, target, config, executor, provider,
                    trade_logger, skip_buys, logger)
                session_rebalance_executed = True
                session_price_fail_count = pfail

                state_mgr.set_last_rebalance_date(today_str)
                state_mgr.save_portfolio(portfolio.to_dict())
                logger.info("Rebalance done. State saved.")
    else:
        logger.info(f"Not rebalance day (last: {last_rebal})")

    # ── Phase 3: Monitor Loop (HWM update + trail warning only) ────
    #    Trail stop EXECUTION happens at EOD (Phase 4) to match backtest
    #    (backtest uses daily close; live must also use EOD close).
    #    Intraday: update HWM, warn if near trigger, but do NOT execute.
    #    Monitor ends at MONITOR_END (15:20). EOD evaluates at EOD_EVAL (15:30).
    MONITOR_END_HOUR, MONITOR_END_MIN = 15, 20
    EOD_EVAL_HOUR, EOD_EVAL_MIN = 15, 30

    trail_warnings = set()  # codes warned during intraday
    monitor_price_fail_count = 0  # track price fetch failures during monitoring

    n_pos = len(portfolio.positions)
    if n_pos == 0:
        logger.info("No positions. Skipping monitor.")
    else:
        logger.info(f"Monitor: {n_pos} positions, 60s interval. Ctrl+C to stop.")
        logger.info("Trail stops evaluated at EOD close (matching backtest).")

        try:
            cycle = 0
            while True:
                now = datetime.now()
                if now.hour > MONITOR_END_HOUR or (
                        now.hour == MONITOR_END_HOUR and now.minute >= MONITOR_END_MIN):
                    break
                if now.hour < 9:
                    time.sleep(60)
                    continue

                # Get live prices + update HWM (but do NOT trigger exits)
                prices = {}
                cycle_fails = 0
                for code in list(portfolio.positions.keys()):
                    p = executor.get_live_price(code)
                    if p > 0:
                        prices[code] = p
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

                # Periodic logging + save
                cycle += 1
                if cycle % 5 == 0:
                    summary = portfolio.summary()
                    logger.info(f"Monitor: equity={summary['equity']:,.0f}, "
                                 f"pos={summary['n_positions']}, "
                                 f"daily={summary['daily_pnl']:.2%}, "
                                 f"risk={summary['risk_mode']}")
                    trade_logger.log_equity(
                        summary["equity"], summary["cash"],
                        summary["n_positions"],
                        summary["daily_pnl"], summary["monthly_dd"],
                        risk_mode=summary["risk_mode"],
                        rebalance_executed=session_rebalance_executed,
                        price_fail_count=session_price_fail_count,
                        reconcile_corrections=reconcile_corrections,
                        monitor_only=session_monitor_only)
                    state_mgr.save_portfolio(portfolio.to_dict())

                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Interrupted (Ctrl+C)")

    # Merge monitor price failures into session total
    session_price_fail_count += monitor_price_fail_count
    if monitor_price_fail_count > 0:
        logger.warning(f"Monitor price failures: {monitor_price_fail_count} "
                       f"(total session: {session_price_fail_count})")

    # ── Phase 4: EOD — Trail Stop Execution (close-based) ────────
    #    Wait until 15:30 so closing prices are settled.
    now = datetime.now()
    eod_target = now.replace(hour=EOD_EVAL_HOUR, minute=EOD_EVAL_MIN, second=0)
    if now < eod_target:
        wait_sec = (eod_target - now).total_seconds()
        logger.info(f"Monitor ended. EOD pending — waiting until "
                    f"{EOD_EVAL_HOUR}:{EOD_EVAL_MIN:02d} ({wait_sec:.0f}s)...")
        time.sleep(max(0, wait_sec))
    logger.info("EOD: evaluating trail stops on close prices...")

    # Final price update (EOD close)
    for code in list(portfolio.positions.keys()):
        p = executor.get_live_price(code)
        if p > 0:
            portfolio.positions[code].current_price = p

    # Trail stop check (close-based, same as backtest)
    # Result tracking: TRIGGERED → ORDER_SENT → FILLED / FAILED
    from report.reporter import make_event_id
    trail_triggered = 0
    trail_sent = 0
    trail_filled = 0
    trail_failed = 0

    for code in list(portfolio.positions.keys()):
        pos = portfolio.positions.get(code)
        if not pos or pos.current_price <= 0:
            continue

        triggered, new_hwm, exit_price = check_trail_stop(
            pos.high_watermark, pos.current_price, config.TRAIL_PCT)
        pos.high_watermark = new_hwm

        if triggered:
            trail_triggered += 1
            logger.warning(f"TRAIL_STOP_TRIGGERED {code}: hwm={new_hwm:,.0f}, "
                            f"close={pos.current_price:,.0f}")
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
                event_id=eid)

            # Order attempt
            trail_sent += 1
            result = executor.execute_sell(code, pos.quantity, "TRAIL_STOP")
            if not result.get("error"):
                trail_filled += 1
                fill_price = result["exec_price"] or pos.current_price
                trade = portfolio.remove_position(code, fill_price, config.SELL_COST)
                if trade:
                    trade["exit_reason"] = "TRAIL_STOP"
                    trade_logger.log_close(trade, "TRAIL_STOP_FILLED",
                                            "PAPER" if is_paper else "LIVE",
                                            event_id=eid)
                logger.info(f"TRAIL_STOP_FILLED {code}: price={fill_price:,.0f}")
            else:
                trail_failed += 1
                logger.error(f"TRAIL_STOP_FAILED {code}: {result['error']}")

    if trail_triggered > 0:
        logger.info(f"Trail stop summary: triggered={trail_triggered}, "
                    f"sent={trail_sent}, filled={trail_filled}, "
                    f"failed={trail_failed}")

    portfolio.end_of_day()
    state_mgr.save_portfolio(portfolio.to_dict())

    # ── EOD Daily Report: open positions snapshot ──────────────
    trade_logger.log_daily_positions(portfolio.positions)

    # ── EOD Daily HTML Report ────────────────────────────────
    try:
        from report.daily_report import generate_daily_report as gen_daily
        rpt_path = gen_daily(config.REPORT_DIR, config)
        if rpt_path:
            logger.info(f"Daily report: {rpt_path}")
    except Exception as e:
        logger.warning(f"Daily report generation failed: {e} (non-critical)")

    # ── Weekly Report (Friday EOD) ─────────────────────────
    if date.today().weekday() == 4:  # Friday
        try:
            from report.weekly_report import generate_weekly_report
            wrpt = generate_weekly_report(config.REPORT_DIR, config)
            if wrpt:
                logger.info(f"Weekly report: {wrpt}")
        except Exception as e:
            logger.warning(f"Weekly report generation failed: {e} (non-critical)")

    # ── Monthly Report (last trading day of month) ─────────
    tomorrow = date.today() + timedelta(days=1)
    if tomorrow.month != date.today().month:
        try:
            from report.monthly_report import generate_monthly_report
            mrpt = generate_monthly_report(config.REPORT_DIR, config)
            if mrpt:
                logger.info(f"Monthly report: {mrpt}")
        except Exception as e:
            logger.warning(f"Monthly report generation failed: {e} (non-critical)")

    summary = portfolio.summary()
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"],
        risk_mode=summary["risk_mode"],
        rebalance_executed=session_rebalance_executed,
        price_fail_count=session_price_fail_count,
        reconcile_corrections=reconcile_corrections,
        monitor_only=session_monitor_only)

    # Order summary
    order_sum = tracker.summary()
    logger.info(f"Orders: {order_sum}")

    # Ghost check
    ghosts = provider.get_ghost_orders()
    if ghosts:
        logger.critical(f"GHOST ORDERS: {len(ghosts)} unresolved! Check HTS!")
        for g in ghosts:
            logger.critical(f"  {g['side']} {g['code']} qty={g['requested_qty']} status={g['status']}")

    provider.shutdown()
    logger.info("=" * 40)
    logger.info("  EOD complete.")
    logger.info("=" * 40)

    try:
        app.quit()
    except Exception:
        pass


def _reconcile_with_broker(portfolio, provider, logger, trade_logger=None):
    """
    Sync internal state TO broker truth. Broker is always authoritative.

    Returns:
        dict with keys:
            "ok": bool — True if sync succeeded (even if corrections were made)
            "error": str — non-empty if sync failed entirely
            "corrections": int — number of fields corrected
    """
    from core.portfolio_manager import Position

    summary = provider.query_account_summary()
    if summary.get("error") and summary["error"] not in ("", "empty_account"):
        logger.warning(f"Broker sync failed: {summary['error']}")
        return {"ok": False, "error": summary["error"], "corrections": 0}
    if summary.get("holdings_reliable") is False:
        logger.warning("Broker holdings unreliable (msg_rejected) — cash only sync")
        broker_cash = summary.get("available_cash", 0)
        if broker_cash > 0:
            old_cash = portfolio.cash
            portfolio.cash = broker_cash
            logger.info(f"Cash synced: {old_cash:,.0f} -> {broker_cash:,.0f}")
        return {"ok": True, "error": "", "corrections": 0}

    # ── 1. Cash: broker wins ──────────────────────────────────────────
    corrections = 0
    broker_cash = summary.get("available_cash", 0)
    old_cash = portfolio.cash
    portfolio.cash = broker_cash
    if old_cash != broker_cash:
        corrections += 1
        logger.info(f"[RECON] Cash synced: {old_cash:,.0f} -> {broker_cash:,.0f}")

    # ── 2. Holdings: broker is truth ──────────────────────────────────
    broker_holdings = {h["code"]: h for h in summary.get("holdings", [])}
    internal_codes = set(portfolio.positions.keys())
    broker_codes = set(broker_holdings.keys())

    # 2a. ENGINE-ONLY → remove (broker sold or never held)
    for code in internal_codes - broker_codes:
        corrections += 1
        old_pos = portfolio.positions[code]
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
        )
        portfolio.positions[code] = pos
        logger.warning(f"[RECON] Added BROKER-ONLY position {code}: "
                       f"qty={h['qty']}, avg={h['avg_price']:,.0f}")
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
            corrections += 1
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

    synced = len(broker_holdings)
    if corrections > 0:
        logger.warning(f"[RECON] {corrections} corrections applied — state forced to broker truth")
    logger.info(f"[RECON] Done — cash={broker_cash:,.0f}, positions={synced}")
    return {"ok": True, "error": "", "corrections": corrections}


def _execute_rebalance_live(portfolio, target, config, executor, provider,
                             trade_logger, skip_buys, logger) -> int:
    """Execute rebalance with live Kiwoom orders.

    Returns: number of price-failed codes (for equity log context).
    """
    from strategy.rebalancer import compute_orders
    from report.reporter import make_event_id

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

    is_paper = executor.paper
    mode_str = "PAPER" if is_paper else "LIVE"

    sell_orders, buy_orders = compute_orders(
        current_positions={code: {"quantity": pos.quantity, "avg_price": pos.avg_price}
                           for code, pos in portfolio.positions.items()},
        target_tickers=target_tickers,
        total_equity=portfolio.get_current_equity(),
        current_cash=portfolio.cash,
        buy_cost=config.BUY_COST,
        sell_cost=config.SELL_COST,
        prices=prices)

    # ── Execute Sells First ──────────────────────────────────────
    logger.info(f"Sells: {len(sell_orders)} orders")
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
                event_id=eid)

        result = executor.execute_sell(order.ticker, order.quantity, "REBALANCE_EXIT")
        if not result.get("error"):
            fill_price = result["exec_price"] or prices.get(order.ticker, 0)
            trade = portfolio.remove_position(order.ticker, fill_price, config.SELL_COST)
            if trade:
                trade["exit_reason"] = "REBALANCE_EXIT"
                trade_logger.log_close(trade, "REBALANCE_EXIT", mode_str,
                                       event_id=eid)
        else:
            logger.error(f"SELL failed {order.ticker}: {result['error']}")

    time.sleep(2)  # Brief pause between sells and buys

    # ── Execute Buys ─────────────────────────────────────────────
    if skip_buys:
        logger.warning("Buys BLOCKED by DD guard. Sells completed only.")
        return len(price_fail_codes)

    logger.info(f"Buys: {len(buy_orders)} orders")
    for rank_idx, order in enumerate(buy_orders, 1):
        live_price = executor.get_live_price(order.ticker)
        if live_price <= 0:
            logger.warning(f"No price for {order.ticker}, skip")
            continue

        eid = make_event_id(order.ticker, "BUY")
        s = scores.get(order.ticker, {})

        # Decision log: buy context
        trade_logger.log_decision_buy(
            order.ticker, "REBALANCE_ENTRY",
            score_vol=s.get("vol_12m", 0),
            score_mom=s.get("mom_12_1", 0),
            rank=rank_idx,
            target_weight=order.target_amount,
            price=live_price,
            cash_before=portfolio.cash,
            event_id=eid)

        # Recalculate qty with fresh price
        qty = int(order.target_amount / (live_price * (1 + config.BUY_COST)))
        if qty <= 0:
            continue

        result = executor.execute_buy(order.ticker, qty, "REBALANCE_ENTRY")
        if not result.get("error"):
            fill_price = result["exec_price"] or live_price
            portfolio.add_position(
                order.ticker, result["exec_qty"], fill_price,
                entry_date=str(date.today()),
                buy_cost=config.BUY_COST)
        else:
            logger.error(f"BUY failed {order.ticker}: {result['error']}")

    # Summary: skipped buys due to price failure
    buy_skipped = [o.ticker for o in buy_orders
                   if prices.get(o.ticker, 0) <= 0]
    if buy_skipped or price_fail_codes:
        logger.warning(f"Rebalance summary — price-failed codes: {price_fail_codes}, "
                       f"buy-skipped: {buy_skipped}")

    trade_logger.log_rebalance_summary(len(sell_orders), len(buy_orders),
                                        portfolio.get_current_equity())
    return len(price_fail_codes)


# ── Mock Mode ────────────────────────────────────────────────────────────────
def run_mock(config):
    """Mock mode: test state + target + reporter without broker."""
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from strategy.factor_ranker import load_target_portfolio
    from report.reporter import TradeLogger

    logger = logging.getLogger("gen4.mock")
    logger.info("=" * 60)
    logger.info("  Gen4 Mock Mode")
    logger.info("=" * 60)

    state_mgr = StateManager(config.STATE_DIR, paper=True)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    trade_logger = TradeLogger(config.REPORT_DIR)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)
        logger.info(f"Restored: {len(portfolio.positions)} positions")

    target = load_target_portfolio(config.SIGNALS_DIR)
    if target:
        logger.info(f"Target: {len(target['target_tickers'])} stocks (date: {target.get('date', '?')})")
        for i, tk in enumerate(target["target_tickers"][:5], 1):
            s = target["scores"].get(tk, {})
            logger.info(f"  {i}. {tk}  mom={s.get('mom_12_1',0):.4f}")
        if len(target["target_tickers"]) > 5:
            logger.info(f"  ... and {len(target['target_tickers'])-5} more")
    else:
        logger.info("No target portfolio. Run --batch first.")

    summary = portfolio.summary()
    logger.info(f"Portfolio: equity={summary['equity']:,.0f}, "
                 f"cash={summary['cash']:,.0f}, pos={summary['n_positions']}, "
                 f"risk={summary['risk_mode']}")

    # Log equity snapshot
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"])

    state_mgr.save_portfolio(portfolio.to_dict())
    logger.info("State saved. Mock complete.")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gen4 Core Trading System")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", action="store_true", help="Batch: update data + scoring")
    group.add_argument("--live", action="store_true", help="Live: Kiwoom trading")
    group.add_argument("--rebalance", action="store_true", help="Force rebalance now")
    group.add_argument("--backtest", action="store_true", help="Run backtester")
    group.add_argument("--mock", action="store_true", help="Mock mode (test)")
    parser.add_argument("--start", default="2019-01-02")
    parser.add_argument("--end", default="2026-03-20")
    args = parser.parse_args()

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
        run_batch(config)
    elif args.live:
        setup_logging(config.LOG_DIR, "live")
        run_live(config)
    elif args.mock:
        setup_logging(config.LOG_DIR, "mock")
        run_mock(config)
    elif args.rebalance:
        setup_logging(config.LOG_DIR, "rebalance")
        run_live(config)


if __name__ == "__main__":
    main()
