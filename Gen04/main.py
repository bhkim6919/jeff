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
import signal
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


def _resolve_trading_mode(config) -> str:
    """Resolve TRADING_MODE from config, with PAPER_TRADING backward compat."""
    mode = getattr(config, "TRADING_MODE", None)
    if mode and mode in ("mock", "paper", "live"):
        return mode
    # Fallback: derive from deprecated PAPER_TRADING
    if getattr(config, "PAPER_TRADING", True):
        logging.getLogger("gen4").warning(
            "[DEPRECATED_CONFIG] PAPER_TRADING is deprecated; use TRADING_MODE")
        return "paper"
    return "live"


def validate_trading_mode(trading_mode: str, server_type: str,
                          broker_connected: bool = True) -> None:
    """
    Hard gate: abort if trading_mode and server_type mismatch.

    TRADING_MODE is the operator's intended mode.
    server_type is the broker's actual connected environment.
    If they do not match, abort immediately.
      mock  = internal simulation only
      paper = broker mock trading
      live  = broker real trading

    Raises RuntimeError on mismatch.
    """
    _logger = logging.getLogger("gen4.live")

    if trading_mode == "mock":
        if broker_connected:
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=mock but broker is connected "
                f"(server_type={server_type}). Mock mode must not use broker.")
        return  # mock + no broker = OK

    if trading_mode == "paper":
        if server_type != "MOCK":
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=paper server_type={server_type}. "
                f"Paper mode requires MOCK server (모의투자).")
        return  # paper + MOCK = OK

    if trading_mode == "live":
        if server_type != "REAL":
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=live server_type={server_type}. "
                f"Live mode requires REAL server.")
        return  # live + REAL = OK

    raise RuntimeError(f"[MODE_MISMATCH_ABORT] Unknown trading_mode={trading_mode!r}")


def _safe_save(state_mgr, portfolio, context: str = "",
               max_retries: int = 3, retry_delay: float = 0.5) -> bool:
    """Save portfolio state with retry and logging."""
    _logger = logging.getLogger("gen4.live")
    for attempt in range(1, max_retries + 1):
        saved = state_mgr.save_portfolio(portfolio.to_dict())
        if saved:
            _logger.info(f"[STATE_SAVE_OK] {context}")
            return True
        if attempt < max_retries:
            _logger.warning(f"[STATE_SAVE_RETRY] {context} — attempt {attempt}/{max_retries}")
            time.sleep(retry_delay)
    _logger.error(f"[STATE_SAVE_FAIL] {context} — {max_retries} attempts exhausted!")
    return False


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

    trading_mode = actual_mode  # use actual (validated against intent)
    logger.info(f"[TRADING_MODE] {trading_mode}  "
                f"(intended={intended_mode}, server={server_type})")

    provider = Gen4KiwoomProvider(kiwoom, str(config.SECTOR_MAP))

    tracker = OrderTracker(journal_dir=config.LOG_DIR, trading_mode=trading_mode)
    trade_logger = TradeLogger(config.REPORT_DIR)
    # simulate=False: orders go via Kiwoom API. Server determines virtual/real.
    executor = OrderExecutor(provider, tracker, trade_logger,
                             simulate=False, trading_mode=trading_mode)

    mode_label = trading_mode.upper()  # "PAPER" or "LIVE"
    logger.info(f"Mode: {mode_label}  (server={server_type})")

    # ── Phase 1: State Restore + Broker Sync ─────────────────────
    state_mgr = StateManager(config.STATE_DIR, trading_mode=trading_mode)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)
        logger.info(f"Restored: {len(portfolio.positions)} positions, cash={portfolio.cash:,.0f}")

    # Ghost fill sync: executor needs portfolio + state_mgr for immediate sync
    executor.set_ghost_fill_context(portfolio, state_mgr,
                                     buy_cost=config.BUY_COST)
    provider.set_ghost_fill_callback(executor.on_ghost_fill)

    # Wait for broker data to settle after login (mitigation, not root fix)
    logger.info("[RECON_WAIT] Waiting 3s for broker data to settle...")
    time.sleep(3.0)
    logger.info("[RECON_WAIT] Done — proceeding with reconciliation")

    # Broker reconciliation — broker is truth, engine state synced (with safety guards)
    recon = _reconcile_with_broker(portfolio, provider, logger, trade_logger)
    reconcile_corrections = recon.get("corrections", 0) if recon else 0
    if recon and reconcile_corrections > 0:
        _safe_save(state_mgr, portfolio,
                   context=f"recon/{reconcile_corrections}corrections")
        logger.info(f"[RECON] State saved after {recon['corrections']} corrections")
    if recon and not recon.get("ok", True):
        logger.critical("Broker sync FAILED — aborting LIVE to prevent stale-state trading")
        save_forensic_snapshot(
            config.STATE_DIR,
            portfolio_data=portfolio.to_dict(),
            error_msg=f"Broker sync failed: {recon.get('error', 'unknown')}",
            extra={"recon": recon})
        return
    # Session-level context for equity log (initialized early for safe_mode path)
    session_rebalance_executed = False
    session_price_fail_count = 0
    session_monitor_only = False

    if recon and recon.get("safe_mode"):
        reason = recon.get("safe_mode_reason", "excessive corrections")
        logger.critical(f"[RECON_SAFE_MODE] {reason} — blocking new entries for this session")
        guard.force_safe_mode(reason)
        # holdings_unreliable → full monitor-only (block rebalance sells too)
        if "holdings_unreliable" in reason:
            session_monitor_only = True
            logger.critical("[BROKER_STATE_UNRELIABLE] Session forced to MONITOR-ONLY. "
                            "No rebalance, no buy, no sell until next session with "
                            "reliable holdings.")

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

    # Monitor-only guard: skip rebalance entirely if session is monitor-only
    if need_rebalance and session_monitor_only:
        logger.critical("[MONITOR_ONLY] Rebalance day but session is MONITOR-ONLY "
                        "(holdings unreliable or forced safe mode). Skipping rebalance.")
        need_rebalance = False

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

                try:
                    pfail = _execute_rebalance_live(
                        portfolio, target, config, executor, provider,
                        trade_logger, skip_buys, logger,
                        state_mgr=state_mgr, today_str=today_str,
                        buy_scale=buy_scale, risk_action=risk_action,
                        mode_str=mode_label)
                    session_rebalance_executed = True
                    session_price_fail_count = pfail

                    # Mark trim executed (prevents same-day repeat)
                    if risk_action.get("trim_ratio", 0) > 0:
                        guard.mark_trim_executed(risk_action["level"])

                    # Commit order: portfolio FIRST, then runtime.
                    # If portfolio save fails, rebalance date stays unmarked → retry next session.
                    portfolio_saved = _safe_save(
                        state_mgr, portfolio, context="rebalance_commit/portfolio")
                    if portfolio_saved:
                        state_mgr.set_last_rebalance_date(today_str)
                        logger.info("[REBALANCE_COMMIT_OK] Portfolio saved, "
                                    "rebalance date marked: %s", today_str)
                    else:
                        logger.critical(
                            "[REBALANCE_COMMIT_PARTIAL_FAIL] Portfolio save failed! "
                            "Rebalance date NOT marked — will retry next session.")
                except Exception as e:
                    logger.error(f"Rebalance crashed: {e}", exc_info=True)
                    # Save portfolio (preserve sell results) but do NOT mark date
                    # → next session will retry rebalance
                    _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")
                    logger.info("Crash recovery: portfolio saved, "
                                "rebalance date NOT marked (will retry)")
    else:
        logger.info(f"Not rebalance day (last: {last_rebal})")

    # ── Phase 2.5: Intraday Collector Setup ──────────────────────
    from data.intraday_collector import IntradayCollector
    collector = IntradayCollector(config.INTRADAY_DIR,
                                  date.today().strftime("%Y-%m-%d"))
    collector.set_active_codes(list(portfolio.positions.keys()))

    if portfolio.positions:
        provider.set_real_data_callback(collector.on_tick)
        provider.register_real(list(portfolio.positions.keys()), fids="10;27")
        logger.info("[Intraday] Real-time tick collection started "
                    f"for {len(portfolio.positions)} positions")

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

        except KeyboardInterrupt:
            logger.info("Interrupted (Ctrl+C)")
        except Exception as e:
            logger.error(f"Monitor loop crashed: {e}", exc_info=True)
        finally:
            # Always save state + flush intraday + cleanup on monitor exit
            try:
                collector.flush_all()
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
        logger.info("[EOD_SKIP] Ctrl+C — skipping trail stop + EOD evaluation")
        # Still save state and cleanup
        _safe_save(state_mgr, portfolio, context="early_exit")
        provider.shutdown()
        signal.signal(signal.SIGINT, prev_handler)
        try:
            app.quit()
        except Exception:
            pass
        return
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
            logger.warning(f"[EOD_PRICE_MISSING] {code}: decision=SKIP_NO_PRICE "
                           f"(source={price_source})")
            continue

        # [BEHAVIOR CHANGE] Only execute trail stop on verified close prices.
        # Cached/fallback prices may be stale → skip execution, log warning.
        if price_source in ("provider_cached", "position_fallback"):
            logger.warning(
                f"[EOD_SKIP_NO_OFFICIAL_CLOSE] {code}: price={close_price:,.0f} "
                f"source={price_source} — trail stop check SKIPPED "
                f"(non-official price). HWM update only.")
            # Still update HWM for observability, but do NOT trigger exit
            if close_price > pos.high_watermark:
                pos.high_watermark = close_price
            continue

        triggered, new_hwm, exit_price = check_trail_stop(
            pos.high_watermark, close_price, config.TRAIL_PCT)
        pos.high_watermark = new_hwm

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
                event_id=eid)

            # Order attempt
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
                    trade_logger.log_close(trade, "TRAIL_STOP_FILLED",
                                            mode_label,
                                            event_id=eid)
                logger.info(f"TRAIL_STOP_FILLED {code}: price={fill_price:,.0f}")
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
    provider.unregister_real()
    provider.set_real_data_callback(None)

    portfolio.end_of_day()
    _safe_save(state_mgr, portfolio, context="EOD")

    # ── EOD Daily Report: open positions snapshot ──────────────
    trade_logger.log_daily_positions(
        portfolio.positions,
        buy_cost=config.BUY_COST,
        sell_cost=config.SELL_COST)

    # ── EOD KOSPI close injection (Kiwoom opt20006) ──────────
    try:
        kospi_close = provider.get_kospi_close()
        if kospi_close > 0:
            from report.kospi_utils import inject_kospi_close
            inject_kospi_close(config.INDEX_FILE,
                               date.today().strftime("%Y-%m-%d"), kospi_close)
            logger.info(f"KOSPI close injected: {kospi_close:.2f}")
    except Exception as e:
        logger.warning(f"KOSPI fetch failed: {e} (non-critical)")

    # ── EOD Intraday Analysis + Daily HTML Report ────────────
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
            config.INTRADAY_DIR, today_date_str)
        if ia_bars:
            prev_closes = ia_prev_closes(
                config.INTRADAY_DIR, today_date_str, list(ia_bars.keys()))
            ia_results = ia_analyze_all(ia_bars, prev_closes)
            intraday_summary = ia_generate_summary(ia_results, today_date_str)
            ia_save_json(intraday_summary, config.REPORT_DIR, today_date_str)
            ia_save_csv(intraday_summary, config.REPORT_DIR, today_date_str)
            logger.info(f"[INTRADAY_ANALYSIS] {intraday_summary['n_stocks']} stocks, "
                        f"risk_score={intraday_summary.get('risk_score', 'N/A')}, "
                        f"worst_dd={intraday_summary.get('worst_dd_pct', 0):.2f}%")
        else:
            logger.info("[INTRADAY_ANALYSIS] No intraday data for today")
    except Exception as e:
        logger.warning(f"Intraday analysis failed: {e} (non-critical)")

    try:
        from report.daily_report import generate_daily_report as gen_daily
        rpt_path = gen_daily(config.REPORT_DIR, config,
                              intraday_dir=config.INTRADAY_DIR,
                              intraday_summary=intraday_summary)
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
                "[RECON_CASH_SPIKE] %,.0f -> %,.0f (%.0f%% change)",
                old_cash, broker_cash, cash_change_ratio * 100)
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
        )
        portfolio.positions[code] = pos
        correction_details.append(("BROKER_ONLY", code, "0", str(h["qty"])))
        logger.critical(f"[RECON_BROKER_ONLY] Added {code}: "
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
    return {"ok": True, "error": "", "corrections": corrections,
            "safe_mode": safe_mode, "safe_mode_reason": safe_mode_reason}


def _execute_dd_trim(portfolio, trim_ratio, executor, config,
                      trade_logger, mode_str, logger):
    """Trim all positions by trim_ratio during DD drawdown.
    Called once per rebalance — same-day duplicate trim prevented by caller.
    """
    from report.reporter import make_event_id

    trimmed = 0
    for code in list(portfolio.positions.keys()):
        pos = portfolio.positions[code]
        qty_to_sell = int(pos.quantity * trim_ratio)
        if qty_to_sell <= 0:
            continue

        price = executor.get_live_price(code)
        if price <= 0:
            logger.warning(f"[DD_TRIM] {code}: no price, skip")
            continue

        eid = make_event_id(code, "DD_TRIM")
        result = executor.execute_sell(code, qty_to_sell, "DD_REDUCTION")
        if not result.get("error"):
            fill_price = result["exec_price"] or price
            exec_qty = result.get("exec_qty", qty_to_sell)
            trade = portfolio.remove_position(code, fill_price, config.SELL_COST,
                                              qty=exec_qty)
            if trade:
                trade["exit_reason"] = "DD_REDUCTION"
                trade_logger.log_close(trade, "DD_REDUCTION", mode_str,
                                       event_id=eid)
            remaining = portfolio.positions[code].quantity if code in portfolio.positions else 0
            logger.warning(f"[DD_POSITION_REDUCED] {code}: sold {exec_qty}, "
                           f"remaining={remaining}")
            trimmed += 1
        else:
            logger.error(f"[DD_TRIM] {code}: sell failed: {result['error']}")

    logger.info(f"[DD_TRIM_DONE] trimmed {trimmed} positions by {trim_ratio:.0%}")


def _execute_rebalance_live(portfolio, target, config, executor, provider,
                             trade_logger, skip_buys, logger,
                             state_mgr=None, today_str="",
                             buy_scale: float = 1.0,
                             risk_action: dict = None,
                             mode_str: str = "LIVE") -> int:
    """Execute rebalance with live Kiwoom orders.

    Returns: number of price-failed codes (for equity log context).
    """
    from strategy.rebalancer import compute_orders
    from report.reporter import make_event_id

    # -- Rebalance dedup: reject if already executed today --
    if state_mgr and today_str:
        runtime_check = state_mgr.load_runtime()
        if runtime_check.get("last_rebalance_date") == today_str:
            logger.warning("Rebalance already recorded today (%s) — SKIP", today_str)
            return 0

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
            exec_qty = result.get("exec_qty", order.quantity)
            logger.info(f"[PORTFOLIO] SELL {order.ticker} requested={order.quantity} "
                        f"exec_qty={exec_qty} fill_price={fill_price:,.0f}")
            trade = portfolio.remove_position(order.ticker, fill_price, config.SELL_COST,
                                              qty=exec_qty)
            if trade:
                trade["exit_reason"] = "REBALANCE_EXIT"
                trade_logger.log_close(trade, "REBALANCE_EXIT", mode_str,
                                       event_id=eid)
        else:
            logger.error(f"SELL failed {order.ticker}: {result['error']}")

    # Post-sell checkpoint: save portfolio (positions/cash) but NOT rebalance date.
    # If buys crash, rebalance date stays unmarked → next session retries.
    # Already-sold positions won't be re-sold (compute_orders uses set diff).
    if state_mgr:
        _safe_save(state_mgr, portfolio, context="recon/monitor/checkpoint")
        logger.info("Post-sell checkpoint saved (rebalance date NOT yet marked)")

    time.sleep(2)  # Brief pause between sells and buys

    # ── DD Graduated: Position Trim (before buys) ────────────────
    if risk_action and risk_action.get("trim_ratio", 0) > 0:
        trim_ratio = risk_action["trim_ratio"]
        level = risk_action["level"]
        logger.warning(f"[DD_TRIM_START] {level}: trimming {trim_ratio:.0%} of all positions")
        _execute_dd_trim(portfolio, trim_ratio, executor, config,
                         trade_logger, mode_str, logger)

    # ── Execute Buys ─────────────────────────────────────────────
    if skip_buys or buy_scale <= 0:
        reason = risk_action["level"] if risk_action else "DD_GUARD"
        logger.warning(f"[DD_GUARD_TRIGGERED] {reason}: buys BLOCKED")
        return len(price_fail_codes)

    if buy_scale < 1.0:
        logger.info(f"[DD_BUY_SCALED] buy allocation * {buy_scale:.0%}")

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

        # Recalculate qty with fresh price (apply DD buy_scale)
        scaled_amount = order.target_amount * buy_scale
        qty = int(scaled_amount / (live_price * (1 + config.BUY_COST)))
        if qty <= 0:
            continue

        result = executor.execute_buy(order.ticker, qty, "REBALANCE_ENTRY")
        if not result.get("error"):
            fill_price = result["exec_price"] or live_price
            applied_qty = result["exec_qty"]
            logger.info(f"[PORTFOLIO] BUY {order.ticker} requested={qty} "
                        f"exec_qty={applied_qty} fill_price={fill_price:,.0f}")
            portfolio.add_position(
                order.ticker, applied_qty, fill_price,
                entry_date=str(date.today()),
                buy_cost=config.BUY_COST)
            _safe_save(state_mgr, portfolio, context=f"buy/{order.ticker}")
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

    trading_mode = "mock"
    logger = logging.getLogger("gen4.mock")
    logger.info("=" * 60)
    logger.info("  Gen4 Mock Mode")
    logger.info(f"  [TRADING_MODE] {trading_mode}")
    logger.info("=" * 60)

    state_mgr = StateManager(config.STATE_DIR, trading_mode=trading_mode)
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

    _safe_save(state_mgr, portfolio, context="mock_complete")
    logger.info("Mock complete.")


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
    group.add_argument("--rebalance", action="store_true", help="Force rebalance now")
    group.add_argument("--backtest", action="store_true", help="Run backtester")
    group.add_argument("--mock", action="store_true", help="Mock mode (test)")
    parser.add_argument("--start", default="2019-01-02")
    parser.add_argument("--end", default=str(date.today()))
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
        _run_live_with_restart(config)
    elif args.mock:
        setup_logging(config.LOG_DIR, "mock")
        run_mock(config)
    elif args.rebalance:
        setup_logging(config.LOG_DIR, "rebalance")
        run_live(config)


if __name__ == "__main__":
    main()
