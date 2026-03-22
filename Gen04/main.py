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
from datetime import date, datetime
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

    if not is_weekday():
        logger.warning("Weekend — batch may use stale data.")

    ohlcv_dir = config.OHLCV_DIR

    # Step 1: pykrx OHLCV update
    logger.info("[1/4] Updating OHLCV via pykrx...")
    try:
        existing = [f.stem for f in ohlcv_dir.glob("*.csv")]
        codes = existing if existing else get_stock_list("KOSPI")
        if codes:
            updated = update_ohlcv_incremental(ohlcv_dir, codes, days=30)
            logger.info(f"  Updated {updated}/{len(codes)} stocks")
    except Exception as e:
        logger.warning(f"  pykrx update failed: {e}. Using existing data.")

    # Step 2: Build universe
    logger.info("[2/4] Building universe...")
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
    logger.info("[3/4] Loading OHLCV...")
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
    logger.info("[4/5] Scoring...")
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
    from report.reporter import TradeLogger

    kiwoom, server_type = create_loggedin_kiwoom()
    is_paper = (server_type == "MOCK") or config.PAPER_TRADING
    provider = Gen4KiwoomProvider(kiwoom, str(config.SECTOR_MAP))

    tracker = OrderTracker()
    trade_logger = TradeLogger(config.REPORT_DIR)
    executor = OrderExecutor(provider, tracker, trade_logger, paper=is_paper)

    logger.info(f"Mode: {'PAPER' if is_paper else 'LIVE'}")

    # ── Phase 1: State Restore + Broker Sync ─────────────────────
    state_mgr = StateManager(config.STATE_DIR, paper=is_paper)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)
        logger.info(f"Restored: {len(portfolio.positions)} positions, cash={portfolio.cash:,.0f}")

    # Broker reconciliation
    _reconcile_with_broker(portfolio, provider, logger)

    # ── Phase 2: Rebalance Check ─────────────────────────────────
    runtime = state_mgr.load_runtime()
    last_rebal = runtime.get("last_rebalance_date", "")
    today_str = date.today().strftime("%Y%m%d")
    need_rebalance = (not last_rebal or last_rebal[:6] != today_str[:6])

    if need_rebalance:
        logger.info("=" * 40)
        logger.info("  REBALANCE DAY")
        logger.info("=" * 40)

        target = load_target_portfolio(config.SIGNALS_DIR)
        if target:
            data_date = target.get("date", "?")
            logger.info(f"Target loaded: {len(target['target_tickers'])} stocks (data: {data_date})")

            # Warn stale target
            try:
                if abs(int(today_str) - int(data_date)) > 5:
                    logger.warning(f"Target is stale ({data_date})! Run --batch to refresh.")
            except (ValueError, TypeError):
                pass

            # Risk check
            skip_buys, reason = guard.should_skip_rebalance(
                portfolio.get_daily_pnl_pct(), portfolio.get_monthly_dd_pct())

            _execute_rebalance_live(
                portfolio, target, config, executor, provider,
                trade_logger, skip_buys, logger)

            state_mgr.set_last_rebalance_date(today_str)
            state_mgr.save_portfolio(portfolio.to_dict())
            logger.info("Rebalance done. State saved.")
        else:
            logger.error("No target portfolio! Run: python main.py --batch")
    else:
        logger.info(f"Not rebalance day (last: {last_rebal})")

    # ── Phase 3: Monitor Loop (HWM update + trail warning only) ────
    #    Trail stop EXECUTION happens at EOD (Phase 4) to match backtest
    #    (backtest uses daily close; live must also use EOD close).
    #    Intraday: update HWM, warn if near trigger, but do NOT execute.
    trail_warnings = set()  # codes warned during intraday

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
                if now.hour >= 15 and now.minute >= 20:
                    break
                if now.hour < 9:
                    time.sleep(60)
                    continue

                # Get live prices + update HWM (but do NOT trigger exits)
                prices = {}
                for code in list(portfolio.positions.keys()):
                    p = executor.get_live_price(code)
                    if p > 0:
                        prices[code] = p
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
                        summary["daily_pnl"], summary["monthly_dd"])
                    state_mgr.save_portfolio(portfolio.to_dict())

                time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Interrupted (Ctrl+C)")

    # ── Phase 4: EOD — Trail Stop Execution (close-based) ────────
    #    Evaluate trail stops using EOD close prices.
    #    This matches backtest behavior exactly.
    logger.info("EOD: evaluating trail stops on close prices...")

    # Final price update (EOD close)
    for code in list(portfolio.positions.keys()):
        p = executor.get_live_price(code)
        if p > 0:
            portfolio.positions[code].current_price = p

    # Trail stop check (close-based, same as backtest)
    for code in list(portfolio.positions.keys()):
        pos = portfolio.positions.get(code)
        if not pos or pos.current_price <= 0:
            continue

        triggered, new_hwm, exit_price = check_trail_stop(
            pos.high_watermark, pos.current_price, config.TRAIL_PCT)
        pos.high_watermark = new_hwm

        if triggered:
            logger.warning(f"TRAIL STOP TRIGGERED {code}: hwm={new_hwm:,.0f}, "
                            f"close={pos.current_price:,.0f}")
            result = executor.execute_sell(code, pos.quantity, "TRAIL_STOP")
            if not result.get("error"):
                fill_price = result["exec_price"] or pos.current_price
                trade = portfolio.remove_position(code, fill_price, config.SELL_COST)
                if trade:
                    trade["exit_reason"] = "TRAIL_STOP"
                    trade_logger.log_close(trade, "TRAIL_STOP",
                                            "PAPER" if is_paper else "LIVE")

    portfolio.end_of_day()
    state_mgr.save_portfolio(portfolio.to_dict())

    summary = portfolio.summary()
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"])

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


def _reconcile_with_broker(portfolio, provider, logger):
    """Compare internal state with broker holdings."""
    summary = provider.query_account_summary()
    if summary.get("error") and summary["error"] not in ("", "empty_account"):
        logger.warning(f"Broker sync failed: {summary['error']}")
        return

    # Update cash from broker
    broker_cash = summary.get("available_cash", 0)
    if broker_cash > 0:
        old_cash = portfolio.cash
        portfolio.cash = broker_cash
        logger.info(f"Cash synced: {old_cash:,.0f} -> {broker_cash:,.0f}")

    # Update prices from broker
    broker_holdings = {h["code"]: h for h in summary.get("holdings", [])}
    for code, h in broker_holdings.items():
        if code in portfolio.positions and h.get("cur_price", 0) > 0:
            portfolio.positions[code].current_price = h["cur_price"]

    # Log mismatches
    internal = set(portfolio.positions.keys())
    broker = set(broker_holdings.keys())
    engine_only = internal - broker
    broker_only = broker - internal
    if engine_only:
        logger.warning(f"ENGINE-ONLY (not in broker): {engine_only}")
    if broker_only:
        logger.warning(f"BROKER-ONLY (not in engine): {broker_only}")

    for code in internal & broker:
        eng_qty = portfolio.positions[code].quantity
        brk_qty = broker_holdings[code]["qty"]
        if eng_qty != brk_qty:
            logger.warning(f"QTY MISMATCH {code}: engine={eng_qty}, broker={brk_qty}")


def _execute_rebalance_live(portfolio, target, config, executor, provider,
                             trade_logger, skip_buys, logger):
    """Execute rebalance with live Kiwoom orders."""
    from strategy.rebalancer import compute_orders

    target_tickers = target["target_tickers"]

    # Get live prices for all involved stocks
    all_codes = set(portfolio.positions.keys()) | set(target_tickers)
    prices = {}
    for code in all_codes:
        p = executor.get_live_price(code)
        if p > 0:
            prices[code] = p
    portfolio.update_prices(prices)

    is_paper = executor.paper
    mode_str = "PAPER" if is_paper else "LIVE"

    sell_orders, buy_orders = compute_orders(
        current_positions={code: {"qty": pos.quantity, "avg_price": pos.avg_price}
                           for code, pos in portfolio.positions.items()},
        target_tickers=target_tickers,
        total_equity=portfolio.get_current_equity(),
        current_cash=portfolio.cash,
        buy_cost=config.BUY_COST,
        prices=prices)

    # ── Execute Sells First ──────────────────────────────────────
    logger.info(f"Sells: {len(sell_orders)} orders")
    for order in sell_orders:
        result = executor.execute_sell(order.ticker, order.quantity, "REBALANCE_EXIT")
        if not result.get("error"):
            fill_price = result["exec_price"] or prices.get(order.ticker, 0)
            trade = portfolio.remove_position(order.ticker, fill_price, config.SELL_COST)
            if trade:
                trade["exit_reason"] = "REBALANCE_EXIT"
                trade_logger.log_close(trade, "REBALANCE_EXIT", mode_str)
        else:
            logger.error(f"SELL failed {order.ticker}: {result['error']}")

    time.sleep(2)  # Brief pause between sells and buys

    # ── Execute Buys ─────────────────────────────────────────────
    if skip_buys:
        logger.warning("Buys BLOCKED by DD guard. Sells completed only.")
        return

    logger.info(f"Buys: {len(buy_orders)} orders")
    for order in buy_orders:
        live_price = executor.get_live_price(order.ticker)
        if live_price <= 0:
            logger.warning(f"No price for {order.ticker}, skip")
            continue

        # Recalculate qty with fresh price
        qty = int(order.target_amount / (live_price * (1 + config.BUY_COST)))
        if qty <= 0:
            continue

        result = executor.execute_buy(order.ticker, qty, "REBALANCE_ENTRY")
        if not result.get("error"):
            fill_price = result["exec_price"] or live_price
            portfolio.add_position(
                order.ticker, result["exec_qty"], fill_price,
                entry_date=str(date.today()))
        else:
            logger.error(f"BUY failed {order.ticker}: {result['error']}")

    trade_logger.log_rebalance_summary(len(sell_orders), len(buy_orders),
                                        portfolio.get_current_equity())


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
    portfolio = PortfolioManager(config.INITIAL_CASH)
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
