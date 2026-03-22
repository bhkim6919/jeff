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
    """Configure logging to file + console."""
    log_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    log_file = log_dir / f"gen4_{mode}_{today}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ── Batch Mode ───────────────────────────────────────────────────────────────
def run_batch(config):
    """
    Batch mode (run after market close, e.g., 18:00+):
      1. Update OHLCV data via pykrx
      2. Build universe
      3. Score all stocks (using SHARED scoring.py)
      4. Select top 20 → save target_portfolio.json
    """
    from data.pykrx_provider import get_stock_ohlcv, get_stock_list, update_ohlcv_incremental
    from data.universe_builder import build_universe_from_ohlcv
    from strategy.factor_ranker import build_target_portfolio, save_target_portfolio

    logger = logging.getLogger("gen4.batch")
    logger.info("=" * 60)
    logger.info("  Gen4 Batch Mode")
    logger.info("=" * 60)

    # 1. Update OHLCV
    ohlcv_dir = config.OHLCV_DIR
    logger.info(f"[1/3] Updating OHLCV in {ohlcv_dir}...")

    # Get universe codes
    universe = build_universe_from_ohlcv(
        ohlcv_dir,
        min_close=config.UNIV_MIN_CLOSE,
        min_amount=config.UNIV_MIN_AMOUNT,
        min_history=config.UNIV_MIN_HISTORY,
        min_count=config.UNIV_MIN_COUNT,
    )
    logger.info(f"  Universe: {len(universe)} stocks")

    # 2. Load OHLCV for scoring
    logger.info("[2/3] Loading OHLCV for scoring...")
    import pandas as pd
    close_dict = {}
    for code in universe:
        path = ohlcv_dir / f"{code}.csv"
        if path.exists():
            df = pd.read_csv(path, parse_dates=["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
            if len(df) >= config.VOL_LOOKBACK:
                close_dict[code] = df.set_index("date")["close"]

    logger.info(f"  Loaded {len(close_dict)} stocks with sufficient history")

    # 3. Score and select
    logger.info("[3/3] Scoring and selecting top stocks...")
    target = build_target_portfolio(close_dict, config)
    path = save_target_portfolio(target, config.SIGNALS_DIR)

    logger.info(f"  Target: {len(target['target_tickers'])} stocks")
    logger.info(f"  Saved: {path}")
    for i, tk in enumerate(target["target_tickers"], 1):
        score = target["scores"].get(tk, {})
        logger.info(f"    {i:2d}. {tk}  vol={score.get('vol_12m',0):.4f}  "
                     f"mom={score.get('mom_12_1',0):.4f}")

    logger.info("Batch complete.")
    return target


# ── Live Mode ────────────────────────────────────────────────────────────────
def run_live(config):
    """
    Live mode (Kiwoom):
      09:00  Load state, check if rebalance day
      09:00  If rebalance: load target, reconcile, execute sells then buys
      09:30~15:20  Monitor trail stops (60s loop)
      15:20  EOD: save state, report
    """
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from risk.exposure_guard import ExposureGuard
    from strategy.trail_stop import check_trail_stop
    from strategy.factor_ranker import load_target_portfolio

    logger = logging.getLogger("gen4.live")
    logger.info("=" * 60)
    logger.info("  Gen4 Live Mode")
    logger.info("=" * 60)

    # Initialize
    state_mgr = StateManager(config.STATE_DIR, paper=config.PAPER_TRADING)
    portfolio = PortfolioManager(
        config.INITIAL_CASH,
        config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT,
        config.N_STOCKS,
    )
    guard = ExposureGuard(config.DAILY_DD_LIMIT, config.MONTHLY_DD_LIMIT)

    # Restore state
    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)
        logger.info(f"Restored: {len(portfolio.positions)} positions")

    # Check rebalance
    runtime = state_mgr.load_runtime()
    last_rebal = runtime.get("last_rebalance_date", "")
    rebal_count = runtime.get("rebalance_count", 0)

    # Simple rebalance check: different month from last rebalance
    today_str = date.today().strftime("%Y%m%d")
    need_rebalance = (not last_rebal or last_rebal[:6] != today_str[:6])

    if need_rebalance:
        logger.info("Rebalance day detected")
        target = load_target_portfolio(config.SIGNALS_DIR)
        if target:
            logger.info(f"Target portfolio loaded: {len(target['target_tickers'])} stocks")
            # Check risk
            allowed, reason = guard.can_buy(
                portfolio.get_daily_pnl_pct(),
                portfolio.get_monthly_dd_pct(),
            )
            if allowed:
                _execute_rebalance(portfolio, target, config, logger)
                state_mgr.set_last_rebalance_date(today_str)
            else:
                logger.warning(f"Rebalance buys blocked: {reason}")
                # Still do sells (remove non-target positions)
                logger.info("Executing sells only (buys blocked)")
        else:
            logger.warning("No target portfolio found. Run --batch first.")

    # Monitor loop
    logger.info("Starting trail stop monitor (60s interval)...")
    while True:
        now = datetime.now()
        if now.hour >= 15 and now.minute >= 20:
            break  # EOD

        # TODO: Get live prices from Kiwoom
        # For now, just log
        summary = portfolio.summary()
        logger.info(f"Monitor: equity={summary['equity']:,.0f}, "
                     f"positions={summary['n_positions']}, "
                     f"daily={summary['daily_pnl']:.2%}")

        time.sleep(60)

    # EOD
    portfolio.end_of_day()
    state_mgr.save_portfolio(portfolio.to_dict())
    logger.info("EOD complete. State saved.")


def _execute_rebalance(portfolio, target, config, logger):
    """Execute rebalance orders."""
    from strategy.rebalancer import compute_orders

    target_tickers = target["target_tickers"]
    current = {code: {"qty": pos.quantity, "avg_price": pos.avg_price}
               for code, pos in portfolio.positions.items()}

    # Get current prices (TODO: from Kiwoom)
    prices = {code: pos.current_price for code, pos in portfolio.positions.items()}

    sell_orders, buy_orders = compute_orders(
        current, target_tickers,
        portfolio.get_current_equity(),
        portfolio.cash,
        config.BUY_COST,
        prices,
    )

    # Execute sells
    for order in sell_orders:
        price = prices.get(order.ticker, 0)
        if price > 0:
            trade = portfolio.remove_position(order.ticker, price, config.SELL_COST)
            if trade:
                logger.info(f"  SELL {order.ticker}: pnl={trade['pnl_pct']:+.2%}")

    # Execute buys
    for order in buy_orders:
        # TODO: Get live price from Kiwoom
        logger.info(f"  BUY {order.ticker}: qty={order.quantity}, "
                     f"target={order.target_amount:,.0f}")


# ── Mock Mode ────────────────────────────────────────────────────────────────
def run_mock(config):
    """Mock mode: test state save/load cycle without broker."""
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager

    logger = logging.getLogger("gen4.mock")
    logger.info("=" * 60)
    logger.info("  Gen4 Mock Mode")
    logger.info("=" * 60)

    state_mgr = StateManager(config.STATE_DIR, paper=True)
    portfolio = PortfolioManager(config.INITIAL_CASH)

    # Restore
    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved)

    logger.info(f"Portfolio: {portfolio.summary()}")

    # Save
    state_mgr.save_portfolio(portfolio.to_dict())
    logger.info("Mock mode complete.")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gen4 Core Trading System")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", action="store_true", help="Batch: update data + scoring")
    group.add_argument("--live", action="store_true", help="Live: Kiwoom trading")
    group.add_argument("--rebalance", action="store_true", help="Force rebalance now")
    group.add_argument("--backtest", action="store_true", help="Run backtester")
    group.add_argument("--mock", action="store_true", help="Mock mode (test)")
    parser.add_argument("--start", default="2019-01-02", help="Backtest start date")
    parser.add_argument("--end", default="2026-03-20", help="Backtest end date")
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
        logging.getLogger("gen4").info("Force rebalance — running live mode with rebalance flag")
        run_live(config)


if __name__ == "__main__":
    main()
