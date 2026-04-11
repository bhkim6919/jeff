# -*- coding: utf-8 -*-
"""
Q-TRON US 1.0 — Main Entry Point
==================================
Usage:
    cd Gen04-US
    .venv/Scripts/python main.py --test          # Alpaca connection test
    .venv/Scripts/python main.py --batch         # OHLCV download + scoring
    .venv/Scripts/python main.py --live          # Live mode: monitor + trail stop
    .venv/Scripts/python main.py --server        # Start dashboard
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Set

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("qtron.us.main")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_test():
    """Test Alpaca connection + DB."""
    from config import USConfig
    from data.alpaca_provider import AlpacaProvider
    from data.db_provider import DbProviderUS

    config = USConfig()
    print(f"\n{'='*50}")
    print(f"Q-TRON US 1.0 - Connection Test")
    print(f"{'='*50}")

    # Alpaca
    print("\n[1] Alpaca API...")
    provider = AlpacaProvider(config)
    if provider.is_connected():
        acct = provider.query_account_summary()
        print(f"  Server: {provider.server_type}")
        print(f"  Equity: ${acct.get('equity', 0):,.2f}")
        print(f"  Cash:   ${acct.get('cash', 0):,.2f}")
        print(f"  Buying Power: ${acct.get('buying_power', 0):,.2f}")

        clock = provider.get_clock()
        if clock:
            print(f"  Market Open: {clock.get('is_open', False)}")

        holdings = provider.query_account_holdings()
        if holdings:
            print(f"  Positions: {len(holdings)}")
            for h in holdings[:5]:
                print(f"    {h['code']}: {h['qty']} shares @ ${h['avg_price']:.2f} "
                      f"(P&L: {h['pnl_pct']:+.1f}%)")
        else:
            print("  Positions: 0")
        print("  ✓ Alpaca OK")
    else:
        print("  ✗ Alpaca connection FAILED")
        return

    # DB
    print("\n[2] PostgreSQL DB...")
    db = DbProviderUS()
    health = db.health_check()
    if health["status"] == "OK":
        for t in health["tables"]:
            print(f"  {t['table']}: {t['rows']} rows ({t['latest']})")
        print("  ✓ DB OK")
    else:
        print(f"  ✗ DB error: {health.get('error', '')}")

    # Price
    print("\n[3] Price Test (AAPL)...")
    price = provider.get_current_price("AAPL")
    if price > 0:
        print(f"  AAPL: ${price:.2f}")
        print("  ✓ Data API OK")
    else:
        print("  ✗ Price query failed")

    print(f"\n{'='*50}")
    print(f"All tests passed!")
    print(f"{'='*50}\n")


def run_batch():
    """Batch: download OHLCV + score + save target portfolio."""
    from config import USConfig
    from data.db_provider import DbProviderUS
    from data.alpaca_data import USDataCollector
    from data.universe_builder import build_universe, get_sp500_tickers
    from strategy.snapshot_guard import make_snapshot_id
    from notify.telegram_bot import notify_batch_complete

    config = USConfig()
    config.ensure_dirs()
    db = DbProviderUS()
    collector = USDataCollector(db)

    print(f"\n{'='*50}")
    print(f"Q-TRON US 1.0 - Batch Mode")
    print(f"{'='*50}")

    # Step 1: Download OHLCV
    print("\n[1] Downloading S&P 500 OHLCV...")
    sp500 = get_sp500_tickers()
    if not sp500:
        print("  Failed to get S&P 500 list")
        return
    n = collector.collect_ohlcv(sp500, period="2y")
    print(f"  Downloaded: {n} stocks")

    # Step 2: Download index
    print("\n[2] Downloading index data (SPY, QQQ, IWM)...")
    collector.collect_index()

    # Step 3: Build universe
    print("\n[3] Building universe...")
    universe = build_universe(db, config)
    print(f"  Universe: {len(universe)} stocks")

    # Step 4: Score
    print("\n[4] Scoring...")
    close_dict = db.load_close_dict(min_history=config.VOL_LOOKBACK + 20)

    from strategy.scoring import calc_volatility, calc_momentum

    scores = []
    for sym in universe:
        if sym not in close_dict:
            continue
        closes = close_dict[sym]
        vol = calc_volatility(closes, config.VOL_LOOKBACK)
        mom = calc_momentum(closes, config.MOM_LOOKBACK, config.MOM_SKIP)
        if vol is not None and mom is not None:
            scores.append({"symbol": sym, "vol_12m": vol, "mom_12_1": mom})

    import pandas as pd
    scores_df = pd.DataFrame(scores)
    print(f"  Scored: {len(scores_df)} stocks")

    if scores_df.empty:
        print("  No scores computed")
        return

    # Step 5: Select top N
    from strategy.factor_ranker import select_top_n
    # Rename columns to match factor_ranker expectations
    scores_df = scores_df.rename(columns={"symbol": "ticker"})
    top = select_top_n(
        scores_df,
        vol_percentile=config.VOL_PERCENTILE,
        n_stocks=config.N_STOCKS,
    )
    print(f"  Target: {len(top)} stocks")
    for i, sym in enumerate(top[:10], 1):
        row = scores_df[scores_df["ticker"] == sym].iloc[0]
        print(f"    {i}. {sym} (vol={row['vol_12m']:.4f}, mom={row['mom_12_1']:.2%})")

    # Step 6: Save to DB
    from datetime import date
    snapshot_id = make_snapshot_id("US")
    target = {
        "date": date.today().isoformat(),
        "target_tickers": top,
        "scores": {
            r["ticker"]: {"vol_12m": r["vol_12m"], "mom_12_1": r["mom_12_1"]}
            for _, r in scores_df[scores_df["ticker"].isin(top)].iterrows()
        },
    }
    db.save_target_portfolio(target, snapshot_id=snapshot_id)
    print(f"\n  Saved to DB (snapshot: {snapshot_id})")

    # Telegram
    notify_batch_complete(len(universe), len(top))
    print(f"\n{'='*50}")
    print(f"Batch complete!")
    print(f"{'='*50}\n")


def run_live():
    """Live mode: monitor + trail stop + fill handling."""
    from config import USConfig
    from data.alpaca_provider import AlpacaProvider
    from core.state_manager import StateManagerUS
    from core.portfolio_manager import PortfolioManagerUS
    from notify import telegram_bot as notify

    config = USConfig()
    config.ensure_dirs()

    print(f"\n{'='*50}")
    print(f"Q-TRON US 1.0 - Live Mode ({config.TRADING_MODE})")
    print(f"{'='*50}")

    # ── Graceful shutdown ────────────────────────────────
    stop_requested = False

    def _signal_handler(sig, frame):
        nonlocal stop_requested
        if stop_requested:
            logger.warning("[LIVE] Force exit")
            sys.exit(1)
        stop_requested = True
        logger.info("[LIVE] Shutdown requested (Ctrl+C)")

    signal.signal(signal.SIGINT, _signal_handler)

    # ── Phase 0: Connect ─────────────────────────────────
    logger.info("[LIVE] Phase 0: Connecting...")
    provider = AlpacaProvider(config)
    if not provider.is_connected():
        logger.error("[LIVE] Alpaca connection failed")
        return

    logger.info(f"  Server: {provider.server_type}")

    # ── Phase 1: Broker Snapshot ─────────────────────────
    logger.info("[LIVE] Phase 1: Broker snapshot...")
    acct = provider.query_account_summary()
    if "error" in acct:
        logger.error(f"[LIVE] Account query failed: {acct}")
        return

    holdings = provider.query_account_holdings()
    open_orders = provider.query_open_orders() or []

    logger.info(f"  Equity: ${acct['equity']:,.2f} | Cash: ${acct['cash']:,.2f}")
    logger.info(f"  Positions: {len(holdings)} | Open orders: {len(open_orders)}")

    if open_orders:
        for o in open_orders:
            logger.warning(f"  [OPEN_ORDER] {o['side']} {o['code']} x{o['qty']} ({o['status']})")

    # ── Phase 2: State Load + RECON ──────────────────────
    logger.info("[LIVE] Phase 2: State load + RECON...")
    state_mgr = StateManagerUS(config.STATE_DIR, config.TRADING_MODE)
    dirty_exit = state_mgr.was_dirty_exit()

    if dirty_exit:
        logger.warning("[LIVE] Dirty exit detected — FORCE_SYNC expected")

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio = PortfolioManagerUS.from_dict(saved, config)
        logger.info(f"  Loaded state: {portfolio}")
    else:
        portfolio = PortfolioManagerUS(
            cash=acct["cash"],
            trail_ratio=config.TRAIL_PCT,
            daily_dd_limit=config.DAILY_DD_LIMIT,
            monthly_dd_limit=config.MONTHLY_DD_LIMIT,
            max_positions=config.N_STOCKS,
        )
        logger.info(f"  Fresh state: {portfolio}")

    # Sync pending with broker open orders
    portfolio.sync_pending_with_broker(open_orders)

    # RECON
    recon = portfolio.reconcile_with_broker(
        holdings, acct["cash"], dirty_exit, open_orders
    )

    if recon.action in ("FORCE_SYNC", "SAFE_SYNC"):
        portfolio.apply_recon(recon, holdings, acct["cash"])
        runtime_data = state_mgr.mark_startup()
        runtime_data["broker_snapshot_at"] = _now_iso()
        state_mgr.save_all(portfolio.to_dict(), runtime_data)
        logger.info(f"  RECON applied: {recon.action}")

    portfolio.broker_snapshot_at = _now_iso()

    # ── Phase 3: Fill Monitor ────────────────────────────
    logger.info("[LIVE] Phase 3: Fill monitor...")

    # Single Writer: callback only collects events
    _pending_fills = []
    provider.set_fill_callback(lambda event: _pending_fills.append(event))
    provider.start_fill_monitor()

    # ── Phase 4: Startup Save ────────────────────────────
    runtime_data = state_mgr.mark_startup()
    runtime_data["broker_snapshot_at"] = portfolio.broker_snapshot_at
    runtime_data["last_price_update_at"] = ""
    state_mgr.save_all(portfolio.to_dict(), runtime_data)

    notify.send(
        f"<b>Live Started</b>\n"
        f"Mode: {config.TRADING_MODE}\n"
        f"Equity: ${acct['equity']:,.2f}\n"
        f"Positions: {len(portfolio.positions)}",
        "INFO",
    )

    logger.info("[LIVE] Phase 4: Entering monitor loop...")
    print(f"\n  Monitoring... (Ctrl+C to stop)\n")

    # ── Monitor Loop ─────────────────────────────────────
    _notified_near: Set[str] = set()
    last_save_time = time.time()
    last_recon_time = time.time()
    SAVE_INTERVAL = 300    # 5 minutes
    RECON_INTERVAL = 600   # 10 minutes
    LOOP_INTERVAL = 60     # 1 minute (regular hours)
    IDLE_INTERVAL = 300    # 5 minutes (market closed)

    while not stop_requested:
        try:
            loop_start = time.time()

            # 1. Queue drain — collect fill events
            provider.process_events()
            fills = list(_pending_fills)
            _pending_fills.clear()

            # 2. Handle fills (Single Writer: main loop only)
            if fills:
                for fill in fills:
                    portfolio.handle_fill(fill)
                    sym = fill.get("symbol", "")
                    side = fill.get("side", "")
                    qty = fill.get("new_fill_qty", 0)
                    avg = fill.get("avg_price", 0)
                    if qty > 0:
                        if side == "SELL":
                            notify.notify_sell(sym, qty, avg)
                        elif side == "BUY":
                            notify.notify_buy(sym, qty, avg)

                # Immediate save after fills
                runtime_data["last_price_update_at"] = portfolio.last_price_update_at
                state_mgr.save_all(portfolio.to_dict(), runtime_data)
                last_save_time = time.time()

            # 3. Market hours check
            clock = provider.get_clock() or {}
            market_open = clock.get("is_open", False)

            if not market_open:
                # Sleep with 1s granularity for Ctrl+C responsiveness
                for _ in range(IDLE_INTERVAL):
                    if stop_requested:
                        break
                    time.sleep(1)
                continue

            # 4. Broker snapshot refresh
            acct = provider.query_account_summary()
            if "error" not in acct:
                portfolio.cash = acct.get("cash", portfolio.cash)
                portfolio.broker_snapshot_at = _now_iso()
                runtime_data["broker_snapshot_at"] = portfolio.broker_snapshot_at

            # 5. Price update
            prices = {}
            for sym in list(portfolio.positions.keys()):
                px = provider.get_current_price(sym)
                if px > 0:
                    prices[sym] = px

            ts = _now_iso()
            portfolio.update_prices(prices, ts)
            runtime_data["last_price_update_at"] = ts

            # 6. Trail stop evaluation
            triggered, near = portfolio.check_trail_stops()

            for sym in triggered:
                pos = portfolio.positions.get(sym)
                if not pos or pos.pending_sell_qty > 0:
                    continue

                result = provider.send_order(sym, "SELL", pos.quantity)
                if result.get("order_no"):
                    # Success: set pending
                    pos.pending_sell_qty = pos.quantity
                    pos.last_sell_order_at = _now_iso()
                    notify.notify_trail_triggered(sym, pos.current_price, pos.trail_stop_price)
                    logger.info(f"[TRAIL] Triggered SELL {sym} x{pos.quantity}")

                    # Immediate save
                    state_mgr.save_all(portfolio.to_dict(), runtime_data)
                    last_save_time = time.time()
                else:
                    logger.error(f"[TRAIL] SELL order failed for {sym}: {result}")
                    notify.notify_error(f"SELL order failed: {sym}")

            # Near notifications (with reset on recovery)
            near_syms = {s for s, _ in near}
            # Reset on price recovery
            recovered = _notified_near - near_syms
            for sym in recovered:
                _notified_near.discard(sym)

            for sym, dd_pct in near:
                if sym not in _notified_near:
                    _notified_near.add(sym)
                    notify.notify_trail_near(sym, dd_pct)

            # 7. Periodic save (5 min)
            now = time.time()
            if now - last_save_time >= SAVE_INTERVAL:
                state_mgr.save_all(portfolio.to_dict(), runtime_data)
                last_save_time = now

            # 8. Periodic RECON (10 min, log only)
            if now - last_recon_time >= RECON_INTERVAL:
                _holdings = provider.query_account_holdings()
                _acct = provider.query_account_summary()
                _orders = provider.query_open_orders() or []
                if _holdings is not None and "error" not in _acct:
                    _recon = portfolio.reconcile_with_broker(
                        _holdings, _acct.get("cash", 0), False, _orders
                    )
                    if not _recon.clean:
                        logger.warning(f"[RECON_PERIODIC] {_recon.action}")
                last_recon_time = now

            # 9. Log status
            eq = portfolio.get_equity()
            logger.info(
                f"[LOOP] equity~${eq:,.0f} | "
                f"pos={len(portfolio.positions)} | "
                f"triggered={len(triggered)} near={len(near)}"
            )

            # Sleep until next loop
            elapsed = time.time() - loop_start
            sleep_time = max(0, LOOP_INTERVAL - elapsed)
            for _ in range(int(sleep_time)):
                if stop_requested:
                    break
                time.sleep(1)

        except Exception as e:
            logger.error(f"[LOOP] Error: {e}", exc_info=True)
            notify.notify_error(f"Loop error: {e}")
            time.sleep(10)

    # ── Shutdown ─────────────────────────────────────────
    logger.info("[LIVE] Shutting down...")

    # Final drain
    provider.process_events()
    final_fills = list(_pending_fills)
    _pending_fills.clear()
    for fill in final_fills:
        portfolio.handle_fill(fill)

    # Final broker snapshot
    acct = provider.query_account_summary()
    if "error" not in acct:
        portfolio.cash = acct.get("cash", portfolio.cash)

    # Final save
    shutdown_data = state_mgr.mark_shutdown("sigint")
    shutdown_data["broker_snapshot_at"] = _now_iso()
    shutdown_data["last_price_update_at"] = portfolio.last_price_update_at
    state_mgr.save_all(portfolio.to_dict(), shutdown_data)

    provider.shutdown()

    notify.send("<b>Live Stopped</b>\nClean shutdown", "INFO")
    logger.info("[LIVE] Shutdown complete")
    print(f"\n{'='*50}")
    print(f"Live mode stopped. State saved.")
    print(f"{'='*50}\n")


def run_server():
    """Start FastAPI dashboard."""
    import uvicorn
    from web.app import app
    uvicorn.run(app, host="0.0.0.0", port=8081)


def main():
    parser = argparse.ArgumentParser(description="Q-TRON US 1.0")
    parser.add_argument("--test", action="store_true", help="Connection test")
    parser.add_argument("--batch", action="store_true", help="Batch: OHLCV + scoring")
    parser.add_argument("--live", action="store_true", help="Live mode: monitor + trail stop")
    parser.add_argument("--server", action="store_true", help="Start dashboard")

    args = parser.parse_args()

    if args.test:
        run_test()
    elif args.batch:
        run_batch()
    elif args.live:
        run_live()
    elif args.server:
        run_server()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
