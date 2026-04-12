"""
Batch mode entry point extracted from main.py.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path

from lifecycle.utils import is_weekday


def run_batch(config, fast: bool = False):
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
            live_list = set()
            for market in config.MARKETS:
                try:
                    market_list = set(get_stock_list(market, ohlcv_dir=ohlcv_dir))
                    live_list |= market_list
                    logger.info(f"  {market}: {len(market_list)} tickers")
                except Exception as e:
                    logger.warning(f"  {market} ticker list failed: {e}")
            codes = sorted(existing | live_list)
            new_count = len(live_list - existing)
            if new_count > 0:
                logger.info(f"  New listings detected: {new_count} stocks")
            if codes:
                updated = update_ohlcv_incremental(ohlcv_dir, codes, days=30)
                logger.info(f"  Updated {updated}/{len(codes)} stocks")
                # DB sync: CSV → DB for updated stocks
                try:
                    from data.db_provider import DbProvider
                    db = DbProvider()
                    db_synced = 0
                    for code in codes:
                        csv_path = ohlcv_dir / f"{code}.csv"
                        if csv_path.exists():
                            import pandas as _pd
                            _df = _pd.read_csv(csv_path, parse_dates=["date"])
                            # Only last 5 days (incremental)
                            _df = _df.tail(5)
                            if not _df.empty:
                                db.upsert_ohlcv(code, _df)
                                db_synced += 1
                    logger.info(f"  DB synced: {db_synced} stocks")
                except Exception as e2:
                    logger.warning(f"  DB sync failed: {e2} (non-critical)")
        except Exception as e:
            logger.warning(f"  pykrx update failed: {e}. Using existing data.")

    # Step 2: Build universe
    logger.info("[2/5] Building universe...")
    # Load sector map for market filter
    _sector_map_path = config.BASE_DIR / "data" / "sector_map.json"
    _sector_map_batch = {}
    if _sector_map_path.exists():
        try:
            _sector_map_batch = json.load(open(_sector_map_path, encoding="utf-8"))
        except Exception:
            pass
    _markets = getattr(config, "MARKETS", None)
    logger.info(f"  Market filter: {_markets or 'ALL'}")
    universe = build_universe_from_ohlcv(
        ohlcv_dir, min_close=config.UNIV_MIN_CLOSE,
        min_amount=config.UNIV_MIN_AMOUNT,
        min_history=config.UNIV_MIN_HISTORY,
        min_count=config.UNIV_MIN_COUNT,
        allowed_markets=_markets,
        sector_map=_sector_map_batch)
    logger.info(f"  Universe: {len(universe)} stocks")
    if not universe:
        logger.error("Empty universe!")
        return None

    # Step 3: Load OHLCV for scoring (DB first, CSV fallback)
    logger.info("[3/5] Loading OHLCV...")
    close_dict = {}
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        close_dict = db.load_close_dict(min_history=config.VOL_LOOKBACK)
        # Filter to universe
        close_dict = {k: v for k, v in close_dict.items() if k in universe}
        logger.info(f"  Loaded {len(close_dict)} stocks [DB]")
    except Exception as e:
        logger.warning(f"  DB load failed ({e}), CSV fallback")
        for code in universe:
            path = ohlcv_dir / f"{code}.csv"
            if path.exists():
                df = pd.read_csv(path, parse_dates=["date"])
                df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
                if len(df) >= config.VOL_LOOKBACK:
                    close_dict[code] = df.set_index("date")["close"]
        logger.info(f"  Loaded {len(close_dict)} stocks [CSV]")

    # Step 4: Score and select
    logger.info("[4/5] Scoring and selecting...")
    target = build_target_portfolio(close_dict, config)
    path = save_target_portfolio(target, config.SIGNALS_DIR)
    logger.info(f"  Target: {len(target['target_tickers'])} stocks -> {path}")

    # DB 저장 (PostgreSQL)
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        db.save_target_portfolio(target)
        logger.info(f"  Target saved to DB")
    except Exception as e:
        logger.warning(f"  DB save failed: {e} (non-critical)")
    for i, tk in enumerate(target["target_tickers"], 1):
        s = target["scores"].get(tk, {})
        logger.info(f"    {i:2d}. {tk}  vol={s.get('vol_12m',0):.4f}  mom={s.get('mom_12_1',0):.4f}")

    # Step 5 (fast): Fundamental snapshot (lightweight, Lab 9전략 필수)
    if fast:
        logger.info("[5/5] Fundamental snapshot (fast, for Lab strategies)...")
        try:
            fund_dir = config.OHLCV_DIR.parent / "fundamental"
            fund_dir.mkdir(parents=True, exist_ok=True)
            fund_date = target.get("date", datetime.now().strftime("%Y%m%d"))
            fund_path = fund_dir / f"fundamental_{fund_date}.csv"

            if fund_path.exists():
                logger.info(f"  Already exists: {fund_path}")
            else:
                from data.fundamental_collector import fetch_daily_snapshot
                fund_df = fetch_daily_snapshot()
                if fund_df is not None:
                    fund_df.to_csv(fund_path, index=False)
                    logger.info(f"  Fundamental: {fund_path} ({len(fund_df)} stocks)")
                    # DB 저장
                    try:
                        from data.db_provider import DbProvider
                        db = DbProvider()
                        db.upsert_fundamental(fund_date, fund_df)
                        logger.info(f"  Fundamental saved to DB")
                    except Exception as e2:
                        logger.warning(f"  Fundamental DB save failed: {e2}")
        except Exception as e:
            logger.warning(f"  Fundamental failed: {e} (Lab uses latest available)")

        # Lab Live daily run (9전략 forward paper trading)
        try:
            _run_lab_live_daily(config, logger)
        except Exception as e:
            logger.warning(f"  Lab Live failed: {e} (non-critical)")

        # Advisor daily analysis + Telegram
        try:
            _run_advisor(config, logger)
        except Exception as e:
            logger.warning(f"  Advisor failed: {e} (non-critical)")

        logger.info("Batch complete (fast).")
        return target
    logger.info("[5/7] Generating Top20 MA report...")
    try:
        from report.top20_report import generate_top20_report
        html_path = generate_top20_report(target, ohlcv_dir, config.REPORT_DIR)
        if html_path:
            logger.info(f"  Report: {html_path}")
    except Exception as e:
        logger.warning(f"  Report generation failed: {e} (non-critical)")

    # Step 6: Collect daily fundamental snapshot (for backtest DB + Valuation report)
    # Skips if today's file already exists (avoid 11min re-crawl)
    logger.info("[6/7] Collecting fundamental snapshot...")
    try:
        fund_dir = config.OHLCV_DIR.parent / "fundamental"
        fund_dir.mkdir(parents=True, exist_ok=True)
        fund_date = target.get("date", datetime.now().strftime("%Y%m%d"))
        fund_path = fund_dir / f"fundamental_{fund_date}.csv"

        if fund_path.exists():
            logger.info(f"  Already exists: {fund_path} - skipping")
        else:
            from data.fundamental_collector import fetch_daily_snapshot
            fund_df = fetch_daily_snapshot()
            if fund_df is not None:
                fund_df.to_csv(fund_path, index=False)
                logger.info(f"  Fundamental snapshot: {fund_path} ({len(fund_df)} stocks)")
    except Exception as e:
        logger.warning(f"  Fundamental collection failed: {e} (non-critical)")

    # Step 7: Generate Valuation Top20 report (reuses Step 6 CSV)
    logger.info("[7/7] Generating Valuation Top20 report...")
    try:
        from report.top20_valuation import generate_top20_valuation_report
        # Load sector map for sector PER comparison
        sector_map_dict = {}
        if config.SECTOR_MAP.exists():
            import json as _json
            sector_map_dict = _json.loads(config.SECTOR_MAP.read_text(encoding="utf-8"))

        val_date = target.get("date", datetime.now().strftime("%Y%m%d"))
        val_path = generate_top20_valuation_report(
            ohlcv_dir=ohlcv_dir,
            output_dir=config.REPORT_DIR,
            universe=list(close_dict.keys()),  # full universe, not just top20
            sector_map=sector_map_dict,
            report_date=val_date,
        )
        if val_path:
            logger.info(f"  Valuation Report: {val_path}")
    except Exception as e:
        logger.warning(f"  Valuation report failed: {e} (non-critical)")

    # Step 8: Lab Live daily run (9전략 forward paper trading)
    logger.info("[8/9] Lab Live daily run...")
    try:
        _run_lab_live_daily(config, logger)
    except Exception as e:
        logger.warning(f"  Lab Live failed: {e} (non-critical)")

    # Step 9: Advisor daily analysis + Telegram
    logger.info("[9/9] Advisor daily analysis...")
    try:
        _run_advisor(config, logger)
    except Exception as e:
        logger.warning(f"  Advisor failed: {e} (non-critical)")

    logger.info("Batch complete.")
    return target


def _run_lab_live_daily(config, logger):
    """Lab Live 9전략 forward paper trading daily run."""
    try:
        from web.lab_live.engine import LabLiveSimulator
        sim = LabLiveSimulator()
        sim.initialize()
        result = sim.run_daily()
        if result.get("ok"):
            logger.info(f"  Lab Live: {result['date']}, {result['trades']} trades, "
                        f"{result['elapsed']:.1f}s")
        elif result.get("skipped"):
            logger.info(f"  Lab Live: already ran for {result['date']}")
        else:
            logger.warning(f"  Lab Live: {result}")
    except Exception as e:
        logger.warning(f"  Lab Live error: {e}")


def _run_advisor(config, logger):
    """Advisor 일일 분석 + 텔레그램 알림."""
    try:
        from advisor.runner import run_analysis
        from notify.helpers import _notify_advisor
        from datetime import datetime

        today = datetime.now().strftime("%Y%m%d")
        mode = getattr(config, "TRADING_MODE", "live")

        result = run_analysis(today, mode)
        status = result.get("status", "UNKNOWN")

        # Summary log
        alerts = result.get("alerts", [])
        recs = result.get("recommendations", [])
        n_high = sum(1 for a in alerts if a.get("priority") == "HIGH")
        logger.info(f"  Advisor: {status}, {len(alerts)} alerts ({n_high} HIGH), "
                     f"{len(recs)} recommendations, {result.get('elapsed_sec', 0):.1f}s")

        # Telegram
        _notify_advisor(alerts, recs)

    except Exception as e:
        logger.warning(f"  Advisor error: {e}")
