"""
Batch mode entry point extracted from main.py.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path

from lifecycle.utils import is_weekday

try:
    from notify.helpers import alert_data_failure as _alert_data
except Exception:
    def _alert_data(*a, **kw): pass  # notify 미초기화 시 no-op


def _load_checkpoint(config) -> dict:
    """배치 진행 체크포인트 로드. 중단 후 재시작 시 완료 단계 skip."""
    cp_path = Path(config.OHLCV_DIR).parent / "batch_checkpoint.json"
    if cp_path.exists():
        try:
            cp = json.loads(cp_path.read_text(encoding="utf-8"))
            if cp.get("date") == datetime.now().strftime("%Y-%m-%d"):
                return cp
        except Exception:
            pass
    return {"date": datetime.now().strftime("%Y-%m-%d"), "completed_steps": []}


def _save_checkpoint(config, step: str, cp: dict) -> None:
    """완료 단계를 체크포인트에 기록."""
    cp_path = Path(config.OHLCV_DIR).parent / "batch_checkpoint.json"
    if step not in cp.get("completed_steps", []):
        cp.setdefault("completed_steps", []).append(step)
    cp["last_step"] = step
    cp["last_ts"] = datetime.now().isoformat(timespec="seconds")
    try:
        cp_path.write_text(json.dumps(cp, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def run_batch(config, fast: bool = False):
    """Batch: pykrx update → universe → scoring → target portfolio.

    중단 후 재시작 시 완료 단계는 skip (batch_checkpoint.json 기반).
    """
    from data.pykrx_provider import update_ohlcv_incremental, get_stock_list
    from data.universe_builder import build_universe_from_ohlcv
    from strategy.factor_ranker import build_target_portfolio, save_target_portfolio
    import pandas as pd

    logger = logging.getLogger("gen4.batch")
    logger.info("=" * 60)
    logger.info("  Gen4 Batch Mode")
    logger.info("=" * 60)

    # 체크포인트 로드 (동일 날짜 재실행 시 완료 단계 skip)
    _cp = _load_checkpoint(config)
    _done = set(_cp.get("completed_steps", []))
    if _done:
        logger.info(f"  [RESUME] Skipping completed steps: {sorted(_done)}")

    ohlcv_dir = config.OHLCV_DIR

    # Step 1: pykrx OHLCV update (existing + new listings)
    if "step1_ohlcv" in _done:
        logger.info("[1/5] OHLCV update — SKIP (checkpoint)")
    elif not is_weekday():
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

        # KOSPI index 업데이트 (DB + CSV)
        _update_kospi_index(config, logger)

        _save_checkpoint(config, "step1_ohlcv", _cp)

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
        _notify_batch_error("Empty universe — batch 중단", logger)
        return None

    # Step 3: Load OHLCV for scoring (DB only — CSV fallback 금지)
    logger.info("[3/5] Loading OHLCV...")
    close_dict = {}
    selected_source = "DB"
    from data.db_provider import DbProvider
    db = DbProvider()
    close_dict = db.load_close_dict(min_history=config.VOL_LOOKBACK)
    # Filter to universe
    close_dict = {k: v for k, v in close_dict.items() if k in universe}
    logger.info(f"  Loaded {len(close_dict)} stocks [DB]")
    # PG 실패 시 pg_base retry 3회 후 raise → batch 중단 (올바른 동작)

    # Step 4: Score and select
    logger.info("[4/5] Scoring and selecting...")
    target = build_target_portfolio(close_dict, config)

    # KR-P0-004: persist snapshot_version so downstream (lab_live, rebalance API)
    # can detect stale/duplicated batches by comparing the same key format.
    #   {trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash}
    try:
        import hashlib as _hl
        _data_last_dates = [s.index.max() for s in close_dict.values()
                            if hasattr(s, 'index') and len(s) > 0]
        if _data_last_dates:
            _dl = max(_data_last_dates)
            _dl_str = _dl.strftime("%Y-%m-%d") if hasattr(_dl, 'strftime') else str(_dl)[:10]
        else:
            _dl_str = "?"
        # matrix_hash: deterministic fingerprint of loaded close-series (code → last10 values)
        _h = _hl.sha1()
        for _k in sorted(close_dict.keys()):
            _s = close_dict[_k]
            try:
                _tail = list(_s.tail(10).values)
                _h.update(f"{_k}:{_tail}".encode("utf-8"))
            except Exception:
                _h.update(f"{_k}:?".encode("utf-8"))
        _matrix_hash = _h.hexdigest()[:12]
        _snap_ver = (
            f"{target.get('date', '')}:{selected_source}:{_dl_str}"
            f":{len(close_dict)}:{_matrix_hash}"
        )
        target["snapshot_version"] = _snap_ver
        target["selected_source"] = selected_source
        target["data_last_date"] = _dl_str
        target["universe_count"] = len(close_dict)
        target["matrix_hash"] = _matrix_hash
        logger.info(f"[BATCH_SNAPSHOT_VERSION] {_snap_ver}")
        # P1-5: data freshness gate — warn if data_last_date lags trade_date
        try:
            from datetime import date as _date, timedelta as _td
            _tdate = target.get("date", "")
            if _tdate and _dl_str and _dl_str != "?":
                _td_dt = _date.fromisoformat(_tdate[:10])
                _dl_dt = _date.fromisoformat(_dl_str[:10])
                _lag_days = (_td_dt - _dl_dt).days
                if _lag_days > 4:
                    logger.critical(
                        f"[BATCH_DATA_STALE] market=KR data_last={_dl_dt} "
                        f"trade_date={_td_dt} lag={_lag_days}d > 4d — "
                        f"review OHLCV sync before next rebalance")
        except Exception as _e:
            logger.warning(f"[BATCH_DATA_STALE_CHECK_FAIL] {_e}")
    except Exception as _e:
        logger.warning(f"[BATCH_SNAPSHOT_VERSION_FAIL] {_e} — target saved without snapshot_version")

    path = save_target_portfolio(target, config.SIGNALS_DIR)
    logger.info(f"  Target: {len(target['target_tickers'])} stocks -> {path}")

    # DB 저장 (PostgreSQL) — AUDIT ONLY: rebalance는 JSON만 읽음
    # canonical = signals/target_portfolio_{date}.json
    # PG target_portfolio 테이블은 이력 조회/감사용으로만 사용
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        db.save_target_portfolio(target)
        logger.info(f"  Target saved to DB (audit)")
    except Exception as e:
        logger.warning(f"  DB audit save failed: {e} (non-critical)")
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
                        _alert_data("fundamental_db", str(e2), {"fund_date": fund_date})
                else:
                    logger.warning("  Fundamental: fetch_daily_snapshot returned None")
                    _alert_data("fundamental", "fetch_daily_snapshot returned None",
                                {"expected_date": fund_date})
        except Exception as e:
            logger.warning(f"  Fundamental failed: {e} (Lab uses latest available)")
            _alert_data("fundamental", f"fetch exception: {e}",
                        {"expected_date": fund_date})

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
        _notify_batch_result(target, logger, mode="fast")
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

    # Note: AUTO GATE advisory observation (gate_observer.run_today) is triggered
    # by kr/tray_server.py post-EOD to keep a single-producer contract.
    # Do not run it here — the tray_server is the sole producer.

    logger.info("Batch complete.")
    _notify_batch_result(target, logger, mode="full")
    return target


def _update_kospi_index(config, logger):
    """KOSPI index 파일 + DB 업데이트 (yfinance fallback).

    pykrx get_index_ohlcv_by_date가 빈 결과를 반환하는 경우 yfinance(^KS11) fallback.
    KOSPI.csv와 kospi_index DB 테이블 모두 업데이트.
    """
    import pandas as pd
    from datetime import datetime, timedelta

    index_file = config.INDEX_FILE
    try:
        existing = pd.read_csv(index_file, parse_dates=["index"])
        date_col = "index"
        existing = existing.rename(columns={date_col: "date"})
        existing["date"] = pd.to_datetime(existing["date"])
        last_date = existing["date"].max()
    except Exception:
        last_date = None

    today = datetime.now()
    if today.hour < 16:
        today -= timedelta(days=1)
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")

    if last_date is not None and str(last_date.date()) >= today_str:
        logger.info(f"  KOSPI index up-to-date ({today_str})")
        return

    # Fetch missing days via yfinance
    from_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d") if last_date else "2019-01-01"
    to_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    new_df = None
    try:
        import yfinance as yf
        raw = yf.download("^KS11", start=from_date, end=to_date,
                          auto_adjust=True, progress=False)
        if not raw.empty:
            # Flatten MultiIndex columns if present
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw = raw.reset_index()
            raw = raw.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                      "Low": "low", "Close": "close", "Volume": "volume"})
            raw["date"] = pd.to_datetime(raw["date"])
            new_df = raw[["date", "open", "high", "low", "close", "volume"]]
            logger.info(f"  KOSPI index: +{len(new_df)} rows via yfinance")
    except Exception as e:
        logger.warning(f"  KOSPI index yfinance failed: {e}")
        _alert_data("yfinance", str(e), {"from_date": from_date})

    if new_df is None or new_df.empty:
        logger.warning("  KOSPI index: no new data fetched")
        _alert_data("KOSPI_index", "no new data from yfinance",
                    {"last_date": str(last_date), "today": today_str})
        return

    # Update KOSPI.csv
    try:
        existing_df = pd.read_csv(index_file)
        date_col = "index" if "index" in existing_df.columns else "date"
        existing_df = existing_df.rename(columns={date_col: "date"})
        existing_df["date"] = pd.to_datetime(existing_df["date"])
        combined = pd.concat([existing_df, new_df]).drop_duplicates("date").sort_values("date")
        combined = combined.rename(columns={"date": "index", "open": "Open", "high": "High",
                                            "low": "Low", "close": "Close", "volume": "Volume"})
        combined.to_csv(index_file, index=False)
        logger.info(f"  KOSPI.csv updated: {len(combined)} rows total")
    except Exception as e:
        logger.warning(f"  KOSPI.csv update failed: {e}")

    # Update DB kospi_index table
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        upserted = db.upsert_kospi_index(new_df)
        logger.info(f"  kospi_index DB upserted: {upserted} rows")
    except Exception as e:
        logger.warning(f"  kospi_index DB upsert failed: {e} (non-critical)")
        _alert_data("kospi_index_db", str(e))


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


def _notify_batch_result(target: dict, logger, mode: str = "full") -> None:
    """Batch 완료 텔레그램 알림."""
    try:
        from notify.telegram_bot import send
        tickers = target.get("target_tickers", [])
        date = target.get("date", "?")
        send(
            f"✅ <b>KR Batch Complete</b> ({mode})\n"
            f"Date: {date}\n"
            f"Target: {len(tickers)}종목",
            severity="INFO",
        )
    except Exception:
        pass


def _notify_batch_error(reason: str, logger) -> None:
    """Batch 에러 텔레그램 알림."""
    try:
        from notify.telegram_bot import send
        from datetime import datetime as _dt
        send(
            f"🚨 <b>KR Batch Error</b>\n"
            f"시간: {_dt.now().strftime('%H:%M:%S')}\n"
            f"사유: {reason}",
            severity="CRITICAL",
        )
    except Exception:
        pass
