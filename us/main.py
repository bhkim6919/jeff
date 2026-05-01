# -*- coding: utf-8 -*-
"""
Q-TRON US 1.0 — Main Entry Point
==================================
Usage:
    cd us
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

# sys.path bootstrap — us/ + project root (single source: us/_bootstrap_path.py)
sys.path.insert(0, str(Path(__file__).resolve().parent))  # audit:allow-syspath: bootstrap-locator
import _bootstrap_path  # noqa: F401  -- side-effect: sys.path setup

_LOG_DIR = Path(__file__).resolve().parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOG_DIR / f"us_app_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),                              # stdout
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),     # us/logs/us_app_YYYYMMDD.log
    ],
)
logger = logging.getLogger("qtron.us.main")
logger.info(f"[BOOT] log file: {_LOG_FILE}")


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

    # US-P0-003: OHLCV quality gate — halt if failed_ratio > 10%
    _failed_ratio = getattr(collector, "last_failed_ratio", 0.0)
    _last_errors = getattr(collector, "last_errors", 0)
    _last_total = getattr(collector, "last_total", len(sp500))
    if _failed_ratio > 0.10:
        msg = (
            f"[US_BATCH_HALT_DATA_QUALITY] failed_ratio={_failed_ratio:.2%} "
            f"({_last_errors}/{_last_total}) > 10% — batch aborted. "
            f"State NOT updated. Retry after network/yfinance recovery.")
        print(msg)
        logger = logging.getLogger("qtron.us.main")
        logger.critical(msg)
        try:
            from notify.telegram_bot import send as _tg_send
            _tg_send(msg, severity="CRITICAL")
        except Exception:
            pass
        return

    # DB freshness gate — expected last_date >= business_date - 1 trading day
    try:
        from data.alpaca_provider import AlpacaProvider
        _prov_fresh = AlpacaProvider(config)
        from core.state_manager import get_current_trading_day
        _bd_today = get_current_trading_day(_prov_fresh)
    except Exception:
        from core.state_manager import get_current_trading_day
        _bd_today = get_current_trading_day()
    try:
        _db_last = db.get_ohlcv_last_date()
    except AttributeError:
        _db_last = None
    except Exception as _e:
        logging.getLogger("qtron.us.main").warning(
            f"[US_BATCH_FRESHNESS_QUERY_FAIL] {_e}")
        _db_last = None
    if _db_last is not None:
        try:
            from datetime import date as _date, timedelta as _td
            _bd_dt = _date.fromisoformat(_bd_today)
            # 직전 1영업일 허용 (주말/휴일 대응: 오늘 업데이트 전 상태)
            _cutoff = _bd_dt - _td(days=4)  # 보수적으로 4일 이내면 OK
            _db_last_dt = (_db_last if isinstance(_db_last, _date)
                           else _date.fromisoformat(str(_db_last)[:10]))
            if _db_last_dt < _cutoff:
                # P1-5: also emit canonical [BATCH_DATA_STALE] tag for alerting
                logging.getLogger("qtron.us.main").critical(
                    f"[BATCH_DATA_STALE] market=US db_last={_db_last_dt} "
                    f"cutoff={_cutoff} bd={_bd_today}")
                msg = (
                    f"[US_BATCH_HALT_DATA_STALE] db_last={_db_last_dt} "
                    f"< cutoff={_cutoff} (business_date={_bd_today}). "
                    f"OHLCV too stale — batch aborted.")
                print(msg)
                logging.getLogger("qtron.us.main").critical(msg)
                try:
                    from notify.telegram_bot import send as _tg_send
                    _tg_send(msg, severity="CRITICAL")
                except Exception:
                    pass
                return
            print(f"  [US_BATCH_OHLCV_FRESHNESS_OK] db_last={_db_last_dt}, bd={_bd_today}")
        except Exception as _e:
            logging.getLogger("qtron.us.main").warning(
                f"[US_BATCH_FRESHNESS_PARSE_FAIL] {_e}")

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

    # ── snapshot_version 저장 (Step 7 앞으로 이동 — 2026-04-29 fix) ──
    # US-P0-001/002 + batch 폭주 방지: research ingestion (Step 7) 이 600s
    # subprocess timeout 을 초과하면 main.py 가 외부에서 kill 되어 snapshot
    # 저장이 안 됨. 그러면 runtime_state.last_batch_business_date 가 어제 날짜로
    # 남고, tray 의 auto-batch 가 매 30s tick 마다 재트리거 → 매 ~10분마다
    # batch 재실행 (28 alerts/24h 관찰됨). 저장을 research 앞으로 옮기면
    # research 실패/timeout 무관하게 batch 메타데이터는 정상 commit.
    try:
        from core.state_manager import (
            get_last_closed_trading_day, get_current_trading_day,
            is_post_market_close, US_ET,
        )
        from zoneinfo import ZoneInfo

        try:
            from data.alpaca_provider import AlpacaProvider
            prov = AlpacaProvider(config)
        except Exception:
            prov = None

        et_now = datetime.now(ZoneInfo("US/Eastern"))
        post_close = is_post_market_close(et_now)

        if post_close:
            # 정규 post-close batch — 오늘 거래일로 기록
            today_bd = get_current_trading_day(prov) if prov else get_current_trading_day()
            bd_marker = "POST_CLOSE"
        else:
            # pre-market / 장중 수동 실행 — 직전 종가일로 기록
            today_bd = get_last_closed_trading_day(prov) if prov else get_last_closed_trading_day()
            bd_marker = "PRE_MARKET"

        sv = f"{today_bd}_batch_{int(et_now.timestamp())}_{bd_marker}"

        # State write — route through StateManagerUS so the
        # transition is atomic + the rebal-completion fields written
        # by ``us/web/app.py`` (last_execute_*, last_rebalance_date,
        # next_rebalance_date) are preserved across batch ticks.
        #
        # Pre-fix history (Jeff 2026-04-30): the original code did a
        # direct ``json.load`` → mutate → ``json.dump`` cycle. That
        # bypassed ``state_manager.save_all``'s ``_REBAL_DEFAULTS``
        # merge entirely, so any inter-process race (batch run after
        # rebal completion) clobbered the rebal-completion record.
        # Live evidence: 04-30 batch ran post-close, then the
        # dashboard regressed from EXECUTED → BATCH_DONE / Last:--
        # / Execute: Ready, even though tray-side PR #36 was active.
        #
        # ``transition_phase_with_updates`` only writes the keys we
        # explicitly pass plus the new phase, so non-batch state
        # (last_execute_*, dd_label, regime_*, etc.) survives.
        from core.state_manager import StateManagerUS
        sm = StateManagerUS(config.STATE_DIR, config.TRADING_MODE)
        batch_updates = {
            "snapshot_version":           sv,
            "snapshot_created_at":        et_now.isoformat(),
            "last_batch_business_date":   today_bd,
            "last_batch_post_close":      post_close,  # P0-001 marker
            # batch_fresh: compute_batch_fresh re-validates post-close,
            # this is the cached marker for restart-time normalization.
            "batch_fresh":                bool(post_close),
            # New batch — reset per-snapshot attempt tracking.
            "last_rebal_attempt_snapshot": "",
            "last_rebal_attempt_at":       "",
            "last_rebal_attempt_result":   "",
            "last_rebal_attempt_count":    0,
            "last_rebal_attempt_reason":   "",
        }
        # Walk the phase machine to BATCH_DONE. The starting phase
        # may be IDLE (cold), EXECUTED / PARTIAL_EXECUTED / FAILED
        # (after a rebal day), or BATCH_DONE (re-run on the same
        # day) — we step through whatever intermediates are needed.
        # Each ``transition_phase_with_updates`` call is atomic under
        # the state manager lock, so the rebal-completion fields
        # (last_execute_*, last_rebalance_date, next_rebalance_date)
        # are preserved by the load-merge-write contract.
        current_phase = (sm.get_rebal_state() or {}).get("rebal_phase", "IDLE")
        # Map current → ordered intermediates needed before BATCH_RUNNING.
        _to_running_path = {
            "IDLE":              ["BATCH_RUNNING"],
            "BATCH_RUNNING":     [],                  # already there
            "BATCH_DONE":        ["IDLE", "BATCH_RUNNING"],
            "DUE":               [],                  # invalid — fall through
            "EXECUTING":         [],                  # invalid — fall through
            "EXECUTED":          ["BATCH_RUNNING"],
            "PARTIAL_EXECUTED":  ["BATCH_RUNNING"],
            "FAILED":            ["BATCH_RUNNING"],
            "BLOCKED":           ["IDLE", "BATCH_RUNNING"],
        }
        steps = _to_running_path.get(current_phase, [])
        for intermediate in steps:
            ok, reason = sm.transition_phase_with_updates(intermediate, {})
            if not ok:
                print(f"  [US_BATCH_PHASE_REJECT] {current_phase} → "
                      f"{intermediate}: {reason}")
        # Final transition to BATCH_DONE with the full update payload.
        ok, reason = sm.transition_phase_with_updates("BATCH_DONE", batch_updates)
        if not ok:
            print(f"  [US_BATCH_PHASE_REJECT] → BATCH_DONE: {reason}")

        print(f"  [US_BATCH_OK] snapshot={sv} post_close={post_close}")
        # P1-5: explicit marker for post-close confirmed batches (UI badge gate)
        if post_close:
            print(f"  [BATCH_POST_CLOSE_CONFIRMED] bd={today_bd} "
                  f"created_et={et_now.isoformat()} snapshot={sv}")
    except Exception as e:
        print(f"  [US_BATCH_SNAPSHOT_FAIL] {e}")

    # Step 7: Research universe OHLCV ingestion (Lab/Forward EOD 의존)
    # Invariant: Strategy uses Universe U → Data(U) must exist.
    # 운영(ohlcv_us)과 분리된 ohlcv_us_research 테이블에 R1000 + R3000 small-cap ingestion.
    # 실패해도 operating batch는 중단하지 않음 (research는 lab-only) — 대신 loud log + Telegram.
    import os as _os
    _skip_research = _os.environ.get("US_BATCH_SKIP_RESEARCH", "").lower() in ("1", "true", "yes")
    if _skip_research:
        print("\n[7] Research ingestion SKIPPED (US_BATCH_SKIP_RESEARCH=1)")
    else:
        print("\n[7] Research universe OHLCV ingestion (R1000 + R3000)...")
        try:
            from data.universe_builder import load_universe_snapshot

            r1000 = load_universe_snapshot("RESEARCH_R1000")
            r3000 = load_universe_snapshot("RESEARCH_R3000")
            if not r1000:
                raise RuntimeError("RESEARCH_R1000 snapshot empty or missing")

            r3000_only = sorted(set(r3000) - set(r1000)) if r3000 else []
            print(f"  Universes: R1000={len(r1000)}, R3000={len(r3000)}, "
                  f"R3000-only={len(r3000_only)}")

            # Period 결정: tag별로 stale + coverage 평가 (R1000/R3000 독립).
            # last_date만 보면 일부만 fresh인 상황을 못 잡음 → coverage<80% 면 강제 2y.
            from datetime import date as _date

            def _research_symbol_count(tag: str) -> int:
                try:
                    conn = db._conn()
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT COUNT(DISTINCT symbol) FROM ohlcv_us_research WHERE universe_tag=%s",
                        (tag,),
                    )
                    n = cur.fetchone()[0] or 0
                    cur.close()
                    conn.close()
                    return int(n)
                except Exception:
                    return 0

            def _decide_period(tag: str, expected: int) -> tuple:
                got = _research_symbol_count(tag)
                coverage = got / max(expected, 1)
                last = db.get_ohlcv_research_last_date(tag)
                # Coverage 부족 (신규 universe 또는 부분 ingest 상태) → 강제 backfill
                if expected > 0 and coverage < 0.80:
                    return "2y", f"low coverage ({tag}, {got}/{expected}={coverage:.0%}) → backfill"
                if last is None:
                    return "2y", f"initial backfill ({tag})"
                days = (_date.today() - last).days
                if days > 30:
                    return "2y", f"full backfill ({tag}, {days}d stale)"
                if days > 5:
                    return "1mo", f"mid incremental ({tag}, {days}d stale, cov={coverage:.0%})"
                return "5d", f"daily incremental ({tag}, last={last}, cov={coverage:.0%})"

            period_r1000, msg_r1000 = _decide_period("R1000", len(r1000))
            period_r3000, msg_r3000 = _decide_period("R3000", len(r3000_only))
            print(f"  [RESEARCH_BATCH] R1000: {msg_r1000} → period={period_r1000}")
            print(f"  [RESEARCH_BATCH] R3000: {msg_r3000} → period={period_r3000}")

            n_r1000 = collector.collect_ohlcv_research(
                r1000, period=period_r1000, universe_tag="R1000"
            )
            n_r3000 = collector.collect_ohlcv_research(
                r3000_only, period=period_r3000, universe_tag="R3000"
            )
            total_n = n_r1000 + n_r3000
            print(f"  [RESEARCH_BATCH] done: R1000={n_r1000}, R3000={n_r3000}, "
                  f"total={total_n}/{len(r1000) + len(r3000_only)}")
            # Use logging.getLogger() inline — run_batch() has a conditional
            # `logger = logging.getLogger(...)` at line 148 that makes `logger`
            # a local name, so referencing it here when that branch is not
            # taken raises UnboundLocalError (observed 2026-04-21 batch run —
            # Step 7 completed but the whole batch crashed afterward, leaving
            # snapshot_version unwritten).
            logging.getLogger("qtron.us.main").info(
                f"[BATCH_R3000] r1000={n_r1000}/{len(r1000)} (period={period_r1000}) "
                f"r3000={n_r3000}/{len(r3000_only)} (period={period_r3000})"
            )
        except Exception as e:
            msg = f"[BATCH_RESEARCH_FAIL] {e}"
            print(f"  ✗ {msg}")
            logging.getLogger("qtron.us.main").error(msg, exc_info=True)
            try:
                from notify.telegram_bot import send as _tg_send
                _tg_send(f"US batch research ingestion failed: {e}",
                         severity="WARNING")
            except Exception:
                pass

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

    # ── Phase 1.5: Cancel stale open orders ─────────────
    _buy_blocked_startup = False
    if open_orders:
        logger.warning(f"[STARTUP_CANCEL] {len(open_orders)} open orders found — cancelling all...")
        n = provider.cancel_all_open_orders()
        if n is None:
            logger.critical("[STARTUP_CANCEL_FAIL] cancel_all returned None")
            _buy_blocked_startup = True
            try:
                from notify import alert_dedup as _ad, telegram_bot as _tg
                if _ad.startup_block_should_fire():
                    _tg.send(
                        "<b>STARTUP_BLOCKED</b> cancel_all returned None "
                        "— BUY blocked, SELL/trail allowed",
                        "WARN",
                    )
            except Exception:
                pass
        else:
            logger.info(f"[STARTUP_CANCEL] Cancelled {n} orders — waiting 1s...")
            time.sleep(1)
            open_orders = provider.query_open_orders() or []
            if open_orders:
                logger.critical(f"[STARTUP_CANCEL_INCOMPLETE] {len(open_orders)} orders remain"
                                " — BUY/rebalance blocked, SELL/trail allowed")
                _buy_blocked_startup = True
                for o in open_orders:
                    logger.critical(f"  [REMAINING] {o['side']} {o['code']} x{o['qty']}")
                # Notify operator: BUY blocked at startup, SELL/trail still allowed.
                try:
                    from notify import alert_dedup as _ad, telegram_bot as _tg
                    if _ad.startup_block_should_fire():
                        _tg.send(
                            f"<b>STARTUP_BLOCKED</b> {len(open_orders)} stale order(s) "
                            f"not cleared — BUY blocked, SELL/trail allowed",
                            "WARN",
                        )
                except Exception:
                    pass
            else:
                logger.info("[STARTUP_CANCEL_OK] All open orders cleared")
    else:
        logger.info("[STARTUP_CANCEL] No open orders — clean startup")

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
        # P2-RECON-2 fix (2026-04-17): 스타트업 RECON 결과를 runtime에 기록.
        # Gate는 rt.get("last_recon_ok", True)로 읽으므로 명시적으로 False 기록 필수.
        runtime_data["last_recon_ok"] = recon.clean  # FORCE/SAFE_SYNC = not clean
        runtime_data["state_uncertain"] = recon.state_uncertain and not recon.clean
        runtime_data["last_recon_at"] = _now_iso()
        state_mgr.save_all(portfolio.to_dict(), runtime_data)
        logger.info(f"  RECON applied: {recon.action} last_recon_ok={recon.clean}")
    else:
        # NONE / LOG_ONLY / LOG_WARNING — also persist recon result
        # P2-RECON-2 (continued): clean RECON도 명시 기록 (default True 의존 제거)
        pass  # Will be written at Phase 4 Startup Save below

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
    # P2-RECON-2 (2026-04-17): 스타트업 RECON 결과 항상 기록 (NONE 포함).
    # FORCE_SYNC/SAFE_SYNC 분기에서 이미 saved → 여기서는 mark_startup 덮어쓰기 방지
    # 위 분기에서 runtime_data["last_recon_ok"] 설정됐으므로 재설정하지 않음.
    # NONE/LOG_ONLY 경우에만 여기서 설정.
    if "last_recon_ok" not in runtime_data:
        runtime_data["last_recon_ok"] = recon.clean
        # dirty_exit으로 인해 state_uncertain=True가 세팅되더라도,
        # RECON이 clean이면 상태가 검증됐으므로 즉시 해제
        runtime_data["state_uncertain"] = recon.state_uncertain and not recon.clean
        runtime_data["last_recon_at"] = _now_iso()

    # P0 fix: DD tracking 초기화 (fail-closed: 미초기화 시 buy 차단)
    portfolio.init_dd_tracking()

    # P0 fix: stale execute_lock 정리 (이전 크래시 잔여 lock 해제)
    if state_mgr.clear_stale_execute_lock():
        logger.info("[STARTUP] Stale execute_lock cleared from previous crash")

    # P1-3: pre-market batch_fresh 정규화 (snapshot_created_at < today 16:00 ET → False)
    if state_mgr.normalize_batch_state_at_startup(provider):
        logger.warning("[STARTUP] batch_fresh normalized (pre-market snapshot detected)")

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

            # 5.1 Observability: STALE summary alert (one msg, not per-symbol)
            #     Fires when N positions have last_price_at older than 6h.
            #     Throttle: 1h between bursts, recovery always fires once.
            if market_open:
                try:
                    from notify import alert_dedup as _ad, telegram_bot as _tg
                    _stale_n, _stale_syms = _ad.count_stale_positions(
                        portfolio.positions, ts)
                    if _ad.stale_should_fire(_stale_n):
                        if _stale_n > 0:
                            _shown = ", ".join(_stale_syms[:20])
                            _more = f" (+{_stale_n - 20} more)" if _stale_n > 20 else ""
                            _tg.send(
                                f"<b>STALE</b> {_stale_n} position(s) stale "
                                f"≥{_ad.STALE_AGE_THRESHOLD_SEC // 3600}h\n"
                                f"symbols: {_shown}{_more}",
                                "WARN",
                            )
                        else:
                            _tg.send("STALE positions recovered — all fresh", "INFO")
                except Exception:
                    pass

            # 5.4 Startup block release check (2-pass to prevent late fill race)
            if _buy_blocked_startup:
                _still_open = provider.query_open_orders() or []
                _fill_pending = len(_pending_fills) > 0
                logger.info(f"[STARTUP_BLOCK_RELEASE_CHECK] "
                            f"open_orders={len(_still_open)} fill_queue={len(_pending_fills)}")
                if not _still_open and not _fill_pending:
                    # 2nd check after brief settle (late fill race defense)
                    time.sleep(0.5)
                    provider.process_events()
                    _fill_pending_2 = len(_pending_fills) > 0
                    if not _fill_pending_2:
                        _buy_blocked_startup = False
                        runtime_data["buy_blocked"] = False
                        logger.info("[STARTUP_BLOCK_RELEASED] open_orders=0, "
                                    "fill_queue=0 (2-pass confirmed)")
                        notify.send("Startup block released - BUY enabled", "INFO")
                    else:
                        logger.warning("[STARTUP_BLOCK_HELD] late fill detected in 2nd pass")

            # 5.5 DD Guard — buy_blocked evaluation (P0 fix: fail-closed)
            _equity = portfolio.get_equity()
            portfolio.update_dd_tracking()  # month peak 갱신
            _daily_pnl = portfolio.get_daily_pnl_pct()
            _monthly_dd = portfolio.get_monthly_dd_pct()
            _buy_scale = 1.0
            _dd_label = "NORMAL"

            for _thresh, _scale, _trim, _label in config.DD_LEVELS:
                if _monthly_dd <= _thresh:
                    _buy_scale = _scale
                    _dd_label = _label
                    break

            if _daily_pnl <= config.DAILY_DD_LIMIT and _buy_scale > 0:
                _buy_scale = 0.0
                _dd_label = "DAILY_BLOCKED"

            _buy_blocked = _buy_blocked_startup or _buy_scale == 0.0

            if _dd_label != "NORMAL":
                logger.warning(f"[DD_GUARD] {_dd_label} daily={_daily_pnl:.2%} "
                               f"monthly={_monthly_dd:.2%} buy_scale={_buy_scale:.0%}")

            # 5.5.1 Observability: DD label transition alert (no spam)
            try:
                from notify import alert_dedup as _ad, telegram_bot as _tg
                _fired, _prev_label, _new_label = _ad.dd_transition(_dd_label)
                if _fired:
                    if _new_label != "NORMAL":
                        _tg.send(
                            f"<b>DD_GUARD {_new_label}</b> "
                            f"daily={_daily_pnl:.2%} monthly={_monthly_dd:.2%} "
                            f"buy_scale={_buy_scale:.0%}",
                            "WARN",
                        )
                    else:
                        _tg.send(
                            f"DD_GUARD recovered to NORMAL (was {_prev_label}) "
                            f"daily={_daily_pnl:.2%} monthly={_monthly_dd:.2%}",
                            "INFO",
                        )
            except Exception:
                pass

            # 5.5.2 Observability: single-loop equity drop alert
            try:
                from notify import alert_dedup as _ad, telegram_bot as _tg
                _e_fired, _e_prev, _e_curr = _ad.equity_drop_should_fire(_equity)
                if _e_fired and _e_prev > 0:
                    _drop_pct = (_e_curr - _e_prev) / _e_prev * 100
                    _tg.send(
                        f"<b>equity drop {_drop_pct:.2f}%</b> in single loop "
                        f"(${_e_prev:,.0f} → ${_e_curr:,.0f}) — verify broker truth",
                        "CRITICAL",
                    )
            except Exception:
                pass

            # Persist DD state for dashboard / future rebalance
            runtime_data["dd_label"] = _dd_label
            runtime_data["buy_scale"] = _buy_scale
            runtime_data["buy_blocked"] = _buy_blocked

            # 6. Trail stop evaluation (SELL always allowed)
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

            # Near notifications — per-symbol time-based throttle so a
            # position oscillating around the near boundary doesn't spam
            # the channel (Jeff 2026-05-01: DELL fired 165 times in 24h
            # with the prior reset-on-recovery dedup).
            near_syms = {s for s, _ in near}
            try:
                from notify import alert_dedup as _ad_near
                # Mark exits — every tracked symbol that's no longer in
                # the zone is told so the helper can compute recovery.
                for sym in list(_notified_near):
                    if sym not in near_syms:
                        _ad_near.trail_near_should_fire(sym, in_near_zone=False)
                        _notified_near.discard(sym)
                for sym, dd_pct in near:
                    if _ad_near.trail_near_should_fire(sym, in_near_zone=True):
                        notify.notify_trail_near(sym, dd_pct)
                    _notified_near.add(sym)  # track membership for recovery
            except Exception:
                # Defensive — fall back to old behavior if helper missing
                # or import fails.
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

            # 8. Periodic RECON (10 min, apply + persist)
            # P2-RECON-1 fix (2026-04-17): 로그만 찍던 주기적 RECON을
            # SAFE_SYNC 실제 적용 + runtime_data 갱신으로 변경.
            # 브로커 포지션 드리프트(수동 거래/시스템 오류)를 10분 내 교정.
            #
            # INVARIANT (RECON cycle 고정 순서 — STEP 3+ 수정 금지):
            #   1. broker fetch (_holdings, _acct, _orders) — 단일 snapshot
            #   2. reconcile_with_broker()               — diff 계산
            #   3. apply_recon()                         — broker truth 반영 + pending=0
            #   4. sync_pending_with_broker(_orders)     — 동일 _orders 기준 pending 재설정
            #   5. runtime_data["last_recon_ok"] 등 기록 — gate truth 갱신
            #   6. state_mgr.save_all()                  — atomic 저장
            #
            #   이 순서를 바꾸면: stale pending 잔존 / gate 오판 / trail stop 영구 차단.
            if now - last_recon_time >= RECON_INTERVAL:
                _holdings = provider.query_account_holdings()
                _acct = provider.query_account_summary()
                _orders = provider.query_open_orders() or []
                if _holdings is not None and "error" not in _acct:
                    _recon = portfolio.reconcile_with_broker(
                        _holdings, _acct.get("cash", 0), False, _orders
                    )
                    # Persist recon state to runtime (gate reads last_recon_ok)
                    runtime_data["last_recon_ok"] = _recon.clean
                    runtime_data["state_uncertain"] = _recon.state_uncertain
                    runtime_data["last_recon_at"] = _now_iso()

                    if not _recon.clean:
                        logger.warning(
                            f"[RECON_PERIODIC] action={_recon.action} "
                            f"added={len(_recon.added)} removed={len(_recon.removed)} "
                            f"qty_mismatch={len(_recon.qty_mismatch)}"
                        )
                        # Apply correction — SAFE_SYNC only (FORCE_SYNC requires dirty_exit)
                        if _recon.action in ("SAFE_SYNC",):
                            portfolio.apply_recon(_recon, _holdings, _acct.get("cash", 0))
                            logger.info(f"[RECON_PERIODIC] SAFE_SYNC applied")
                        elif _recon.action == "FORCE_SYNC":
                            # Periodic RECON은 dirty_exit 없이 FORCE_SYNC 드문 케이스.
                            # 포지션 추가/삭제 중 qty 큰 차이 → apply + CRITICAL 알림.
                            portfolio.apply_recon(_recon, _holdings, _acct.get("cash", 0))
                            logger.critical(
                                f"[RECON_PERIODIC_FORCE] Unexpected FORCE_SYNC in periodic "
                                f"RECON — large divergence. added={_recon.added} "
                                f"removed={_recon.removed}"
                            )
                            try:
                                from notify import telegram_bot as _notify
                                _notify.send(
                                    f"[RECON_FORCE] Periodic RECON detected large divergence.\n"
                                    f"added={_recon.added} removed={_recon.removed}",
                                    severity="CRITICAL",
                                )
                            except Exception:
                                pass

                        # P2-PENDING-1 fix (2026-04-17): apply_recon 후 pending 재동기화.
                        # 순서 보장: apply_recon(pending=0) → sync_pending(broker 기준 재설정)
                        # 동일 _orders snapshot 사용 — 일관성 보장.
                        portfolio.sync_pending_with_broker(_orders)

                        # [RECON_POST] invariant log — correction 후 상태 검증용
                        _pending_total = sum(
                            p.pending_sell_qty for p in portfolio.positions.values()
                        )
                        logger.warning(
                            f"[RECON_POST] action={_recon.action} "
                            f"positions={len(portfolio.positions)} "
                            f"cash={portfolio.cash:.2f} "
                            f"pending_sell_total={_pending_total} "
                            f"last_recon_ok={runtime_data.get('last_recon_ok')} "
                            f"state_uncertain={runtime_data.get('state_uncertain')}"
                        )

                        # Save immediately after any RECON correction
                        state_mgr.save_all(portfolio.to_dict(), runtime_data)
                        last_save_time = now
                    else:
                        # Clean RECON — still persist recon_ok=True + sync pending
                        runtime_data["last_recon_ok"] = True
                        runtime_data["state_uncertain"] = False
                        # P2-PENDING-1 fix: clean RECON에서도 pending 동기화.
                        # stale pending_sell_qty가 fill event 없이 10분 이상 지속되는 경우 해제.
                        portfolio.sync_pending_with_broker(_orders)
                else:
                    # Holdings/account query failed — mark recon unreliable
                    runtime_data["recon_unreliable"] = True
                    runtime_data["last_recon_at"] = _now_iso()
                    logger.warning("[RECON_PERIODIC_FAIL] Holdings or account query failed")
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
