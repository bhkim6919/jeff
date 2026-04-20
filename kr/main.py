"""
Gen4 Core — Main Entry Point
==============================
LowVol + Momentum 12-1 monthly rebalance strategy.

Usage:
  python main.py --batch      # OHLCV update + scoring + save target
  python main.py --live       # REST live: monitor trail + execute rebalance
  python main.py --rebalance  # Force rebalance now (manual)
  python main.py --backtest   # Run backtester
  python main.py --mock       # Mock mode (no broker, simulated)

Requires: venv python with pykrx
  C:\\Q-TRON-32_ARCHIVE\\.venv\\Scripts\\python.exe main.py --batch
"""
from __future__ import annotations

# ── Path bootstrap MUST be first import (sys.path setup for shared/) ─────────
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent))  # audit:allow-syspath: bootstrap-locator (must precede import _bootstrap_path)
import _bootstrap_path  # noqa: F401  -- side-effect: sys.path setup

import argparse
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

# ── Telegram notifier (failure never blocks trading) ─────────────────────────
try:
    from notify.telegram_bot import send as _tg_send
    from notify.telegram_bot import notify_buy as _kakao_buy
    from notify.telegram_bot import notify_sell as _kakao_sell
    from notify.telegram_bot import notify_trail_triggered as _kakao_trail
    from notify.telegram_bot import notify_dd_warning as _kakao_safe_mode_raw
    from notify.telegram_bot import send as _kakao_notify
    def _kakao_safe_mode(level, reason=""):
        _tg_send(f"<b>SAFE MODE L{level}</b>\n{reason}", "CRITICAL")
    def _kakao_buy_blocked(reason):
        _tg_send(f"<b>BUY BLOCKED</b>\n{reason}", "WARN")
    _KAKAO_OK = True
except Exception:
    _KAKAO_OK = False

# ── Extracted modules ─────────────────────────────────────────────────────────
from notify.helpers import (
    _load_name_cache, _save_name_cache, _enrich_name_cache,
    _kname, _notify_buy, _notify_sell, _notify_trail,
)
from lifecycle.utils import (
    _file_hash, setup_logging, is_weekday, is_trading_day, is_market_hours,
    _resolve_trading_mode, validate_trading_mode,
    _safe_save, _save_test_reentry_meta,
    _count_trading_days, _compute_regime_snapshot,
)
from lifecycle.batch import run_batch
from lifecycle.mock import run_mock
from lifecycle.reconcile import _reconcile_with_broker

# ── Live mode: orchestrator replaces monolithic run_live() ────────────────────
from lifecycle.live_orchestrator import run_live

# ── Risk management (used by rebalance_phase via import) ──────────────────────
from risk.risk_management import _execute_dd_trim
from risk.risk_management import _check_emergency_rebalance


def _try_fast_reentry(state_mgr, portfolio, config, executor, provider,
                      trade_logger, logger, mode_label, tracker=None,
                      name_cache=None, guard=None) -> bool:
    """paper_test only: execute pending buys after delay instead of T+1.

    Returns True if reentry executed (or permanently skipped), False to retry next cycle.
    """
    rt = state_mgr.load_runtime()
    ready_at_str = rt.get("test_reentry_ready_at", "")
    cycle_id = rt.get("test_cycle_id", "")
    sell_status = rt.get("rebal_sell_status", "")
    pending_buys = rt.get("pending_buys", [])
    dd_blocked_buys = rt.get("dd_blocked_buys", [])

    # ── Diagnostic: re-evaluate DD_GUARD with current equity ──
    daily_pnl = portfolio.get_daily_pnl_pct()
    monthly_dd = portfolio.get_monthly_dd_pct()
    can_buy_now = True
    buy_scale_now = 1.0
    risk_level = "UNKNOWN"
    if guard:
        risk_action = guard.get_risk_action(
            daily_pnl, monthly_dd,
            dd_levels=config.DD_LEVELS,
            safe_mode_release=config.SAFE_MODE_RELEASE_THRESHOLD)
        skip_buys, _ = guard.should_skip_rebalance(daily_pnl, monthly_dd)
        buy_scale_now = risk_action["buy_scale"]
        if skip_buys:
            buy_scale_now = 0.0
        can_buy_now = buy_scale_now > 0
        risk_level = risk_action["level"]

    # Use dd_blocked_buys as fallback when pending_buys is empty due to DD block
    effective_buys = pending_buys if pending_buys else dd_blocked_buys

    logger.info(
        f"[FAST_REENTRY_CHECK] ready_at={ready_at_str or 'NONE'} "
        f"sell_status={sell_status or 'NONE'} "
        f"pending_buys={len(pending_buys)} dd_blocked_buys={len(dd_blocked_buys)} "
        f"monthly_dd={monthly_dd:.2%} daily_pnl={daily_pnl:.2%} "
        f"risk={risk_level} buy_scale={buy_scale_now:.0%} can_buy={can_buy_now}")

    if not ready_at_str:
        logger.info("[FAST_REENTRY_BLOCKED] reason=NO_READY_AT (reentry not scheduled)")
        return False

    if not effective_buys:
        logger.info("[FAST_REENTRY_BLOCKED] reason=NO_PENDING_BUYS "
                    "(pending_buys and dd_blocked_buys both empty)")
        return False

    # Check sell_status
    if sell_status not in ("COMPLETE",):
        logger.info(f"[FAST_REENTRY_BLOCKED] reason=SELL_STATUS_{sell_status} "
                    f"(waiting for ghost fills to settle)")
        return False

    # Check time
    try:
        ready_at = datetime.fromisoformat(ready_at_str)
    except (ValueError, TypeError):
        logger.warning(f"[FAST_REENTRY_BLOCKED] reason=INVALID_READY_AT "
                       f"value={ready_at_str} — skipping permanently")
        return True  # skip permanently

    now = datetime.now()
    remaining = (ready_at - now).total_seconds()
    if remaining > 0:
        logger.info(f"[FAST_REENTRY_BLOCKED] reason=NOT_YET "
                    f"remaining={remaining:.0f}s ready_at={ready_at_str}")
        return False  # not yet

    # Re-evaluate DD_GUARD at execution time
    if not can_buy_now:
        logger.warning(
            f"[FAST_REENTRY_BLOCKED] reason=DD_GUARD "
            f"level={risk_level} monthly_dd={monthly_dd:.2%} "
            f"daily_pnl={daily_pnl:.2%} buy_scale={buy_scale_now:.0%} "
            f"— will retry next cycle")
        return False  # retry — DD may improve

    # If original pending_buys was empty but dd_blocked_buys available,
    # validate metadata, filter stale/duplicate, then restore
    if not pending_buys and dd_blocked_buys:
        dd_meta = rt.get("dd_blocked_meta", {})
        blocked_cycle = dd_meta.get("cycle_id", "")
        blocked_created = dd_meta.get("created_at", "")

        if blocked_cycle and cycle_id and blocked_cycle != cycle_id:
            logger.warning(
                f"[DD_BLOCKED_RESTORE] DISCARD_CYCLE_MISMATCH "
                f"blocked_cycle={blocked_cycle} current_cycle={cycle_id} "
                f"— {len(dd_blocked_buys)} buys discarded")
            rt["dd_blocked_buys"] = []
            rt.pop("dd_blocked_meta", None)
            state_mgr.save_runtime(rt)
            return True

        if blocked_created:
            try:
                created_dt = datetime.fromisoformat(blocked_created)
                age_hours = (datetime.now() - created_dt).total_seconds() / 3600
                if age_hours > 24:
                    logger.warning(
                        f"[DD_BLOCKED_RESTORE] DISCARD_STALE "
                        f"created_at={blocked_created} age={age_hours:.1f}h "
                        f"— {len(dd_blocked_buys)} buys discarded")
                    rt["dd_blocked_buys"] = []
                    rt.pop("dd_blocked_meta", None)
                    state_mgr.save_runtime(rt)
                    return True
            except (ValueError, TypeError):
                pass

        existing_codes = set(portfolio.positions.keys())
        restored = []
        for b in dd_blocked_buys:
            ticker = b["ticker"]
            if ticker in existing_codes:
                logger.info(f"[DD_BLOCKED_RESTORE] code={ticker} "
                            f"action=SKIP_EXISTING (already in portfolio)")
                continue
            b["target_amount"] = b.get("original_amount", b["target_amount"]) * buy_scale_now
            restored.append(b)
            logger.info(f"[DD_BLOCKED_RESTORE] code={ticker} action=RESTORE "
                        f"amount={b['target_amount']:,.0f} scale={buy_scale_now:.0%}")

        n_skip = len(dd_blocked_buys) - len(restored)
        logger.info(
            f"[DD_BLOCKED_RESTORE_SUMMARY] total={len(dd_blocked_buys)} "
            f"restored={len(restored)} skipped_existing={n_skip} "
            f"cycle={blocked_cycle} buy_scale={buy_scale_now:.0%}")

        if not restored:
            logger.info("[FAST_REENTRY_BLOCKED] reason=ALL_BLOCKED_BUYS_FILTERED")
            rt["dd_blocked_buys"] = []
            rt.pop("dd_blocked_meta", None)
            state_mgr.save_runtime(rt)
            return True

        effective_buys = restored

    # All conditions met — execute
    logger.info(f"[FAST_REENTRY_EXECUTE] cycle={cycle_id} "
                f"buys={len(effective_buys)} sell_status={sell_status} "
                f"cash={portfolio.cash:,.0f} buy_scale={buy_scale_now:.0%}")
    logger.info(f"[TRACKER_WIRED] fast_reentry tracker={'yes' if tracker else 'no'}")

    _execute_pending_buys(
        portfolio, effective_buys, config, executor, provider,
        trade_logger, state_mgr, logger, mode_label, tracker=tracker,
        name_cache=name_cache)

    # Mark executed
    rt["pending_buys"] = []
    rt["dd_blocked_buys"] = []
    rt.pop("dd_blocked_meta", None)
    rt["rebal_sell_status"] = ""
    rt["test_reentry_executed"] = True
    state_mgr.save_runtime(rt)
    _safe_save(state_mgr, portfolio, context="paper_test_reentry_complete")
    logger.info(f"[PAPER_TEST_REENTRY_DONE] cycle={cycle_id}")
    return True


def _execute_pending_buys(portfolio, pending_buys, config, executor,
                          provider, trade_logger, state_mgr, logger,
                          mode_label, tracker=None, name_cache=None):
    """Execute pending buy orders from previous session (T+1 model).

    Broker cash sync first, then each buy with cash hard cap.
    Returns: (success_count, fail_count)
    """
    # Shadow mode: dry-run only
    if config.SHADOW_MODE:
        for pb in pending_buys:
            logger.info(
                f"[SHADOW_PENDING_BUY] {pb['ticker']}: "
                f"amount={pb.get('target_amount', 0):,.0f} — DRY RUN (not executed)")
        logger.info(f"[SHADOW_PENDING_BUY] {len(pending_buys)} buys skipped (shadow mode)")
        return (0, 0)

    # Broker cash sync
    cash_source = "LOCAL"
    extra_buffer = 1.0
    try:
        summary = provider.query_account_summary()
        if not summary.get("error"):
            broker_cash = summary.get("available_cash", 0)
            if broker_cash > 0:
                cash_source = "BROKER_SYNC"
                if abs(broker_cash - portfolio.cash) / max(portfolio.cash, 1) > 0.05:
                    logger.warning(
                        f"[CASH_DIVERGENCE] broker={broker_cash:,.0f} "
                        f"local={portfolio.cash:,.0f} "
                        f"diff={broker_cash - portfolio.cash:,.0f}")
                    portfolio.cash = broker_cash
    except Exception as e:
        logger.warning(f"[BROKER_CASH_FETCH_FAIL] {e} — using local cash")

    if cash_source != "BROKER_SYNC":
        logger.warning(
            f"[BUY_REDUCED_STALE_CASH] cash_source={cash_source} "
            f"— applying 80% extra buffer")
        extra_buffer = 0.80

    _pb_success = 0
    _pb_fail = 0
    _pb_failed_tickers = []

    logger.info(
        f"[PENDING_BUY_EXEC] {len(pending_buys)} buys, "
        f"cash={portfolio.cash:,.0f}, source={cash_source}")
    logger.info(f"[TRACKER_WIRED] pending_buys tracker={'yes' if tracker else 'no'}")

    # ── P2.4: Auto Trading Gate (BUY-only, T+1 path) ────────────
    try:
        from risk.execution_guard_hook import guard_buy_execution
        from risk.strategy_health import compute_strategy_health
        _rt_for_gate = state_mgr.load_runtime() or {}
        _equity_dd = float(_rt_for_gate.get("equity_dd_pct", 0.0) or 0.0)
        _health = compute_strategy_health(equity_dd_pct=_equity_dd)
        _decision = guard_buy_execution(
            runtime=_rt_for_gate, portfolio=portfolio,
            strategy_health=_health,
        )
        from datetime import datetime as _dt
        _req_id = f"pending-buy-{_dt.now().strftime('%Y%m%d')}"
        if _decision.block_buy:
            logger.critical(
                f"[BUY_BLOCKED_BY_AUTO_GATE] market=KR path=pending_buy "
                f"req={_req_id} mode={_decision.mode} "
                f"top={_decision.highest_blocker} reason={_decision.reason} "
                f"n_skipped={len(pending_buys)}")
            return (0, len(pending_buys))
        elif not _decision.enabled:
            logger.info(
                f"[BUY_ADVISORY] market=KR path=pending_buy req={_req_id} "
                f"mode={_decision.mode} top={_decision.highest_blocker} "
                f"reason={_decision.reason} — proceeding (enforce=OFF)")
        else:
            logger.info(
                f"[BUY_GATE_ALLOWED] market=KR path=pending_buy req={_req_id} "
                f"mode={_decision.mode} n={len(pending_buys)}")
    except Exception as _ge:
        logger.error(f"[BUY_GATE_EVAL_ERROR] path=pending_buy "
                     f"{type(_ge).__name__}: {_ge}")

    # BUY re-entry protection
    _pending_ext_buy_codes = set()
    if tracker:
        from runtime.order_tracker import OrderStatus
        _pending_ext_buy_codes = {r.code for r in tracker._orders.values()
                                  if r.status == OrderStatus.PENDING_EXTERNAL
                                  and r.side == "BUY"}
        if _pending_ext_buy_codes:
            logger.info(f"[PENDING_EXT_BUY_GUARD] blocking codes: {_pending_ext_buy_codes}")

    for pb in pending_buys:
        ticker = pb["ticker"]
        if ticker in _pending_ext_buy_codes:
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: reason=PENDING_EXTERNAL — "
                           f"ghost/reconcile pending, skip to avoid duplicate")
            continue
        if ticker in portfolio.positions:
            existing_qty = portfolio.positions[ticker].quantity
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: reason=EXISTING_POSITION "
                           f"qty={existing_qty} — likely RECON BROKER_ONLY, skip")
            continue
        live_price = executor.get_live_price(ticker)
        if live_price <= 0:
            logger.warning(f"[PENDING_BUY_SKIP] {ticker}: no price")
            continue

        available = max(0, portfolio.cash * config.CASH_BUFFER_RATIO * extra_buffer)
        max_qty = int(available / (live_price * (1 + config.BUY_COST)))
        target_qty = int(pb["target_amount"] / (live_price * (1 + config.BUY_COST)))
        final_qty = min(target_qty, max_qty)

        if final_qty <= 0:
            reason = "INSUFFICIENT_CASH" if max_qty <= 0 else "REPRICE_REDUCED_QTY"
            logger.warning(
                f"[PENDING_BUY_SKIP] {ticker}: {reason} "
                f"available={available:,.0f} price={live_price:,.0f} "
                f"target_qty={target_qty} max_qty={max_qty}")
            continue

        if final_qty < target_qty:
            logger.info(f"[PENDING_BUY_CAPPED] {ticker}: {target_qty} -> {final_qty}")

        result = executor.execute_buy(ticker, final_qty, "REBALANCE_ENTRY")
        if not result.get("error"):
            fill_price = result["exec_price"] or live_price
            applied_qty = result["exec_qty"]
            logger.info(f"[PORTFOLIO] PENDING_BUY {ticker} requested={final_qty} "
                        f"exec_qty={applied_qty} fill_price={fill_price:,.0f}")
            portfolio.add_position(
                ticker, applied_qty, fill_price,
                entry_date=str(date.today()),
                buy_cost=config.BUY_COST)
            _new_pos = portfolio.positions.get(ticker)
            if _new_pos:
                _new_pos.entry_rank = pb.get("rank", 0)
                _new_pos.score_mom = pb.get("score_mom", 0.0)
            _notify_buy(ticker, name_cache or {}, applied_qty, fill_price)
            _safe_save(state_mgr, portfolio, context=f"pending_buy/{ticker}")
            _pb_success += 1
        else:
            error_str = result.get("error", "")
            _pb_fail += 1
            _pb_failed_tickers.append(ticker)
            if "TIMEOUT_UNCERTAIN" in error_str:
                logger.warning(f"[PENDING_BUY_PENDING_EXTERNAL] {ticker}: "
                               f"timeout — ghost/reconcile will resolve")
            else:
                logger.error(f"[PENDING_BUY_FAILED] {ticker}: {error_str}")

    logger.info(f"[PENDING_BUY_COMPLETE] success={_pb_success} fail={_pb_fail} "
                f"cash_remaining={portfolio.cash:,.0f}")
    if _pb_failed_tickers:
        logger.info(f"[PENDING_BUY_FAILED_TICKERS] {_pb_failed_tickers}")
    return (_pb_success, _pb_fail)


def _execute_rebalance_live(portfolio, target, config, executor, provider,
                             trade_logger, skip_buys, logger,
                             state_mgr=None, today_str="",
                             buy_scale: float = 1.0,
                             risk_action: dict = None,
                             regime: str = "",
                             mode_str: str = "LIVE",
                             tracker=None,
                             name_cache=None) -> tuple:
    """Execute rebalance sells, generate pending buys for T+1.

    Returns: (price_fail_count, pending_buys_list, sell_status)
    """
    from strategy.rebalancer import compute_orders
    from report.reporter import make_event_id

    # Rebalance dedup
    if state_mgr and today_str and mode_str not in ("paper_test", "shadow_test"):
        runtime_check = state_mgr.load_runtime()
        if runtime_check.get("last_rebalance_date") == today_str:
            logger.warning("Rebalance already recorded today (%s) — SKIP", today_str)
            return 0, [], "COMPLETE"

    target_tickers = target["target_tickers"]
    scores = target.get("scores", {})

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

    # KR-P0-003: skip reconcile_pending symbols (QTY_SPIKE isolated by RECON)
    _pending_codes = {
        code for code, pos in portfolio.positions.items()
        if getattr(pos, "reconcile_pending", False)
    }
    if _pending_codes:
        _n_before = (len(sell_orders), len(buy_orders))
        sell_orders = [o for o in sell_orders if o.ticker not in _pending_codes]
        buy_orders = [o for o in buy_orders if o.ticker not in _pending_codes]
        logger.critical(
            f"[REBAL_SKIP_RECONCILE_PENDING] isolated_codes={sorted(_pending_codes)} "
            f"sells={_n_before[0]}->{len(sell_orders)} "
            f"buys={_n_before[1]}->{len(buy_orders)}")

    # Execute Sells First
    logger.info(f"Sells: {len(sell_orders)} orders")
    sell_results = []
    for order in sell_orders:
        pos = portfolio.positions.get(order.ticker)
        eid = make_event_id(order.ticker, "SELL")

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
                event_id=eid,
                regime=regime)

        if config.SHADOW_MODE:
            _price = prices.get(order.ticker, 0)
            logger.info(f"[SHADOW_SELL] {order.ticker}: qty={order.quantity}, "
                        f"price={_price:,.0f}, "
                        f"value={order.quantity * _price:,.0f} — DRY RUN")
            sell_results.append("SHADOW")
            continue

        result = executor.execute_sell(order.ticker, order.quantity, "REBALANCE_EXIT")
        if not result.get("error"):
            fill_price = result["exec_price"] or prices.get(order.ticker, 0)
            exec_qty = result.get("exec_qty", order.quantity)
            logger.info(f"[PORTFOLIO] SELL {order.ticker} requested={order.quantity} "
                        f"exec_qty={exec_qty} fill_price={fill_price:,.0f}")
            _pre_pos = portfolio.positions.get(order.ticker)
            _er = _pre_pos.entry_rank if _pre_pos else 0
            _sm = _pre_pos.score_mom if _pre_pos else 0.0
            _hwm_p = ((_pre_pos.high_watermark / _pre_pos.avg_price - 1)
                      if _pre_pos and _pre_pos.avg_price > 0 else 0)
            trade = portfolio.remove_position(order.ticker, fill_price, config.SELL_COST,
                                              qty=exec_qty)
            if trade:
                trade["exit_reason"] = "REBALANCE_EXIT"
                trade_logger.log_close(trade, "REBALANCE_EXIT", mode_str,
                                       event_id=eid,
                                       entry_rank=_er,
                                       score_mom=_sm,
                                       max_hwm_pct=_hwm_p)
            _pnl = trade.get("pnl_pct", 0.0) if trade else 0.0
            _avg = _pre_pos.avg_price if _pre_pos else 0.0
            _notify_sell(order.ticker, name_cache or {}, exec_qty, fill_price, _pnl, "REBALANCE",
                         avg_price=_avg)
            _sell_status = "FILLED" if exec_qty >= order.quantity else "PARTIAL"
            sell_results.append(_sell_status)
            if state_mgr:
                _safe_save(state_mgr, portfolio,
                           context=f"rebalance_sell/{order.ticker}/{_sell_status}")
        else:
            error_str = result.get("error", "")
            if "TIMEOUT_UNCERTAIN" in error_str:
                logger.warning(f"[SELL_PENDING_EXTERNAL] {order.ticker}: "
                               f"timeout — ghost/reconcile will resolve")
                sell_results.append("PENDING")
                if state_mgr:
                    _safe_save(state_mgr, portfolio,
                               context=f"rebalance_sell/{order.ticker}/PENDING")
            else:
                logger.error(f"SELL failed {order.ticker}: {error_str}")
                sell_results.append("FAILED")

    # Post-sell checkpoint
    if state_mgr:
        _safe_save(state_mgr, portfolio, context="rebalance_post_sell_checkpoint")
        logger.info("Post-sell checkpoint saved (rebalance date NOT yet marked)")

    # Sell status determination
    pending_sells = []
    if tracker:
        pending_sells = [r for r in tracker.pending_today()
                         if r.side == "SELL" and r.reason == "REBALANCE_EXIT"]
    if pending_sells:
        sell_status = "PARTIAL"
    elif not sell_orders:
        sell_status = "COMPLETE"
    elif all(s in ("FILLED", "SHADOW") for s in sell_results):
        sell_status = "COMPLETE"
    elif any(s in ("FILLED", "PENDING", "PARTIAL") for s in sell_results):
        sell_status = "PARTIAL"
    else:
        sell_status = "FAILED"

    logger.info(f"[REBAL_SELL_STATUS] {sell_status} "
                f"(filled={sell_results.count('FILLED')}, "
                f"partial={sell_results.count('PARTIAL')}, "
                f"pending={sell_results.count('PENDING')}, "
                f"failed={sell_results.count('FAILED')}, "
                f"pending_ext={len(pending_sells)})")

    time.sleep(2)

    # DD Graduated: Position Trim
    if risk_action and risk_action.get("trim_ratio", 0) > 0:
        trim_ratio = risk_action["trim_ratio"]
        level = risk_action["level"]
        logger.warning(f"[DD_TRIM_START] {level}: trimming {trim_ratio:.0%} of all positions")
        _execute_dd_trim(portfolio, trim_ratio, executor, config,
                         trade_logger, mode_str, logger)

    # Generate Pending Buys (T+1 model)
    pending_buys_list = []

    if skip_buys or buy_scale <= 0:
        reason = risk_action["level"] if risk_action else "DD_GUARD"
        logger.warning(f"[DD_GUARD_TRIGGERED] {reason}: buys BLOCKED (no pending buys)")
        dd_blocked_list = []
        for rank_idx, order in enumerate(buy_orders, 1):
            dd_blocked_list.append({
                "ticker": order.ticker,
                "target_amount": order.target_amount,
                "original_amount": order.target_amount,
                "score_vol": scores.get(order.ticker, {}).get("vol_12m", 0),
                "score_mom": scores.get(order.ticker, {}).get("mom_12_1", 0),
                "rank": rank_idx,
                "signal_date": today_str,
            })
        if dd_blocked_list:
            _rt = state_mgr.load_runtime()
            _rt["dd_blocked_buys"] = dd_blocked_list
            _rt["dd_blocked_meta"] = {
                "cycle_id": _rt.get("test_cycle_id", today_str),
                "created_at": datetime.now().isoformat(),
                "sell_status": sell_status,
            }
            state_mgr.save_runtime(_rt)
            logger.info(f"[DD_BLOCKED_BUYS_SAVED] {len(dd_blocked_list)} buys "
                        f"saved for reentry if DD clears "
                        f"(cycle={_rt['dd_blocked_meta']['cycle_id']})")
    else:
        if buy_scale < 1.0:
            logger.info(f"[DD_BUY_SCALED] buy allocation * {buy_scale:.0%}")

        for rank_idx, order in enumerate(buy_orders, 1):
            scaled_amount = order.target_amount * buy_scale
            if config.SHADOW_MODE:
                _price = prices.get(order.ticker, 0)
                _qty = int(scaled_amount / _price) if _price > 0 else 0
                logger.info(f"[SHADOW_BUY] {order.ticker}: amount={scaled_amount:,.0f}, "
                            f"est_qty={_qty}, price={_price:,.0f} — DRY RUN")
                continue
            pending_buys_list.append({
                "ticker": order.ticker,
                "target_amount": scaled_amount,
                "score_vol": scores.get(order.ticker, {}).get("vol_12m", 0),
                "score_mom": scores.get(order.ticker, {}).get("mom_12_1", 0),
                "rank": rank_idx,
                "signal_date": today_str,
            })

        _label = "SHADOW — no orders will be sent" if config.SHADOW_MODE else "T+1 model"
        logger.info(f"[PENDING_BUYS_GENERATED] {len(pending_buys_list)} buys "
                    f"queued ({_label})")

    # Summary
    buy_skipped = [o.ticker for o in buy_orders
                   if prices.get(o.ticker, 0) <= 0]
    if buy_skipped or price_fail_codes:
        logger.warning(f"Rebalance summary — price-failed codes: {price_fail_codes}, "
                       f"buy-skipped: {buy_skipped}")

    trade_logger.log_rebalance_summary(len(sell_orders), len(pending_buys_list),
                                        portfolio.get_current_equity())
    return len(price_fail_codes), pending_buys_list, sell_status


# ── Auto-restart wrapper ─────────────────────────────────────────────────────
MAX_RESTART_ATTEMPTS = 3
MIN_RESTART_INTERVAL_SEC = 60


def _run_live_with_restart(config):
    """Run live with auto-restart on crash (max N attempts)."""
    _logger = logging.getLogger("gen4.live")
    last_crash_time = None

    for attempt in range(1, MAX_RESTART_ATTEMPTS + 1):
        try:
            _logger.info(f"[LIVE_START] attempt {attempt}/{MAX_RESTART_ATTEMPTS}")
            run_live(config)
            _logger.info("[LIVE_END] normal exit")
            return
        except KeyboardInterrupt:
            _logger.info("[LIVE_END] user interrupt (Ctrl+C)")
            return
        except SystemExit:
            _logger.info("[LIVE_END] system exit")
            return
        except Exception as e:
            _logger.error(f"[LIVE_CRASH] attempt {attempt}: {e}", exc_info=True)
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
    group.add_argument("--live", action="store_true", help="Live: REST trading")
    group.add_argument("--rebalance", action="store_true",
                       help="[DEPRECATED] Use --paper-test --force-rebalance")
    group.add_argument("--backtest", action="store_true", help="Run backtester")
    group.add_argument("--mock", action="store_true", help="Mock mode (test)")
    group.add_argument("--paper-test", action="store_true",
                       help="Paper test: separate state + test signals")
    group.add_argument("--shadow-test", action="store_true",
                       help="Shadow test: compute only, no orders (dry run)")
    group.add_argument("--server", action="store_true",
                       help="Start REST dashboard server (background-friendly)")
    parser.add_argument("--start", default="2019-01-02")
    parser.add_argument("--end", default=str(date.today()))
    parser.add_argument("--cycle", choices=["full", "sell_only", "buy_only"],
                        default="full",
                        help="paper-test cycle mode (default: full)")
    parser.add_argument("--fresh", action="store_true",
                        help="paper-test/shadow-test: delete state files, start from broker")
    parser.add_argument("--force-rebalance", action="store_true",
                        help="Force rebalance (paper_test/shadow_test only)")
    parser.add_argument("--confirm-force-rebalance", action="store_true",
                        help="Confirm force rebalance intent (required with --force-rebalance)")
    parser.add_argument("--fast", action="store_true",
                        help="Batch: skip reports + fundamental (signal only)")
    args = parser.parse_args()

    # Force-rebalance safety checks
    if args.force_rebalance:
        if args.live:
            print("ERROR: --force-rebalance is NOT allowed in live mode.")
            print("  Use: --paper-test --force-rebalance --confirm-force-rebalance")
            print("   Or: --shadow-test --force-rebalance --confirm-force-rebalance")
            sys.exit(1)
        if not (args.paper_test or args.shadow_test):
            print("ERROR: --force-rebalance requires --paper-test or --shadow-test")
            sys.exit(1)
        if not args.confirm_force_rebalance:
            print("ERROR: --force-rebalance requires --confirm-force-rebalance")
            print("  This is a safety check to prevent accidental force rebalance.")
            sys.exit(1)

    # sys.path setup already done by `import _bootstrap_path` at top of file.
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
        run_batch(config, fast=getattr(args, "fast", False))
    elif args.live:
        setup_logging(config.LOG_DIR, "live")
        _run_live_with_restart(config)
    elif args.paper_test:
        config.TRADING_MODE = "paper_test"
        config.PAPER_TEST_CYCLE = args.cycle
        if args.force_rebalance:
            config.PAPER_TEST_FORCE_REBALANCE = True
            config.FORCE_REBALANCE_CONFIRMED = True
        setup_logging(config.LOG_DIR, "paper_test")
        logger = logging.getLogger("gen4.live")
        if args.fresh:
            config.FRESH_START = True
            for suffix in (".json", ".bak"):
                for prefix in ("portfolio_state_paper_test",
                                "runtime_state_paper_test"):
                    f = config.STATE_DIR / f"{prefix}{suffix}"
                    if f.exists():
                        f.unlink()
                        logger.info(f"[FRESH] Deleted {f.name}")
            logger.info("[FRESH] Clean start — state will sync from broker")
        logger.info(f"[PAPER_TEST_CYCLE] mode={args.cycle}")
        _run_live_with_restart(config)
    elif args.shadow_test:
        config.TRADING_MODE = "shadow_test"
        config.PAPER_TEST_CYCLE = args.cycle
        config.SHADOW_MODE = True
        if args.force_rebalance:
            config.PAPER_TEST_FORCE_REBALANCE = True
            config.FORCE_REBALANCE_CONFIRMED = True
        setup_logging(config.LOG_DIR, "shadow_test")
        logger = logging.getLogger("gen4.live")
        if args.fresh:
            config.FRESH_START = True
            for suffix in (".json", ".bak"):
                for prefix in ("portfolio_state_shadow_test",
                                "runtime_state_shadow_test"):
                    f = config.STATE_DIR / f"{prefix}{suffix}"
                    if f.exists():
                        f.unlink()
                        logger.info(f"[FRESH] Deleted {f.name}")
            logger.info("[FRESH] Clean start — state will sync from broker")
        _live_state_hash = _file_hash(config.STATE_DIR / "portfolio_state_paper.json")
        logger.info(f"[SHADOW_START] Live state hash: {_live_state_hash}")
        logger.info(f"[SHADOW_START] mode=shadow_test, force_rebalance={args.force_rebalance}")
        _run_live_with_restart(config)
        _post_hash = _file_hash(config.STATE_DIR / "portfolio_state_paper.json")
        if _live_state_hash == _post_hash:
            logger.info("[ISOLATION_OK] Live state unchanged after shadow test")
        else:
            logger.critical("[ISOLATION_BREACH] Live state modified during shadow test!")
    elif args.mock:
        setup_logging(config.LOG_DIR, "mock")
        run_mock(config)
    elif args.server:
        setup_logging(config.LOG_DIR, "server")
        _run_server(config)
    elif args.rebalance:
        print("[DEPRECATED] --rebalance is deprecated and unsafe.")
        print("  Use: --paper-test --force-rebalance --confirm-force-rebalance --fresh")
        print("   Or: --shadow-test --force-rebalance --confirm-force-rebalance --fresh")
        sys.exit(1)


def _run_server(config):
    """Start REST dashboard server (uvicorn).

    Background-friendly: can be launched via bat, nssm, or pythonw.
    Logs go to gen4_server_YYYYMMDD.log (daily rotation, 30-day retention).
    """
    import uvicorn

    logger = logging.getLogger("gen4.live")
    port = getattr(config, "DASHBOARD_PORT", 8080)

    logger.info("=" * 50)
    logger.info(f"  REST Dashboard Server starting on :{port}")
    logger.info("=" * 50)

    uvicorn.run(
        "web.app:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        access_log=False,  # uvicorn access log off (gen4 logger handles it)
    )


if __name__ == "__main__":
    main()
