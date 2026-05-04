"""risk_management.py — DD Trim + Emergency Rebalance (extracted from main.py)"""
from __future__ import annotations
import logging
from datetime import date, datetime

from lifecycle.utils import _count_trading_days

logger = logging.getLogger(__name__)


def _execute_dd_trim(portfolio, trim_ratio, executor, config,
                      trade_logger, mode_str, logger,
                      guard=None, level: str = "") -> int:
    """Trim all positions by trim_ratio during DD drawdown.
    Called once per rebalance — same-day duplicate trim prevented by caller.

    Returns: int — number of positions actually trimmed (0 = no-op).

    PR 5 (G5-a fix): If `guard` and `level` are passed, mark_trim_executed
    is called HERE instead of by the caller, conditional on trimmed > 0.
    Previously the caller (rebalance_phase.py) called mark_trim_executed
    unconditionally on `risk_action["trim_ratio"] > 0`, which incorrectly
    blocked same-day re-trim when all positions had qty * trim_ratio < 1
    (no actual trades). Result: DD_TRIM marked done despite zero fills.
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

    # PR 5 / G5-a: only mark executed if at least one position trimmed.
    # Without this, re-trim on the same day is incorrectly blocked even
    # when 0 actual fills happened.
    if guard is not None and trimmed > 0 and level:
        try:
            guard.mark_trim_executed(level)
        except Exception as e:
            logger.warning(f"[DD_TRIM_MARK_FAIL] level={level}: {e}")
    elif guard is not None and trimmed == 0 and level:
        logger.info(
            f"[DD_TRIM_NO_FILL] level={level} trim_ratio={trim_ratio:.0%} — "
            f"no positions had qty * trim_ratio >= 1; mark_trim_executed SKIPPED, "
            f"same-day re-trim remains allowed"
        )

    return trimmed


# ── Emergency Rebalance (Ver.02: Strategy A) ─────────────────────────────────
def _check_emergency_rebalance(portfolio, config, executor, provider,
                                trade_logger, state_mgr, guard,
                                mode_str, logger,
                                session_rebalance_executed=False) -> bool:
    """
    Gen4 Ver.02: Emergency Rebalance on BEAR regime transition.

    Triggers when regime transitions to BEAR (from BULL or SIDE).
    Trims portfolio to BEAR target exposure (40%) by selling weakest positions.

    Safeguards:
      1. same-day scheduled rebalance guard (skip if already rebalanced)
      2. cooldown (N days after last emergency)
      3. same-regime duplicate guard (only on transition, not repeated BEAR days)
      4. defensive trim only (target exposure, not full rebalance)
      5. full state logging

    Returns: True if emergency rebalance was executed.
    """
    from strategy.regime_detector import (
        calc_regime, calc_breadth_from_prices, get_target_exposure, EXPOSURE_MAP)
    from report.reporter import make_event_id

    today_str = date.today().strftime("%Y%m%d")

    # ── Guard 1: Skip if scheduled rebalance already ran today ────
    if session_rebalance_executed:
        logger.info("[EMERGENCY_REBAL] Skipped: scheduled rebalance already executed today")
        return False

    # ── Guard 2: Cooldown check ───────────────────────────────────
    runtime = state_mgr.load_runtime()
    last_emergency = runtime.get("last_emergency_rebal_date", "")
    if last_emergency:
        try:
            last_dt = datetime.strptime(last_emergency, "%Y%m%d").date()
            days_since = _count_trading_days(last_dt, date.today(), config)
            if days_since < config.EMERGENCY_REBAL_COOLDOWN:
                logger.info(f"[EMERGENCY_REBAL] Skipped: cooldown "
                           f"({days_since}/{config.EMERGENCY_REBAL_COOLDOWN} trading days)")
                return False
        except (ValueError, TypeError):
            pass  # parse error, proceed

    # ── Compute current regime ────────────────────────────────────
    # Get KOSPI data for regime calculation
    kospi_close = provider.get_kospi_close() if hasattr(provider, 'get_kospi_close') else 0
    kospi_ma200 = provider.get_kospi_ma200() if hasattr(provider, 'get_kospi_ma200') else 0

    # If provider doesn't have these methods, try loading from INDEX_FILE
    if kospi_close <= 0 or kospi_ma200 <= 0:
        try:
            import pandas as pd
            idx_df = pd.read_csv(config.INDEX_FILE)
            date_col = "index" if "index" in idx_df.columns else "date"
            idx_df = idx_df.rename(columns={date_col: "date"})
            idx_df["date"] = pd.to_datetime(idx_df["date"], errors="coerce")
            idx_df = idx_df.dropna(subset=["date"]).sort_values("date")
            close_col = "Close" if "Close" in idx_df.columns else "close"
            idx_df[close_col] = pd.to_numeric(idx_df[close_col], errors="coerce")
            idx_series = idx_df[close_col].dropna()
            if len(idx_series) >= 200:
                kospi_close = float(idx_series.iloc[-1])
                kospi_ma200 = float(idx_series.iloc[-200:].mean())
                logger.info(f"[REGIME] KOSPI from file: close={kospi_close:.0f}, "
                           f"MA200={kospi_ma200:.0f}")
        except Exception as e:
            logger.warning(f"[REGIME] Failed to load KOSPI data: {e}")
            return False

    if kospi_close <= 0 or kospi_ma200 <= 0:
        logger.warning("[EMERGENCY_REBAL] Cannot compute regime: no KOSPI data")
        return False

    # Breadth: use portfolio positions + available universe prices
    # Simplified: use portfolio holdings as proxy (not full market)
    stock_closes = {}
    stock_ma200s = {}
    for code in portfolio.positions:
        price = executor.get_live_price(code) if executor else 0
        if price > 0:
            stock_closes[code] = price
    # For breadth, we'd need MA200 for each stock - use KOSPI ratio as proxy
    breadth = 0.5  # default
    if kospi_close > 0 and kospi_ma200 > 0:
        # Simple heuristic: if KOSPI is well above/below MA200, breadth correlates
        kospi_ratio = kospi_close / kospi_ma200
        if kospi_ratio > 1.05:
            breadth = 0.65  # likely healthy
        elif kospi_ratio < 0.95:
            breadth = 0.35  # likely weak
        else:
            breadth = 0.50  # neutral

    current_regime = calc_regime(kospi_close, kospi_ma200, breadth)

    # ── Guard 3: Only trigger on transition TO BEAR ───────────────
    prev_regime = runtime.get("last_regime", "SIDE")
    logger.info(f"[REGIME] prev={prev_regime}, current={current_regime}, "
               f"KOSPI={kospi_close:.0f}, MA200={kospi_ma200:.0f}, breadth={breadth:.2f}")

    # Always save current regime for next session
    runtime["last_regime"] = current_regime
    runtime["last_regime_date"] = today_str
    state_mgr.save_runtime(runtime)

    if current_regime != "BEAR" or prev_regime == "BEAR":
        # Not a BEAR transition (either not BEAR, or already was BEAR)
        return False

    # ── BEAR Transition Detected! ─────────────────────────────────
    logger.warning("=" * 60)
    logger.warning("  EMERGENCY REBALANCE TRIGGERED")
    logger.warning(f"  Regime transition: {prev_regime} -> BEAR")
    logger.warning("=" * 60)

    if not portfolio.positions:
        logger.info("[EMERGENCY_REBAL] No positions to trim")
        return False

    # ── Calculate exposure and trim target ────────────────────────
    equity = portfolio.get_current_equity()
    target_exposure_ratio = get_target_exposure("BEAR")  # 0.4
    target_exposure = equity * target_exposure_ratio

    current_invested = sum(
        pos.quantity * (pos.current_price if pos.current_price > 0 else pos.avg_price)
        for pos in portfolio.positions.values()
    )

    tolerance = config.EMERGENCY_EXPOSURE_TOLERANCE  # 5%
    if current_invested <= target_exposure * (1 + tolerance):
        logger.info(f"[EMERGENCY_REBAL] Exposure OK: invested={current_invested:,.0f} "
                   f"<= target={target_exposure:,.0f} (tolerance={tolerance:.0%})")
        # Still record the transition
        runtime["last_emergency_rebal_date"] = today_str
        runtime["last_emergency_rebal_reason"] = "BEAR_TRANSITION_NO_TRIM_NEEDED"
        state_mgr.save_runtime(runtime)
        return False

    trim_target = current_invested - target_exposure
    logger.warning(f"[EMERGENCY_REBAL] Exposure: invested={current_invested:,.0f}, "
                  f"target={target_exposure:,.0f}, need_to_trim={trim_target:,.0f}")

    # ── Trim: sell weakest-performing positions first ─────────────
    pos_rankings = []
    for code, pos in portfolio.positions.items():
        price = executor.get_live_price(code) if executor else 0
        if price <= 0:
            price = pos.current_price if pos.current_price > 0 else pos.avg_price
        if price > 0:
            pnl_pct = (price - pos.avg_price) / pos.avg_price
            pos_value = pos.quantity * price
            pos_rankings.append((code, pnl_pct, price, pos_value))

    pos_rankings.sort(key=lambda x: x[1])  # worst P&L first

    trimmed_count = 0
    trimmed_total = 0
    trimmed_symbols = []

    for code, pnl_pct, price, pos_value in pos_rankings:
        if trimmed_total >= trim_target:
            break

        pos = portfolio.positions.get(code)
        if not pos:
            continue

        remaining_to_trim = trim_target - trimmed_total

        if pos_value <= remaining_to_trim * 1.5:
            # Full position sell
            eid = make_event_id(code, "EMERGENCY_A")
            result = executor.execute_sell(code, pos.quantity, "EMERGENCY_REBAL")
            if not result.get("error"):
                fill_price = result.get("exec_price") or price
                exec_qty = result.get("exec_qty", pos.quantity)
                trade = portfolio.remove_position(code, fill_price, config.SELL_COST)
                if trade:
                    trade["exit_reason"] = "EMERGENCY_A"
                    trade_logger.log_close(trade, "EMERGENCY_A", mode_str,
                                          event_id=eid)
                trimmed_total += pos_value
                trimmed_count += 1
                trimmed_symbols.append(code)
                logger.warning(f"[EMERGENCY_SELL] {code}: FULL sell, "
                             f"pnl={pnl_pct*100:+.1f}%, value={pos_value:,.0f}")
            else:
                logger.error(f"[EMERGENCY_SELL] {code}: sell failed: {result['error']}")
        else:
            # Partial sell
            sell_qty = max(1, int(remaining_to_trim / price))
            sell_qty = min(sell_qty, pos.quantity - 1)
            if sell_qty > 0:
                eid = make_event_id(code, "EMERGENCY_A_PARTIAL")
                result = executor.execute_sell(code, sell_qty, "EMERGENCY_REBAL")
                if not result.get("error"):
                    fill_price = result.get("exec_price") or price
                    exec_qty = result.get("exec_qty", sell_qty)
                    trade = portfolio.remove_position(code, fill_price, config.SELL_COST,
                                                     qty=exec_qty)
                    if trade:
                        trade["exit_reason"] = "EMERGENCY_A_PARTIAL"
                        trade_logger.log_close(trade, "EMERGENCY_A_PARTIAL", mode_str,
                                              event_id=eid)
                    trimmed_total += sell_qty * price
                    trimmed_count += 1
                    trimmed_symbols.append(f"{code}(partial)")
                    logger.warning(f"[EMERGENCY_SELL] {code}: PARTIAL sell {sell_qty}qty, "
                                 f"pnl={pnl_pct*100:+.1f}%")
                else:
                    logger.error(f"[EMERGENCY_SELL] {code}: partial sell failed: "
                               f"{result['error']}")

    # ── State log (modification point 5) ──────────────────────────
    new_invested = current_invested - trimmed_total
    logger.warning("=" * 60)
    logger.warning(f"  EMERGENCY REBALANCE COMPLETE")
    logger.warning(f"  Regime: {prev_regime} -> BEAR")
    logger.warning(f"  Exposure: {current_invested:,.0f} -> {new_invested:,.0f} "
                  f"(target: {target_exposure:,.0f})")
    logger.warning(f"  Trimmed: {trimmed_count} positions, {trimmed_total:,.0f} KRW")
    logger.warning(f"  Symbols: {', '.join(trimmed_symbols)}")
    logger.warning(f"  Positions remaining: {len(portfolio.positions)}")
    logger.warning("=" * 60)

    # Update runtime state
    runtime = state_mgr.load_runtime()
    runtime["last_emergency_rebal_date"] = today_str
    runtime["last_emergency_rebal_reason"] = "BEAR_TRANSITION"
    runtime["last_emergency_rebal_detail"] = {
        "prev_regime": prev_regime,
        "current_regime": current_regime,
        "exposure_before": round(current_invested),
        "exposure_after": round(new_invested),
        "target_exposure": round(target_exposure),
        "trimmed_count": trimmed_count,
        "trimmed_total": round(trimmed_total),
        "trimmed_symbols": trimmed_symbols,
        "rebalance_reason": "emergency",
    }
    state_mgr.save_runtime(runtime)

    return trimmed_count > 0
