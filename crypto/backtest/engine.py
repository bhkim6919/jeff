"""D4 backtest engine — daily walk + anchor-based rebal (Jeff D4 PR #2).

Hard guarantees enforced here (Jeff 보완 + E1~E7):

    1. Anchor-based rebal dates
       Computed as ``[start + i*rebal_days for i = 0,1,...]`` once at config
       time. Skipping a rebal (NaN price etc.) NEVER drifts subsequent dates.

    2. SELL → BUY trade order
       Each rebalance:
         a) compute target qty per pick (equal-weight from current equity)
         b) collect SELLs (close + reduce) sorted by pair asc
         c) execute SELLs first → cash freed
         d) collect BUYs (open + increase) sorted by pair asc
         e) execute BUYs

    3. Cost order: price → cost → portfolio mutation
       Portfolio.buy/sell internally calls calculate_cost on the notional
       BEFORE mutating cash/positions (KR Gen4 incident class).

    4. Deterministic sorting
       universe (Universe.active_pairs returns sorted) → strategy.select
       (returns sorted) → orders sorted by pair → trade_log entries sorted.

    5. No-lookahead (E1=C)
       signal asof = trade_date - 1 calendar day.

    6. NaN at rebal (E5=A)
       Pair skipped THIS rebal (treated as not-in-universe for sizing).
       Returns next rebal automatically.

    7. Warmup (E6=A)
       Engine pre-filters rebal dates: first rebal must have
       ``rebal_date - strategy.lookback_days >= data_min_date``.

Single-mode run: run_backtest(config, cost_mode) → BacktestResult.
Dual-mode run:  run_dual(config) → both modes under one deterministic run_id.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional

import pandas as pd

from crypto.backtest.cost_model import CostConfig, CostMode, cost_diff
from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.metrics import canonical_hash, compute_all
from crypto.backtest.portfolio import Portfolio, PortfolioInvariantError
from crypto.backtest.strategies.base import Strategy
from crypto.backtest.universe import Universe

logger = logging.getLogger(__name__)


# --- Config / Result ----------------------------------------------------


@dataclass(frozen=True)
class BacktestConfig:
    """Inputs for one backtest. Frozen so the run_id hash is stable."""
    strategy: Strategy
    universe: Universe
    start_date: date
    end_date: date
    initial_cash_krw: float = 100_000_000.0  # 1억 KRW default
    rebal_days: int = 21       # Jeff Q5=A: 21 crypto days
    top_n: int = 20            # Jeff E2=A
    cost_config: CostConfig = field(default_factory=CostConfig)


@dataclass
class TradeLogEntry:
    rebal_date: date
    side: str                  # "buy" or "sell"
    pair: str
    qty: float
    price_krw: float
    fee_krw: float
    slippage_krw: float

    def to_dict(self) -> dict:
        return {
            "rebal_date": self.rebal_date.isoformat(),
            "side": self.side,
            "pair": self.pair,
            "qty": self.qty,
            "price_krw": self.price_krw,
            "fee_krw": self.fee_krw,
            "slippage_krw": self.slippage_krw,
        }


@dataclass
class BacktestResult:
    cost_mode: CostMode
    metrics: dict[str, float]
    canonical_hash: str
    trade_log: list[TradeLogEntry]
    equity_curve: list[tuple[date, float]]   # CSV sidecar
    market_curve: list[tuple[date, float]]
    final_cash_krw: float
    final_equity_krw: float
    final_positions: dict[str, float]        # pair → qty
    rebal_dates_executed: list[date]
    rebal_dates_skipped: list[tuple[date, str]]  # (date, reason)


# --- Rebal date / warmup ------------------------------------------------


def compute_rebal_dates(
    start: date,
    end: date,
    rebal_days: int,
    *,
    first_rebal_min: Optional[date] = None,
) -> list[date]:
    """Anchor-based rebal grid (Jeff 보완 #1).

    Anchor = ``start``. Grid = ``[start + i*rebal_days for i = 0..]``
    capped at ``end``. Filters out any grid point earlier than
    ``first_rebal_min`` — the engine sets this to
    ``data_min_date + strategy.lookback_days`` so warmup is satisfied.

    Returns sorted ascending — the engine never reorders.
    """
    if rebal_days < 1:
        raise ValueError(f"rebal_days must be >= 1, got {rebal_days}")
    grid = []
    cur = start
    while cur <= end:
        grid.append(cur)
        cur = cur + timedelta(days=rebal_days)
    if first_rebal_min is not None:
        grid = [d for d in grid if d >= first_rebal_min]
    return grid


def _query_data_min_date(connection_factory: Callable[[], object]) -> Optional[date]:
    """Earliest OHLCV row across crypto_ohlcv. Used to set warmup origin."""
    with connection_factory() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(candle_dt_kst) FROM crypto_ohlcv")
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            v = row[0]
            return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])


# --- Signal asof / pricing ----------------------------------------------


def _signal_asof(trade_date: date) -> date:
    """E1=C: signal at D-1 close, trade at D close."""
    return trade_date - timedelta(days=1)


def _close_at(loader: OhlcvLoader, pair: str, on_date: date) -> Optional[float]:
    """Single-day close lookup; returns None if pair absent or NaN.

    For determinism the loader returns absent rows for non-trading days
    (rare in 24/7 crypto but possible — exchange downtime, etc.). We don't
    forward-fill; missing → None and the caller skips the pair.
    """
    df = loader.load_pair(pair, on_date, on_date)
    if df.empty:
        return None
    close = df["close"].iloc[-1]
    if close is None or (isinstance(close, float) and (math.isnan(close) or math.isinf(close))):
        return None
    val = float(close)
    return val if val > 0 else None


def _value_24h_at(loader: OhlcvLoader, pair: str, on_date: date) -> Optional[float]:
    """Daily KRW volume for STRESS slippage. None → caller pricing path
    handles fallback (we surface None and let calculate_cost decide)."""
    df = loader.load_pair(pair, on_date, on_date)
    if df.empty:
        return None
    val = df["value_krw"].iloc[-1]
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return float(val)


# --- Rebalance order generator ------------------------------------------


def _compute_rebal_orders(
    portfolio: Portfolio,
    picks: list[str],
    prices_today: dict[str, float],
    top_n: int,
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    """Return (sells, buys) — both sorted by pair ascending (Jeff 보완 #4).

    Smart rebal: SELL only the excess + closes; BUY only the new + shortfalls.
    Equal-weight target_qty = (equity / N) / price[pair].

    Pairs in the held portfolio but missing from prices_today → SELL all
    (treated as forced exit; engine emits a "skipped due to NaN" reason
    upstream).
    """
    held = dict(portfolio.positions)
    held_pairs = set(held.keys())
    pick_pairs = set(picks)

    # Equity uses prices_today for held pairs that have a quote, plus
    # last-known carry for any held pair that has NO quote today (we still
    # owe ourselves a sell at SOME price; we use cost basis as a deliberate
    # under-approximation so the equity isn't inflated).
    market_value = 0.0
    for pair, pos in held.items():
        if pair in prices_today:
            market_value += pos.qty * prices_today[pair]
        else:
            market_value += pos.cost_basis_krw  # conservative fallback
    equity = portfolio.cash_krw + market_value

    # Target qty per pick: equal weight of total equity.
    target_qty: dict[str, float] = {}
    n_priced = sum(1 for p in picks if p in prices_today)
    if n_priced == 0:
        return [], []
    per_slot_krw = equity / n_priced
    for pair in picks:
        price = prices_today.get(pair)
        if price is None or price <= 0:
            continue
        target_qty[pair] = per_slot_krw / price

    sells: list[tuple[str, float]] = []
    buys: list[tuple[str, float]] = []

    # Plan sells: any held pair NOT in picks → close; held pair in picks
    # but qty > target → reduce. Pairs without today's price → close at
    # next available rebal (we still emit the SELL at the next price the
    # engine has access to; here we punt by using cost_basis as price).
    for pair in sorted(held_pairs):
        cur_qty = held[pair].qty
        if pair not in pick_pairs:
            sells.append((pair, cur_qty))
            continue
        tgt = target_qty.get(pair, 0.0)
        if cur_qty > tgt:
            sells.append((pair, cur_qty - tgt))

    # Plan buys: any pick NOT held → open at full target; held pair where
    # qty < target → top up.
    for pair in sorted(pick_pairs):
        tgt = target_qty.get(pair, 0.0)
        cur_qty = held[pair].qty if pair in held_pairs else 0.0
        if tgt > cur_qty:
            buys.append((pair, tgt - cur_qty))

    return sells, buys


# --- Run (single mode) --------------------------------------------------


def run_backtest(
    config: BacktestConfig,
    cost_mode: CostMode,
    *,
    connection_factory: Callable[[], object],
) -> BacktestResult:
    """Run one backtest end-to-end for a single cost mode.

    Determinism (G6):
        - rebal_dates: deterministic from config
        - universe.active_pairs: returns sorted
        - strategy.select: returns sorted
        - orders sorted by pair
        - trade_log entries appended in execution order (SELL→BUY,
          pair-asc within each)
        - canonical_hash excludes timestamps
    """
    loader = OhlcvLoader(connection_factory)
    portfolio = Portfolio(
        cash_krw=config.initial_cash_krw,
        max_positions=config.top_n,
        cost_config=config.cost_config,
        cost_mode=cost_mode,
    )

    data_min = _query_data_min_date(connection_factory)
    if data_min is not None:
        warmup_min = data_min + timedelta(days=config.strategy.lookback_days)
    else:
        warmup_min = config.start_date + timedelta(days=config.strategy.lookback_days)
    rebal_dates = compute_rebal_dates(
        start=config.start_date,
        end=config.end_date,
        rebal_days=config.rebal_days,
        first_rebal_min=warmup_min,
    )

    trade_log: list[TradeLogEntry] = []
    equity_curve: list[tuple[date, float]] = []
    market_curve: list[tuple[date, float]] = []
    rebal_executed: list[date] = []
    rebal_skipped: list[tuple[date, str]] = []

    # Daily walk — mark-to-market every day, rebal on grid.
    rebal_set = set(rebal_dates)
    cur = config.start_date
    while cur <= config.end_date:
        # 1) Mark-to-market with today's close
        held_prices: dict[str, float] = {}
        for pair in sorted(portfolio.positions.keys()):
            px = _close_at(loader, pair, cur)
            if px is not None:
                held_prices[pair] = px
        # Equity uses available prices; for pairs with no quote today we
        # carry cost basis (conservative).
        eq = portfolio.cash_krw
        market_val = 0.0
        for pair, pos in portfolio.positions.items():
            if pair in held_prices:
                market_val += pos.qty * held_prices[pair]
            else:
                market_val += pos.cost_basis_krw
        eq += market_val
        equity_curve.append((cur, eq))
        market_curve.append((cur, market_val))

        # 2) Rebal trigger?
        if cur in rebal_set:
            try:
                _execute_rebal(
                    rebal_date=cur,
                    config=config,
                    portfolio=portfolio,
                    loader=loader,
                    trade_log=trade_log,
                )
                rebal_executed.append(cur)
            except _RebalSkipped as exc:
                rebal_skipped.append((cur, exc.reason))
                logger.warning("[engine] rebal %s skipped: %s", cur, exc.reason)

        cur = cur + timedelta(days=1)

    # Final marks (use last equity_curve entry)
    final_equity = equity_curve[-1][1] if equity_curve else config.initial_cash_krw
    metrics = compute_all(equity_curve, market_curve, trade_log)
    chash = canonical_hash(
        metrics,
        trade_count_value=len(trade_log),
        final_equity_krw=final_equity,
    )

    return BacktestResult(
        cost_mode=cost_mode,
        metrics=metrics,
        canonical_hash=chash,
        trade_log=trade_log,
        equity_curve=equity_curve,
        market_curve=market_curve,
        final_cash_krw=portfolio.cash_krw,
        final_equity_krw=final_equity,
        final_positions={p.pair: p.qty for p in portfolio.positions.values()},
        rebal_dates_executed=rebal_executed,
        rebal_dates_skipped=rebal_skipped,
    )


# --- Rebal execution ---------------------------------------------------


class _RebalSkipped(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _execute_rebal(
    *,
    rebal_date: date,
    config: BacktestConfig,
    portfolio: Portfolio,
    loader: OhlcvLoader,
    trade_log: list[TradeLogEntry],
) -> None:
    """Run one rebalance: signal asof D-1, trade at D close, SELL→BUY."""
    asof = _signal_asof(rebal_date)
    universe_pairs = config.universe.active_pairs(asof)
    if not universe_pairs:
        raise _RebalSkipped("empty universe at asof")

    picks = config.strategy.select(
        asof=asof,
        universe=universe_pairs,
        loader=loader,
        top_n=config.top_n,
    )
    if not picks:
        raise _RebalSkipped("strategy returned 0 picks")

    # Today's close prices — engine + held pairs (held pairs may not be in
    # picks but still need a price to sell).
    needed_pairs = sorted(set(picks) | set(portfolio.positions.keys()))
    prices_today: dict[str, float] = {}
    volumes_today: dict[str, float] = {}
    for pair in needed_pairs:
        px = _close_at(loader, pair, rebal_date)
        if px is None:
            continue
        prices_today[pair] = px
        vol = _value_24h_at(loader, pair, rebal_date)
        if vol is not None:
            volumes_today[pair] = vol

    sells, buys = _compute_rebal_orders(
        portfolio, picks, prices_today, config.top_n
    )

    # Execute SELLs first (Jeff 보완 #2).
    for pair, qty in sells:
        if pair not in prices_today:
            # We have to exit but have no price — skip this exit; the next
            # day's mark-to-market will surface a stale position. Engine
            # logs the situation via rebal_skipped only when the entire
            # rebal fails; partial-skip is just a logger.warning.
            logger.warning(
                "[engine] %s SELL %s qty=%.6f deferred — no price today",
                rebal_date, pair, qty,
            )
            continue
        cb = portfolio.sell(
            pair=pair,
            price_krw=prices_today[pair],
            qty=qty,
            volume_24h_krw=volumes_today.get(pair),
        )
        trade_log.append(
            TradeLogEntry(
                rebal_date=rebal_date,
                side="sell",
                pair=pair,
                qty=qty,
                price_krw=prices_today[pair],
                fee_krw=cb.fee_krw,
                slippage_krw=cb.slippage_krw,
            )
        )

    # Execute BUYs.
    for pair, qty in buys:
        if pair not in prices_today:
            continue
        # Cap qty to current cash availability — at this point we've SELLed
        # everything we planned, so cash is at its rebal-time max. If we
        # still can't afford full qty, scale down.
        notional = qty * prices_today[pair]
        # Conservative buffer for fee+slippage in STRESS mode (~3% over).
        max_cash_for_trade = portfolio.cash_krw / (1.0 + 0.03)
        if notional > max_cash_for_trade:
            qty = max(0.0, max_cash_for_trade / prices_today[pair])
            if qty <= 0:
                continue
        try:
            cb = portfolio.buy(
                pair=pair,
                price_krw=prices_today[pair],
                qty=qty,
                volume_24h_krw=volumes_today.get(pair),
            )
        except PortfolioInvariantError as exc:
            logger.warning(
                "[engine] %s BUY %s qty=%.6f rejected: %s",
                rebal_date, pair, qty, exc,
            )
            continue
        trade_log.append(
            TradeLogEntry(
                rebal_date=rebal_date,
                side="buy",
                pair=pair,
                qty=qty,
                price_krw=prices_today[pair],
                fee_krw=cb.fee_krw,
                slippage_krw=cb.slippage_krw,
            )
        )


# --- Run (dual mode + run_id) ------------------------------------------


def compute_run_id(config: BacktestConfig) -> str:
    """Deterministic run identifier (Jeff 보완 #4: same run_id for
    NORMAL+STRESS pair). Excludes timestamp; shape:

        ``{strategy}_{start}_{end}_{universe}_{N}_{cfg_sha8}``
    """
    import hashlib
    cfg_payload = (
        f"{config.cost_config.maker_fee_pct}|"
        f"{config.cost_config.taker_fee_pct}|"
        f"{config.cost_config.stress_fee_pct}|"
        f"{config.cost_config.stress_slippage_factor}|"
        f"{config.initial_cash_krw}|"
        f"{config.rebal_days}|"
        f"{config.top_n}"
    )
    sha8 = hashlib.sha256(cfg_payload.encode()).hexdigest()[:8]
    return (
        f"{config.strategy.name}_"
        f"{config.start_date.isoformat()}_"
        f"{config.end_date.isoformat()}_"
        f"{config.universe.name()}_"
        f"N{config.top_n}_"
        f"{sha8}"
    )


def run_dual(
    config: BacktestConfig,
    *,
    connection_factory: Callable[[], object],
) -> dict:
    """Run the same config under NORMAL and STRESS, return aggregated
    payload suitable for evidence JSON (Jeff Q2=C + 보완 #4)."""
    started_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_id = compute_run_id(config)

    res_normal = run_backtest(config, CostMode.NORMAL, connection_factory=connection_factory)
    res_stress = run_backtest(config, CostMode.STRESS, connection_factory=connection_factory)

    diff = {
        f"{k}_diff": res_stress.metrics[k] - res_normal.metrics[k]
        for k in res_normal.metrics
    }

    return {
        "run_id": run_id,
        "started_at_utc": started_at_utc,
        "completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "config": _config_summary(config),
        "results": {
            "normal": _result_summary(res_normal),
            "stress": _result_summary(res_stress),
        },
        "diff": diff,
        "canonical_hash_normal": res_normal.canonical_hash,
        "canonical_hash_stress": res_stress.canonical_hash,
        "trade_log_normal": [t.to_dict() for t in res_normal.trade_log],
        "trade_log_stress": [t.to_dict() for t in res_stress.trade_log],
        "equity_curve_normal": [(d.isoformat(), eq) for d, eq in res_normal.equity_curve],
        "equity_curve_stress": [(d.isoformat(), eq) for d, eq in res_stress.equity_curve],
    }


def _config_summary(config: BacktestConfig) -> dict:
    return {
        "strategy": config.strategy.name,
        "universe": config.universe.name(),
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "initial_cash_krw": config.initial_cash_krw,
        "rebal_days": config.rebal_days,
        "top_n": config.top_n,
        "cost_config": {
            "maker_fee_pct": config.cost_config.maker_fee_pct,
            "taker_fee_pct": config.cost_config.taker_fee_pct,
            "stress_fee_pct": config.cost_config.stress_fee_pct,
            "stress_slippage_factor": config.cost_config.stress_slippage_factor,
        },
    }


def _result_summary(r: BacktestResult) -> dict:
    return {
        "cost_mode": r.cost_mode.value,
        "metrics": r.metrics,
        "canonical_hash": r.canonical_hash,
        "final_cash_krw": r.final_cash_krw,
        "final_equity_krw": r.final_equity_krw,
        "trade_count": len(r.trade_log),
        "rebal_executed_count": len(r.rebal_dates_executed),
        "rebal_skipped_count": len(r.rebal_dates_skipped),
        "rebal_skipped_reasons": [{"date": d.isoformat(), "reason": r}
                                   for d, r in r.rebal_dates_skipped],
        "final_position_count": len(r.final_positions),
    }
