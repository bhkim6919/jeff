"""
backtester.py — Gen4 Core backtester (stabilized)
===================================================
Uses strategy/scoring.py and strategy/trail_stop.py (SAME code as live).

Fixes applied (2026-03-23):
  1. entry_price stores market price (not gross_price) — no double-counting
  2. Preferred stock filter in load_ohlcv (code[-1] != '0' excluded)
  3. build_matrices: close-only ffill, open/high/volume NaN-preserved
  4. Universe filter (min_close, min_amount) applied in scoring loop
  5. Entry on T+1 open (not T close) via pending_buys
  6. high_watermark initialized to market price
  7. Output to backtest/results/gen4_core/

Usage:
    cd kr-legacy
    python -m backtest.backtester [--start 2019-01-02] [--end 2026-03-20]
"""
from __future__ import annotations
import sys, warnings, time, argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Gen4Config
from strategy.scoring import calc_volatility, calc_momentum
from strategy.trail_stop import check_trail_stop

warnings.filterwarnings("ignore")


def is_valid_common_stock(code: str) -> bool:
    """6-digit numeric code ending in 0 (common stock only)."""
    return len(code) == 6 and code.isdigit() and code[-1] == '0'


# ── Data Loading ─────────────────────────────────────────────────────────────
def load_ohlcv(ohlcv_dir: Path, min_history: int = 60) -> dict:
    """Load per-stock OHLCV CSVs. Common stocks only."""
    data = {}
    for f in sorted(ohlcv_dir.glob("*.csv")):
        code = f.stem
        if not is_valid_common_stock(code):
            continue
        try:
            df = pd.read_csv(f, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
            for c in ("open", "high", "low", "close", "volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            if len(df) >= min_history:
                data[code] = df
        except Exception:
            pass
    return data


def build_matrices(all_data: dict, dates: pd.Series):
    """Build aligned price matrices.
    Close: ffill for MTM continuity.
    Open/High/Low/Volume: NO ffill — NaN = not tradable."""
    d = {tk: df.set_index("date") for tk, df in all_data.items()}
    close = pd.DataFrame({tk: v["close"] for tk, v in d.items()}, index=dates).ffill()
    opn   = pd.DataFrame({tk: v["open"]  for tk, v in d.items()}, index=dates)
    high  = pd.DataFrame({tk: v["high"]  for tk, v in d.items()}, index=dates)
    low   = pd.DataFrame({tk: v["low"]   for tk, v in d.items()}, index=dates)
    vol   = pd.DataFrame({tk: v["volume"]for tk, v in d.items()}, index=dates).fillna(0)
    return close, opn, high, low, vol


# ── Universe Filter ──────────────────────────────────────────────────────────
def get_universe(close: pd.DataFrame, vol: pd.DataFrame, i: int,
                 min_close: int = 2000, min_amount: float = 2e9) -> set:
    """Filter tradeable universe at day index i."""
    if i < 20:
        return set()
    c = close.iloc[i]
    amt = (close.iloc[max(0, i-19):i+1] * vol.iloc[max(0, i-19):i+1]).mean()
    ok = (c >= min_close) & (amt >= min_amount) & (c > 0)
    return set(ok[ok].index.tolist())


# ── Metrics ──────────────────────────────────────────────────────────────────
def calc_metrics(eq: pd.Series, trades: list, idx_rets: pd.Series = None) -> dict:
    """Calculate performance metrics."""
    if len(eq) < 2:
        return {}
    r = eq.pct_change().dropna()
    tot = eq.iloc[-1] / eq.iloc[0] - 1
    ny = len(eq) / 252
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / ny) - 1 if ny > 0 else 0
    pk = eq.expanding().max()
    dd = (eq - pk) / pk
    mdd = float(dd.min())
    sharpe = float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0

    # Sortino
    dr = r[r < 0]
    sortino = float(r.mean() / dr.std() * np.sqrt(252)) if len(dr) > 0 and dr.std() > 0 else 0

    calmar = abs(cagr / mdd) if mdd != 0 else 0

    pnls = [t["pnl_pct"] for t in trades] if trades else []
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    wr = len(wins) / len(pnls) if pnls else 0
    pf = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 999
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0
    hd = [t.get("hold_days", 0) for t in trades]
    avg_hold = np.mean(hd) if hd else 0

    # Exit reason breakdown
    exit_reasons = {}
    for t in trades:
        reason = t.get("exit_reason", "UNKNOWN")
        if reason not in exit_reasons:
            exit_reasons[reason] = {"count": 0, "wins": 0, "total_pnl": 0}
        exit_reasons[reason]["count"] += 1
        exit_reasons[reason]["total_pnl"] += t["pnl_pct"]
        if t["pnl_pct"] > 0:
            exit_reasons[reason]["wins"] += 1

    return dict(
        total_return=tot, cagr=cagr, mdd=mdd, calmar=calmar,
        sharpe=sharpe, sortino=sortino,
        profit_factor=pf, win_rate=wr,
        avg_win=avg_win, avg_loss=avg_loss,
        n_trades=len(trades), avg_hold_days=avg_hold,
        exit_reasons=exit_reasons,
    )


# ── Core Backtest Engine ─────────────────────────────────────────────────────
def run_backtest(close, opn, high, low, vol, idx_close, dates,
                 start_i: int, end_i: int, config: Gen4Config) -> Tuple[pd.Series, list]:
    """
    Run Gen4 LowVol+Mom12-1 backtest.

    Uses scoring.py functions (calc_volatility, calc_momentum) — SHARED with live.
    Entry: signal on T close, buy at T+1 open.
    Exit: trail stop on T close, sell proceeds at T close.
    """
    cash = float(config.INITIAL_CASH)
    positions = {}   # tk -> {qty, entry_price, entry_idx, high_wm, buy_cost_total}
    pending_buys = []  # [{tk, target_idx, per_pos}] — fill at T+1 open
    trades = []
    equity_hist = {}

    last_rebal = -999
    _sample_count = 0

    for i in range(start_i, end_i + 1):
        dt = dates[i]

        # ── 0) Fill pending buys at today's open ──────────────────
        for pb in list(pending_buys):
            if i != pb["target_idx"]:
                continue
            tk = pb["tk"]
            if tk in positions:
                pending_buys.remove(pb)
                continue

            entry_price = float(opn[tk].iloc[i]) if not pd.isna(opn[tk].iloc[i]) else 0
            if entry_price <= 0:
                pending_buys.remove(pb)
                continue

            per_pos = pb["per_pos"]
            buy_cost_total = entry_price * (1 + config.BUY_COST)
            qty = int(min(per_pos, cash * 0.95) / buy_cost_total)
            if qty <= 0 or qty * buy_cost_total > cash:
                pending_buys.remove(pb)
                continue

            cash -= qty * buy_cost_total
            positions[tk] = dict(
                qty=qty,
                entry_price=entry_price,     # market price (no cost included)
                entry_idx=i,
                high_wm=entry_price,         # HWM = market price
                buy_cost_total=qty * entry_price * config.BUY_COST,
            )
            pending_buys.remove(pb)

            if _sample_count < 10:
                _sample_count += 1
                print(f"  [TIMING] #{_sample_count} signal={dates[i-1].date()} "
                      f"entry={dt.date()} open={entry_price:.0f} code={tk}")

        # Expire stale pendings
        pending_buys = [pb for pb in pending_buys if pb["target_idx"] >= i]

        # ── 1) Trail Stop (close-based) ───────────────────────────
        for tk in list(positions.keys()):
            pos = positions[tk]
            p = float(close[tk].iloc[i])
            if p <= 0 or pd.isna(p):
                continue
            triggered, new_hwm, _ = check_trail_stop(
                pos["high_wm"], p, config.TRAIL_PCT)
            pos["high_wm"] = new_hwm
            if triggered:
                net = pos["qty"] * p * (1 - config.SELL_COST)
                invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                pnl = (net - invested) / invested if invested > 0 else 0
                cash += net
                trades.append(dict(
                    ticker=tk,
                    entry_date=str(dates[pos["entry_idx"]].date()),
                    exit_date=str(dt.date()),
                    entry_price=pos["entry_price"],
                    exit_price=p,
                    pnl_pct=pnl,
                    pnl_amount=net - invested,
                    hold_days=i - pos["entry_idx"],
                    exit_reason="TRAIL",
                ))
                del positions[tk]

        # ── 2) Monthly Rebalance ──────────────────────────────────
        if i - last_rebal >= config.REBAL_DAYS:
            last_rebal = i

            # Universe filter FIRST
            universe = get_universe(close, vol, i,
                                    config.UNIV_MIN_CLOSE, config.UNIV_MIN_AMOUNT)

            # Score only universe stocks using SHARED scoring.py
            scored = []
            for tk in universe:
                series = close[tk].iloc[:i+1]
                if len(series) < max(config.VOL_LOOKBACK, config.MOM_LOOKBACK):
                    continue
                c_val = float(series.iloc[-1])
                if c_val <= 0 or pd.isna(c_val):
                    continue

                v = calc_volatility(series, config.VOL_LOOKBACK)
                if np.isnan(v):
                    continue
                m = calc_momentum(series, config.MOM_LOOKBACK, config.MOM_SKIP)
                if np.isnan(m):
                    continue
                scored.append({"tk": tk, "vol": v, "mom": m})

            if scored:
                sdf = pd.DataFrame(scored)
                vol_thresh = sdf["vol"].quantile(config.VOL_PERCENTILE)
                low_vol = sdf[sdf["vol"] <= vol_thresh]
                # Positive momentum only (must match factor_ranker.py)
                candidates = low_vol[low_vol["mom"] > 0]
                top = candidates.sort_values("mom", ascending=False).head(config.N_STOCKS)
                target_codes = set(top["tk"].tolist())

                # ── Sell non-targets (at today's close) ──────────
                for tk in list(positions.keys()):
                    if tk not in target_codes:
                        pos = positions[tk]
                        p = float(close[tk].iloc[i])
                        if p <= 0 or pd.isna(p):
                            p = pos["entry_price"]
                        net = pos["qty"] * p * (1 - config.SELL_COST)
                        invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
                        pnl = (net - invested) / invested if invested > 0 else 0
                        cash += net
                        trades.append(dict(
                            ticker=tk,
                            entry_date=str(dates[pos["entry_idx"]].date()),
                            exit_date=str(dt.date()),
                            entry_price=pos["entry_price"],
                            exit_price=p,
                            pnl_pct=pnl,
                            pnl_amount=net - invested,
                            hold_days=i - pos["entry_idx"],
                            exit_reason="REBALANCE",
                        ))
                        del positions[tk]

                # ── Queue buys for T+1 open ──────────────────────
                pv_held = sum(pos["qty"] * float(close[c].iloc[i])
                              for c, pos in positions.items()
                              if float(close[c].iloc[i]) > 0)
                total_eq = cash + pv_held
                new_codes = [c for c in target_codes if c not in positions]
                slots = config.N_STOCKS - len(positions) - len(pending_buys)

                if new_codes and slots > 0 and i + 1 <= end_i:
                    per_pos = total_eq / config.N_STOCKS
                    for tk in new_codes[:slots]:
                        pending_buys.append({
                            "tk": tk,
                            "target_idx": i + 1,
                            "per_pos": per_pos,
                        })

        # ── Equity snapshot (filled positions only) ────────────────
        pv = cash
        for tk, pos in positions.items():
            c = float(close[tk].iloc[i])
            if c > 0 and not pd.isna(c):
                pv += pos["qty"] * c
        equity_hist[dt] = pv

    # Close remaining (end of test)
    for tk, pos in list(positions.items()):
        p = float(close[tk].iloc[end_i])
        if p > 0 and not pd.isna(p):
            net = pos["qty"] * p * (1 - config.SELL_COST)
            invested = pos["qty"] * pos["entry_price"] + pos["buy_cost_total"]
            pnl = (net - invested) / invested if invested > 0 else 0
            trades.append(dict(
                ticker=tk,
                entry_date=str(dates[pos["entry_idx"]].date()),
                exit_date=str(dates[end_i].date()),
                entry_price=pos["entry_price"],
                exit_price=p,
                pnl_pct=pnl,
                pnl_amount=net - invested,
                hold_days=end_i - pos["entry_idx"],
                exit_reason="EOD",
            ))

    return pd.Series(equity_hist).sort_index(), trades


# ── Report ───────────────────────────────────────────────────────────────────
def print_results(m: dict, label: str = "Gen4 Core"):
    """Print metrics summary."""
    print(f"\n{'='*70}")
    print(f"  {label} Backtest Results")
    print(f"{'='*70}")
    print(f"  Total Return : {m['total_return']*100:+.1f}%")
    print(f"  CAGR         : {m['cagr']*100:+.1f}%")
    print(f"  MDD          : {m['mdd']*100:.1f}%")
    print(f"  Sharpe       : {m['sharpe']:.2f}")
    print(f"  Sortino      : {m['sortino']:.2f}")
    print(f"  Calmar       : {m['calmar']:.2f}")
    print(f"  Profit Factor: {m['profit_factor']:.2f}")
    print(f"  Win Rate     : {m['win_rate']*100:.1f}%")
    print(f"  Avg Win      : {m['avg_win']*100:+.1f}%")
    print(f"  Avg Loss     : {m['avg_loss']*100:+.1f}%")
    print(f"  Trades       : {m['n_trades']}")
    print(f"  Avg Hold     : {m['avg_hold_days']:.1f} days")

    if m.get("exit_reasons"):
        print(f"\n  Exit Reasons:")
        for reason, info in sorted(m["exit_reasons"].items()):
            wr = info["wins"] / info["count"] * 100 if info["count"] > 0 else 0
            avg_pnl = info["total_pnl"] / info["count"] * 100 if info["count"] > 0 else 0
            print(f"    {reason:15s}  {info['count']:4d} trades  "
                  f"WR={wr:.0f}%  AvgPnL={avg_pnl:+.1f}%")
    print(f"{'='*70}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gen4 Core Backtester")
    parser.add_argument("--start", default="2019-01-02", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="", help="End date (YYYY-MM-DD, default: today)")
    parser.add_argument("--ohlcv-dir", default="", help="Override OHLCV directory path")
    parser.add_argument("--label", default="", help="Result label suffix (e.g. 'expanded')")
    args = parser.parse_args()

    config = Gen4Config()

    # --ohlcv-dir override
    ohlcv_dir = Path(args.ohlcv_dir) if args.ohlcv_dir else config.OHLCV_DIR

    # --end default: today
    if not args.end:
        args.end = datetime.today().strftime("%Y-%m-%d")

    label = args.label or ohlcv_dir.name  # e.g. "ohlcv_expanded" or "ohlcv"

    print("=" * 70)
    print("  Gen4 Core Backtester (SHARED scoring.py + trail_stop.py)")
    print(f"  OHLCV : {ohlcv_dir}")
    print(f"  Period: {args.start} ~ {args.end}")
    print("=" * 70)

    t0 = time.time()
    print(f"\n[1/3] Loading OHLCV from {ohlcv_dir}...")
    all_data = load_ohlcv(ohlcv_dir, config.UNIV_MIN_HISTORY)

    idx_df = pd.read_csv(config.INDEX_FILE)
    # Handle both column naming conventions
    date_col = "index" if "index" in idx_df.columns else "date"
    rename = {date_col: "date"}
    for s, d_ in [("Open","open"),("High","high"),("Low","low"),("Close","close"),("Volume","volume")]:
        if s in idx_df.columns:
            rename[s] = d_
    idx_df = idx_df.rename(columns=rename)
    idx_df["date"] = pd.to_datetime(idx_df["date"], errors="coerce")
    idx_df = idx_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for c in ["open","high","low","close","volume"]:
        if c in idx_df.columns:
            idx_df[c] = pd.to_numeric(idx_df[c], errors="coerce").fillna(0)
    dates = idx_df["date"]
    print(f"  {len(all_data)} stocks, {len(dates)} dates")

    close, opn, high, low, vol = build_matrices(all_data, dates)
    idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
    idx_rets = idx_close.pct_change().fillna(0)

    sd = pd.Timestamp(args.start)
    ed = pd.Timestamp(args.end)
    si = int((dates >= sd).values.argmax())
    ei = int(len(dates) - 1 - (dates <= ed).values[::-1].argmax())
    print(f"  Period: {dates[si].date()} ~ {dates[ei].date()} ({ei-si+1} days)")

    print(f"\n[2/3] Running backtest...")
    eq, trades = run_backtest(close, opn, high, low, vol, idx_close, dates,
                              si, ei, config)

    m = calc_metrics(eq, trades, idx_rets)
    elapsed = time.time() - t0

    print_results(m)

    kospi_ret = float(idx_close.iloc[ei] / idx_close.iloc[si] - 1)
    print(f"\n  KOSPI Buy&Hold: {kospi_ret*100:+.1f}%")
    print(f"  Elapsed: {elapsed:.0f}s")

    # Save results to strategy folder (label-separated)
    out_dir = config.BASE_DIR.parent / "backtest" / "results" / f"gen4_{label}"
    out_dir.mkdir(parents=True, exist_ok=True)

    eq_df = eq.reset_index()
    eq_df.columns = ["date", "equity"]
    eq_df.to_csv(out_dir / "equity.csv", index=False)

    pd.DataFrame(trades).to_csv(out_dir / "trades.csv",
                                index=False, encoding="utf-8-sig")

    print(f"\n[3/3] Saved to {out_dir}/")


if __name__ == "__main__":
    main()
