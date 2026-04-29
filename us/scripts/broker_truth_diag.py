"""Broker-truth diagnostic — Alpaca live state vs engine state file.

Read-only. Does not modify state, does not place/cancel orders.
Prints a side-by-side table to distinguish real DD vs stale-cache DD.

Usage (from repo root):
    us/.venv/Scripts/python.exe us/scripts/broker_truth_diag.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "us"))

from data.alpaca_provider import AlpacaProvider  # noqa: E402

STATE_FILE = REPO / "us" / "state" / "portfolio_state_us_paper.json"
RUNTIME_FILE = REPO / "us" / "state" / "runtime_state_us_paper.json"


def _hours_ago(iso_ts: str) -> float:
    if not iso_ts:
        return float("inf")
    try:
        dt = datetime.fromisoformat(iso_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return float("inf")


def main() -> int:
    print("=" * 80)
    print("BROKER TRUTH DIAGNOSTIC — Alpaca live vs Engine state")
    print(f"  asof = {datetime.now(timezone.utc).isoformat()}")
    print("=" * 80)

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    runtime = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    engine_cash = float(state.get("cash", 0))
    engine_positions = state.get("positions", {})

    provider = AlpacaProvider()

    # 1. Account summary
    acct = provider.query_account_summary()
    if "error" in acct:
        print(f"\n[ERROR] account query failed: {acct}")
        return 1
    broker_equity = acct["equity"]
    broker_cash = acct["cash"]
    broker_last_equity = acct["last_equity"]

    # 2. Positions
    holdings = provider.query_account_holdings()
    holdings_by_sym = {h["code"]: h for h in holdings}

    # 3. Open orders
    open_orders = provider.query_open_orders() or []

    # 4. Engine equity = cash + sum(price × qty) using engine's stale cache
    engine_market_value = 0.0
    for sym, pos in engine_positions.items():
        engine_market_value += float(pos.get("current_price", 0)) * int(pos.get("quantity", 0))
    engine_equity = engine_cash + engine_market_value

    # 5. True equity from broker positions
    broker_market_value = sum(h["market_value"] for h in holdings)
    broker_equity_recomputed = broker_cash + broker_market_value

    print("\n--- ACCOUNT (Alpaca = TRUTH) ---")
    print(f"  equity              ${broker_equity:>14,.2f}")
    print(f"  last_equity (PrevD) ${broker_last_equity:>14,.2f}")
    daily_pnl = (broker_equity / broker_last_equity - 1) if broker_last_equity > 0 else 0
    print(f"  → daily P&L          {daily_pnl * 100:>+13.2f}%")
    print(f"  cash                ${broker_cash:>14,.2f}")
    print(f"  market_value (sum)  ${broker_market_value:>14,.2f}")

    print("\n--- ENGINE (state file) ---")
    print(f"  equity (computed)   ${engine_equity:>14,.2f}")
    print(f"  cash                ${engine_cash:>14,.2f}")
    print(f"  market_value (sum)  ${engine_market_value:>14,.2f}")
    print(f"  dd_label            {runtime.get('dd_label', 'n/a')}")
    print(f"  buy_blocked         {runtime.get('buy_blocked', 'n/a')}")
    print(f"  buy_scale           {runtime.get('buy_scale', 'n/a')}")

    diff = engine_equity - broker_equity
    print(f"\n--- DIFF ---")
    print(f"  engine - broker     ${diff:>+14,.2f}  ({diff / broker_equity * 100:+.2f}%)")

    # 6. Per-symbol price comparison
    print("\n--- PER-SYMBOL PRICE COMPARISON ---")
    print(f"  {'SYM':<6} {'qty':>5} {'engine_px':>10} {'broker_px':>10} {'diff%':>8} "
          f"{'last_price_at_age':>20}  status")
    print(f"  {'-'*6} {'-'*5} {'-'*10} {'-'*10} {'-'*8} {'-'*20}  {'-'*20}")

    stale_real: list[str] = []     # engine stale + broker also stale (could be halt, low-volume)
    stale_engine: list[str] = []   # engine stale + broker fresh
    fresh_match: list[str] = []
    diff_summary = {"real_drop": 0.0, "engine_lag": 0.0}

    for sym in sorted(engine_positions.keys()):
        ep = engine_positions[sym]
        ep_qty = int(ep.get("quantity", 0))
        ep_px = float(ep.get("current_price", 0))
        ep_last = ep.get("last_price_at", "")
        age_h = _hours_ago(ep_last)

        bh = holdings_by_sym.get(sym, {})
        bp_px = float(bh.get("cur_price", 0))
        bp_qty = int(bh.get("qty", 0))

        diff_pct = ((bp_px - ep_px) / ep_px * 100) if ep_px > 0 else 0.0

        engine_stale = age_h > 1.0  # 1h+ since last engine update
        broker_fresh = bp_px > 0 and abs(diff_pct) < 50  # broker has price, sane

        if engine_stale and broker_fresh:
            status = "ENGINE_LAG"
            stale_engine.append(sym)
            diff_summary["engine_lag"] += (bp_px - ep_px) * ep_qty
        elif engine_stale and not broker_fresh:
            status = "BOTH_STALE"
            stale_real.append(sym)
        elif diff_pct > 5 or diff_pct < -5:
            status = "PRICE_DIFF"
            stale_engine.append(sym)
            diff_summary["real_drop"] += (bp_px - ep_px) * ep_qty
        else:
            status = "OK"
            fresh_match.append(sym)

        age_str = f"{age_h:.1f}h" if age_h < 1000 else "n/a"
        print(f"  {sym:<6} {ep_qty:>5} {ep_px:>10.2f} {bp_px:>10.2f} {diff_pct:>+7.2f}% "
              f"{age_str:>20}  {status}")

    print(f"\n--- SUMMARY ---")
    print(f"  Total positions:        {len(engine_positions)}")
    print(f"  Fresh (engine matches): {len(fresh_match)}")
    print(f"  Engine LAG (stale eng): {len(stale_engine)}")
    print(f"  Both stale (rare):      {len(stale_real)}")
    print(f"  Engine missing $$:      ${diff_summary['engine_lag']:>+14,.2f}")
    print(f"  Real price diff $$:     ${diff_summary['real_drop']:>+14,.2f}")

    # 7. Open orders
    print(f"\n--- OPEN ORDERS (broker) ---")
    if not open_orders:
        print("  (none — STARTUP_BLOCKED reason should clear)")
    else:
        for o in open_orders:
            print(f"  {o.get('side','?'):<4} {o.get('code','?'):<6} "
                  f"x{o.get('qty','?'):<6} status={o.get('status','?')} "
                  f"submitted={o.get('submitted_at','?')}")

    # 8. Verdict
    print(f"\n--- VERDICT ---")
    if abs(diff) > 1000:
        if diff_summary['engine_lag'] > 500:
            print(f"  🚨 ENGINE STALE-CACHE BUG CONFIRMED")
            print(f"     {len(stale_engine)} positions have engine prices behind broker.")
            print(f"     Engine equity understates broker by ${-diff:,.2f}.")
            print(f"     DD_GUARD reading is FALSE (computed from stale engine prices).")
        else:
            print(f"  ⚠️  Real market move — broker confirms equity diff.")
    else:
        print(f"  ✅ Engine and broker equity match within $1k.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
