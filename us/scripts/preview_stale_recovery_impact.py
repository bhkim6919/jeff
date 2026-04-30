"""Pre-deploy diagnostic: predict trail-stop SELLs on stale-guard fix.

Read-only. Compares the engine state file (post-fix view) against
live Alpaca prices and reports which positions WOULD trigger a SELL
if the stale-guard recovery path landed right now.

Operator usage (run BEFORE merging the P0 stale-guard fix)::

    .venv/Scripts/python.exe us/scripts/preview_stale_recovery_impact.py

Output classifies each stale position into one of:
    OK          — fresh price within trail tolerance, no SELL.
    NEAR_TRAIL  — within 1-2% of trail trigger, watch.
    WOULD_SELL  — drop exceeds trail, SELL fires on first recovery tick.
    HWM_ADVANCE — fresh price > stale HWM, HWM advances (no SELL risk).
    BOTH_STALE  — broker also has no fresh quote (rare; halt or low-vol).

Output is INFO-only. The script does not place orders, modify state,
or modify any file. Safe to run while the tray is running.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "us"))

# Always read state + .env from the canonical install root, not the
# worktree. The worktree has its own ``us/`` source tree but never the
# live state nor the live API credentials.
INSTALL_ROOT = Path(r"C:\Q-TRON-32_ARCHIVE")
STATE_FILE   = INSTALL_ROOT / "us" / "state" / "portfolio_state_us_paper.json"
RUNTIME_FILE = INSTALL_ROOT / "us" / "state" / "runtime_state_us_paper.json"

# Load API creds from install root before importing AlpacaProvider.
try:
    from dotenv import load_dotenv  # noqa: E402
    load_dotenv(INSTALL_ROOT / "us" / ".env")
except ImportError:
    pass

from data.alpaca_provider import AlpacaProvider  # noqa: E402

TRAIL_RATIO = 0.12             # mirror config.US_TRAIL_RATIO
NEAR_TRAIL_BUFFER = 0.02       # within 2% of trigger = NEAR_TRAIL
STALE_THRESHOLD_HOURS = 1.0    # >1h gap = considered "stale"


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
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    positions = state.get("positions", {})

    print("=" * 92)
    print("STALE-RECOVERY IMPACT PREVIEW — read-only Alpaca cross-check")
    print(f"  asof    = {datetime.now(timezone.utc).isoformat()}")
    print(f"  trail_ratio = {TRAIL_RATIO:.0%}, near_trail buffer = {NEAR_TRAIL_BUFFER:.0%}")
    print("=" * 92)

    provider = AlpacaProvider()
    holdings = provider.query_account_holdings()
    holdings_by_sym = {h["code"]: h for h in holdings}

    rows: list[dict] = []
    counts = {"OK": 0, "NEAR_TRAIL": 0, "WOULD_SELL": 0,
              "HWM_ADVANCE": 0, "BOTH_STALE": 0, "FRESH": 0}

    for sym in sorted(positions.keys()):
        p = positions[sym]
        qty = int(p.get("quantity", 0))
        if qty <= 0:
            continue
        cached_px = float(p.get("current_price", 0))
        cached_hwm = float(p.get("high_watermark", 0))
        last_at = p.get("last_price_at", "")
        gap_h = _hours_ago(last_at)

        if gap_h <= STALE_THRESHOLD_HOURS:
            counts["FRESH"] += 1
            continue

        bh = holdings_by_sym.get(sym, {})
        broker_px = float(bh.get("cur_price", 0))

        if broker_px <= 0:
            classification = "BOTH_STALE"
        elif broker_px >= cached_hwm:
            classification = "HWM_ADVANCE"
        else:
            # New trail = max(cached_hwm, broker_px) * (1 - TRAIL_RATIO).
            new_hwm = max(cached_hwm, broker_px)
            trail_price = new_hwm * (1 - TRAIL_RATIO)
            margin = (broker_px - trail_price) / trail_price
            if broker_px <= trail_price:
                classification = "WOULD_SELL"
            elif margin <= NEAR_TRAIL_BUFFER:
                classification = "NEAR_TRAIL"
            else:
                classification = "OK"
        counts[classification] += 1

        rows.append({
            "sym":           sym,
            "qty":           qty,
            "cached_px":     cached_px,
            "broker_px":     broker_px,
            "cached_hwm":    cached_hwm,
            "gap_h":         gap_h,
            "classification": classification,
        })

    # Sort: WOULD_SELL first (most urgent), then NEAR_TRAIL, then others.
    order = {"WOULD_SELL": 0, "NEAR_TRAIL": 1, "BOTH_STALE": 2,
             "OK": 3, "HWM_ADVANCE": 4}
    rows.sort(key=lambda r: order.get(r["classification"], 9))

    print(f"\n  {'SYM':<6} {'qty':>5} {'cached_px':>10} {'broker_px':>10} {'cached_hwm':>10} "
          f"{'gap_h':>7}  classification")
    print(f"  {'-'*6} {'-'*5} {'-'*10} {'-'*10} {'-'*10} {'-'*7}  {'-'*15}")
    for r in rows:
        print(f"  {r['sym']:<6} {r['qty']:>5} {r['cached_px']:>10.2f} "
              f"{r['broker_px']:>10.2f} {r['cached_hwm']:>10.2f} "
              f"{r['gap_h']:>7.1f}  {r['classification']}")

    print(f"\n  --- Summary ---")
    for k in ("FRESH", "OK", "HWM_ADVANCE", "NEAR_TRAIL", "WOULD_SELL", "BOTH_STALE"):
        print(f"    {k:<12} {counts[k]:>3}")

    print()
    if counts["WOULD_SELL"] > 0:
        print(f"  ⚠️  {counts['WOULD_SELL']} position(s) WOULD trigger a SELL on first")
        print(f"     post-recovery tick. Review the WOULD_SELL rows above before")
        print(f"     merging the stale-guard fix. Acceptable IF those drops are")
        print(f"     real and trail-stop logic should have fired during the stale")
        print(f"     period anyway. Consider deploying after market close (ET")
        print(f"     16:00 / KST 05:00) so the SELL queue clears at next session.")
    elif counts["NEAR_TRAIL"] > 0:
        print(f"  ⚠️  {counts['NEAR_TRAIL']} position(s) NEAR trail trigger.")
        print(f"     Within {NEAR_TRAIL_BUFFER:.0%} of trail price — small adverse")
        print(f"     move would fire SELL.")
    else:
        print(f"  ✅  No positions will trigger SELL on recovery.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
