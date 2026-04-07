"""Generate synthetic 30-day test data for Phase 3 validation.

Creates:
  - report/output_test/equity_log.csv (30 days)
  - report/output_test/trades.csv (buys + sells)
  - report/output_test/close_log.csv (exit records)
  - report/output_test/reconcile_log.csv (a few recon events)

Run:
    cd Gen04
    python -m advisor.tests.generate_test_data
"""
from __future__ import annotations

import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "report" / "output_test"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)  # deterministic

# ── Trading days (skip weekends) ──
def trading_days(start: str, n: int) -> list[str]:
    dt = datetime.strptime(start, "%Y-%m-%d")
    days = []
    while len(days) < n:
        if dt.weekday() < 5:
            days.append(dt.strftime("%Y-%m-%d"))
        dt += timedelta(days=1)
    return days

DAYS = trading_days("2026-03-01", 30)

# ── Stock pool ──
STOCKS = [
    "005930", "000660", "005380", "000270", "035420",
    "051910", "006400", "009150", "028260", "032830",
    "005830", "055550", "086790", "105560", "316140",
    "078930", "161390", "001120", "175330", "004690",
]

# ── Generate equity_log.csv ──
def gen_equity_log():
    rows = []
    equity = 500_000_000
    cash = 50_000_000
    peak = equity

    risk_modes = ["NORMAL"] * 20 + ["DD_CAUTION"] * 5 + ["MONTHLY_BLOCKED"] * 5
    random.shuffle(risk_modes)

    for i, day in enumerate(DAYS):
        daily_pnl = random.gauss(0.001, 0.015)
        equity = int(equity * (1 + daily_pnl))
        peak = max(peak, equity)
        monthly_dd = (equity / peak - 1)
        cash = int(equity * 0.1)
        n_pos = random.randint(15, 20)
        risk = risk_modes[i]
        recon = random.choice([0, 0, 0, 0, 1, 2, 15])
        rebal = "Y" if i == 0 else "N"

        rows.append({
            "date": day,
            "equity": f"{equity:.2f}",
            "cash": f"{cash:.2f}",
            "n_positions": str(n_pos),
            "daily_pnl_pct": f"{daily_pnl:.6f}",
            "monthly_dd_pct": f"{monthly_dd:.6f}",
            "risk_mode": risk,
            "rebalance_executed": rebal,
            "price_fail_count": "0",
            "reconcile_corrections": str(recon),
            "monitor_only": "N",
            "kospi_close": str(random.randint(4800, 5500)),
            "kosdaq_close": "",
            "regime": random.choice(["BULL", "BULL", "SIDEWAYS"]),
            "kospi_ma200": "3928",
            "breadth": "",
        })

    path = OUTPUT_DIR / "equity_log.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  equity_log.csv: {len(rows)} rows -> {path}")

# ── Generate trades.csv ──
def gen_trades():
    rows = []
    trade_id = 0
    for day in DAYS:
        n_trades = random.randint(0, 4)
        for _ in range(n_trades):
            trade_id += 1
            code = random.choice(STOCKS)
            side = random.choice(["BUY", "BUY", "SELL"])
            qty = random.randint(10, 500)
            price = random.randint(10000, 500000)
            cost = int(price * qty * 0.00115)
            slip = round(random.uniform(0.001, 0.05), 4)
            event_id = f"{day.replace('-', '')}_{trade_id:06d}_{code}_{side}"

            rows.append({
                "date": day,
                "code": code,
                "side": side,
                "quantity": str(qty),
                "price": f"{price:.2f}",
                "cost": f"{cost:.2f}",
                "slippage_pct": f"{slip:.4f}",
                "mode": "PAPER_TEST",
                "event_id": event_id,
            })

    path = OUTPUT_DIR / "trades.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  trades.csv: {len(rows)} rows -> {path}")

# ── Generate close_log.csv ──
def gen_close_log():
    rows = []
    reasons = ["TRAIL_STOP"] * 10 + ["REBALANCE"] * 5 + ["UNIVERSE_EXIT"] * 2

    for day in DAYS:
        n_closes = random.randint(0, 2)
        for _ in range(n_closes):
            code = random.choice(STOCKS)
            reason = random.choice(reasons)
            entry_price = random.randint(10000, 400000)
            hold = random.randint(3, 60)
            pnl_pct = random.gauss(0.01, 0.08)
            exit_price = int(entry_price * (1 + pnl_pct))
            pnl_amount = int((exit_price - entry_price) * random.randint(10, 200))

            entry_dt = datetime.strptime(day, "%Y-%m-%d") - timedelta(days=hold)
            entry_date = entry_dt.strftime("%Y-%m-%d")

            rows.append({
                "date": day,
                "code": code,
                "exit_reason": reason,
                "quantity": str(random.randint(10, 200)),
                "entry_price": f"{entry_price:.2f}",
                "exit_price": f"{exit_price:.2f}",
                "entry_date": entry_date,
                "hold_days": str(hold),
                "pnl_pct": f"{pnl_pct:.6f}",
                "pnl_amount": f"{pnl_amount:.2f}",
                "mode": "PAPER_TEST",
                "event_id": f"{day.replace('-', '')}_{code}_CLOSE",
                "entry_rank": str(random.randint(1, 20)),
                "score_mom": f"{random.uniform(-0.5, 2.0):.4f}",
                "max_hwm_pct": f"{random.uniform(0.01, 0.15):.4f}",
            })

    path = OUTPUT_DIR / "close_log.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  close_log.csv: {len(rows)} rows -> {path}")

# ── Generate reconcile_log.csv ──
def gen_reconcile_log():
    rows = []
    for day in DAYS[:5]:  # Only first few days have recon events
        if random.random() < 0.3:
            rows.append({
                "date": day,
                "time": "08:33:00",
                "code": random.choice(STOCKS),
                "diff_type": "BROKER_ONLY",
                "engine_qty": "0",
                "broker_qty": str(random.randint(10, 100)),
                "engine_avg": "0",
                "broker_avg": str(random.randint(10000, 300000)),
                "resolution": "ADDED",
            })

    path = OUTPUT_DIR / "reconcile_log.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  reconcile_log.csv: {len(rows)} rows -> {path}")


if __name__ == "__main__":
    print("Generating synthetic test data for Phase 3...")
    gen_equity_log()
    gen_trades()
    gen_close_log()
    gen_reconcile_log()
    print("Done.")
