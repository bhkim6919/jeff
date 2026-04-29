"""D5 Survivor evaluation helper — 5y NORMAL backtests for SMA + ATR.

Runs the missing 5y NORMAL backtests for sma_50_200_trend and atr_breakout
to fill the survivor evaluation matrix.

Output: backup/reports/implementation/g10_logs/survivor_5y_sma_atr.json
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import CostConfig, CostMode  # noqa: E402
from crypto.backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from crypto.backtest.strategies.atr_breakout import ATRBreakout  # noqa: E402
from crypto.backtest.strategies.sma_50_200 import SMA50_200Trend  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

OUT_DIR = WORKTREE_ROOT.parent / "Q-TRON-32_ARCHIVE" / "backup" / "reports" / "implementation" / "g10_logs"
OUT_PATH = OUT_DIR / "survivor_5y_sma_atr.json"

START = date(2021, 1, 1)
END = date(2026, 4, 26)
TOP_N = 20

REQUIRED_KEYS = ("cagr", "mdd", "sharpe", "calmar", "trades", "exposure_pct")


def _build_universe():
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    return KRWStaticTop100.from_csv_and_listings(DEFAULT_TOP100_CSV, listings), connection


def _run(strategy, name: str) -> dict:
    print(f"\n[5y] {name} starting @ {datetime.now().strftime('%H:%M:%S')}")
    universe, conn_factory = _build_universe()
    cfg = BacktestConfig(
        strategy=strategy,
        universe=universe,
        start_date=START,
        end_date=END,
        initial_cash_krw=100_000_000.0,
        rebal_days=21,
        top_n=TOP_N,
        cost_config=CostConfig(),
    )
    res = run_backtest(cfg, CostMode.NORMAL, connection_factory=conn_factory)
    metrics = {k: float(res.metrics.get(k, 0)) for k in REQUIRED_KEYS}
    print(f"[5y] {name} done @ {datetime.now().strftime('%H:%M:%S')}")
    return {
        "name": name,
        "metrics": {k: round(v, 6) for k, v in metrics.items()},
        "trades": len(res.trade_log),
        "rebal_executed": len(res.rebal_dates_executed),
        "rebal_skipped": len(res.rebal_dates_skipped),
        "final_equity_krw": round(res.final_equity_krw, 2),
        "canonical_hash": res.canonical_hash,
    }


def main() -> int:
    print(f"=== Survivor 5y NORMAL helper @ {datetime.now(timezone.utc).isoformat()} ===")
    print(f"Window: {START} ~ {END}, top_n={TOP_N}")

    out = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "window": {"start": str(START), "end": str(END), "top_n": TOP_N},
        "results": {},
    }

    for cls, name in [(SMA50_200Trend(), "sma_50_200_trend"),
                      (ATRBreakout(), "atr_breakout")]:
        try:
            out["results"][name] = _run(cls, name)
        except Exception as e:
            print(f"[ERROR] {name}: {e}")
            out["results"][name] = {"error": str(e)}

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(
        json.dumps(out, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"\n=== Saved: {OUT_PATH} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
