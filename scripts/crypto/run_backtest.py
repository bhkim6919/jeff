"""D4 PR #2 backtest CLI runner.

Usage::

    # Default: Momentum 12-1, KRW Top 100, 2018-01-01 ~ 2026-04-26
    python -X utf8 scripts/crypto/run_backtest.py

    # Custom range / N
    python -X utf8 scripts/crypto/run_backtest.py \
        --start 2020-01-01 --end 2026-04-26 --top-n 10

    # Custom evidence dir (verifier uses scratch)
    python -X utf8 scripts/crypto/run_backtest.py \
        --evidence-dir crypto/data/_verification/_d4_scratch/manual

Outputs (under ``--evidence-dir``):
    backtest_<run_id>.json                — summary + metrics + trade log + curve
    backtest_<run_id>_normal_equity.csv   — equity curve sidecar (Jeff E7=A)
    backtest_<run_id>_stress_equity.csv   — same, stress mode
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import CostConfig  # noqa: E402
from crypto.backtest.engine import (  # noqa: E402
    BacktestConfig,
    compute_run_id,
    run_dual,
)
from crypto.backtest.strategies.momentum_12_1 import Momentum12_1  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

DEFAULT_EVIDENCE_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--start", type=date.fromisoformat, default=date(2018, 1, 1))
    p.add_argument("--end", type=date.fromisoformat, default=date(2026, 4, 26))
    p.add_argument("--top-n", type=int, default=20)
    p.add_argument("--rebal-days", type=int, default=21)
    p.add_argument("--initial-cash-krw", type=float, default=100_000_000.0)
    p.add_argument("--strategy", type=str, default="momentum_12_1",
                   choices=["momentum_12_1"])
    p.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    p.add_argument("--universe-csv", type=Path, default=DEFAULT_TOP100_CSV)
    return p.parse_args(argv)


def _build_strategy(name: str):
    if name == "momentum_12_1":
        return Momentum12_1()
    raise ValueError(f"unknown strategy: {name}")


def run(argv=None) -> int:
    args = _parse_args(argv)
    print("=" * 78)
    print(f"[D4] backtest @ start={args.start} end={args.end} top_n={args.top_n}")
    print("=" * 78)

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection

    # Load listings + universe
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    universe = KRWStaticTop100.from_csv_and_listings(args.universe_csv, listings)

    config = BacktestConfig(
        strategy=_build_strategy(args.strategy),
        universe=universe,
        start_date=args.start,
        end_date=args.end,
        initial_cash_krw=args.initial_cash_krw,
        rebal_days=args.rebal_days,
        top_n=args.top_n,
        cost_config=CostConfig(),
    )
    run_id = compute_run_id(config)
    print(f"  run_id: {run_id}")
    print(f"  universe: {universe.name()} ({len(universe.all_pairs)} pairs)")

    payload = run_dual(config, connection_factory=connection)

    # Persist JSON (without inline equity curves to keep it small;
    # equity is sidecar CSV per Jeff E7=A).
    eq_normal = payload.pop("equity_curve_normal")
    eq_stress = payload.pop("equity_curve_stress")

    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.evidence_dir / f"backtest_{run_id}.json"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    print(f"[evidence] {json_path}")

    csv_n_path = args.evidence_dir / f"backtest_{run_id}_normal_equity.csv"
    _write_equity_csv(csv_n_path, eq_normal)
    csv_s_path = args.evidence_dir / f"backtest_{run_id}_stress_equity.csv"
    _write_equity_csv(csv_s_path, eq_stress)
    print(f"[equity-csv] {csv_n_path.name}")
    print(f"[equity-csv] {csv_s_path.name}")

    print()
    print("=== NORMAL metrics ===")
    for k, v in payload["results"]["normal"]["metrics"].items():
        print(f"  {k:<14} {v:+.6f}")
    print(f"  trades        {payload['results']['normal']['trade_count']}")
    print(f"  hash          {payload['results']['normal']['canonical_hash'][:16]}…")

    print()
    print("=== STRESS metrics ===")
    for k, v in payload["results"]["stress"]["metrics"].items():
        print(f"  {k:<14} {v:+.6f}")
    print(f"  trades        {payload['results']['stress']['trade_count']}")
    print(f"  hash          {payload['results']['stress']['canonical_hash'][:16]}…")

    print()
    print("=== STRESS - NORMAL diff ===")
    for k, v in payload["diff"].items():
        print(f"  {k:<20} {v:+.6f}")

    return 0


def _write_equity_csv(path: Path, curve: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "equity_krw"])
        for d, eq in curve:
            w.writerow([d, f"{eq:.2f}"])


if __name__ == "__main__":
    raise SystemExit(run())
