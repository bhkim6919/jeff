"""
run_lab.py — Strategy Lab CLI entry point
==========================================
Usage:
    cd kr
    python -m lab.run_lab [--start 2026-03-01] [--end 2026-04-08]
    python -m lab.run_lab --group rebal
    python -m lab.run_lab --group event --experimental-same-day
    python -m lab.run_lab --strategies momentum_base,lowvol_momentum
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

# Ensure kr is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.lab_config import LabConfig, STRATEGY_CONFIGS, STRATEGY_GROUPS
from lab.runner import run_lab
from lab.report import generate_report


def main():
    parser = argparse.ArgumentParser(description="Strategy Lab — Multi-Strategy Comparison")
    parser.add_argument("--start", default="2026-03-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="", help="End date (default: latest)")
    parser.add_argument("--group", default="", choices=["", "rebal", "event", "macro", "regime"],
                        help="Strategy group filter")
    parser.add_argument("--strategies", default="", help="Comma-separated strategy names")
    parser.add_argument("--capital", type=int, default=100_000_000, help="Initial capital")
    parser.add_argument("--n-stocks", type=int, default=20, help="Default max positions")
    parser.add_argument("--experimental-same-day", action="store_true",
                        help="Enable SAME_DAY_CLOSE for event strategies")
    parser.add_argument("--mode", default="portfolio", choices=["portfolio", "pure_signal"],
                        help="Lab mode")
    parser.add_argument("--no-charts", action="store_true", help="Skip chart generation")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Build config
    config = LabConfig(
        START_DATE=args.start,
        END_DATE=args.end,
        GROUP=args.group,
        INITIAL_CASH=args.capital,
        N_STOCKS=args.n_stocks,
        EXPERIMENTAL_SAME_DAY=args.experimental_same_day,
        LAB_MODE=args.mode,
    )

    if args.strategies:
        config.STRATEGIES = args.strategies.split(",")

    # Print config summary
    active = config.get_active_strategies()
    active_groups = config.get_active_groups()
    print("=" * 70)
    print("  Strategy Lab - Multi-Strategy Comparison Simulator")
    print("=" * 70)
    print(f"  Mode     : {config.LAB_MODE}")
    print(f"  Period   : {config.START_DATE} ~ {config.END_DATE or 'latest'}")
    print(f"  Capital  : {config.INITIAL_CASH:,}")
    print(f"  Groups   : {list(active_groups.keys())}")
    print(f"  Strategies: {active}")
    if config.EXPERIMENTAL_SAME_DAY:
        print(f"  ⚠ EXPERIMENTAL: event strategies use SAME_DAY_CLOSE")
    print("=" * 70)

    # Run
    result = run_lab(config)

    # Report
    if result.get("results"):
        generate_report(result, no_charts=args.no_charts)

    print(f"\n  Done. Output: {result.get('output_dir', 'N/A')}")


if __name__ == "__main__":
    main()
