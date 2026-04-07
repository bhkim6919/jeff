"""Scenario Comparator - compare current vs conservative vs aggressive configs.

Wraps Gen4 backtester with config variations.
Results are read-only comparisons, never auto-applied.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from ..config import BASE_DIR


def compare_scenarios(scenarios: list[dict] | None = None) -> dict:
    """Run backtester with different configs and compare results.

    Args:
        scenarios: List of {name, config_overrides} dicts.
                   If None, uses default 3 scenarios.

    Returns dict with comparison table.
    """
    if scenarios is None:
        scenarios = _default_scenarios()

    results = {}
    for sc in scenarios:
        name = sc["name"]
        overrides = sc.get("config_overrides", {})

        try:
            metrics = _run_backtest_with_overrides(overrides)
            results[name] = metrics
        except Exception as e:
            results[name] = {"error": str(e)}

    return {
        "timestamp": datetime.now().isoformat(),
        "scenarios": results,
        "action_required": "MANUAL_REVIEW",
    }


def _default_scenarios() -> list[dict]:
    return [
        {
            "name": "current",
            "config_overrides": {},
        },
        {
            "name": "conservative",
            "config_overrides": {
                "TRAIL_PCT": 0.15,    # wider stop
                "N_STOCKS": 25,       # more diversified
            },
        },
        {
            "name": "aggressive",
            "config_overrides": {
                "TRAIL_PCT": 0.10,    # tighter stop
                "N_STOCKS": 15,       # more concentrated
            },
        },
    ]


def _run_backtest_with_overrides(overrides: dict) -> dict:
    """Run Gen4 backtester and parse results.

    Note: This is a placeholder. Full implementation would:
    1. Create temp config with overrides
    2. Run backtester subprocess
    3. Parse equity.csv output
    4. Return metrics dict

    For now, returns a stub indicating the scenario was requested.
    """
    # In production, this would call:
    # python -m backtest.backtester --start 2019-01-02 --end 2026-03-31
    # with modified config parameters

    return {
        "status": "NOT_IMPLEMENTED",
        "overrides": overrides,
        "note": ("Scenario comparison requires backtester integration. "
                 "Run manually: python -m backtest.backtester with modified config."),
    }
