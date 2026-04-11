# -*- coding: utf-8 -*-
"""
runner.py — Lab Job Runner
============================
Orchestrates: universe load → data load → missing filter → engine → metrics.
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional

import pandas as pd

from .lab_config import (
    STRATEGY_CONFIGS, STRATEGY_GROUPS, MISSING_THRESHOLDS,
    DEFAULT_UNIVERSE,
)
from .engine import run_simulation
from .metrics import calc_metrics
from .job_store import (
    JobStore, LabJob, compute_config_hash, compute_data_snapshot_id, _now_iso,
)

logger = logging.getLogger("qtron.us.lab.runner")

_store = JobStore()
_running_lock = threading.Lock()


def get_store() -> JobStore:
    return _store


def filter_universe_for_strategy(strategy_name: str, tickers: List[str],
                                  close_dict: Dict[str, pd.Series]) -> tuple:
    """
    Filter tickers by strategy-specific missing data threshold.
    Returns (valid_tickers, excluded_list).
    """
    cfg = MISSING_THRESHOLDS.get(strategy_name, {"min_history": 252, "max_missing": 0.10})
    min_hist = cfg["min_history"]
    max_missing = cfg["max_missing"]

    valid, excluded = [], []
    for t in tickers:
        if t not in close_dict:
            excluded.append((t, 1.0))
            continue
        series = close_dict[t]
        actual = len(series.dropna())

        # Hard cutoff: less than 50% of min_history → always exclude
        if actual < min_hist * 0.5:
            excluded.append((t, 1 - actual / min_hist if min_hist > 0 else 1.0))
            continue

        ratio = 1 - (actual / min_hist) if min_hist > 0 else 0
        if ratio <= max_missing:
            valid.append(t)
        else:
            excluded.append((t, ratio))

    return valid, excluded


def run_lab_job(group: str, start_date: str, end_date: str,
                universe_name: str = DEFAULT_UNIVERSE,
                force: bool = False) -> LabJob:
    """
    Start a lab simulation job.
    Returns job (may be cached if identical config exists).
    """
    # Resolve strategies for group
    if group == "all":
        strategies = []
        for g_strategies in STRATEGY_GROUPS.values():
            strategies.extend(g_strategies)
    else:
        strategies = STRATEGY_GROUPS.get(group, [])
    if not strategies:
        raise ValueError(f"Unknown group: {group}. Available: ['all'] + {list(STRATEGY_GROUPS.keys())}")

    # Load universe snapshot
    from data.universe_builder import load_universe_snapshot, get_universe_snapshot_id
    tickers = load_universe_snapshot(universe_name)
    if not tickers:
        raise ValueError(f"No snapshot for universe: {universe_name}")

    universe_snapshot_id = get_universe_snapshot_id(universe_name)

    # Load data
    from data.db_provider import DbProviderUS
    db = DbProviderUS()
    close_dict = db.load_close_dict_research(min_history=20, symbols=tickers)
    ohlcv_dict = db.load_ohlcv_dict_research(min_history=20, symbols=tickers)

    if not close_dict:
        raise ValueError("No OHLCV data available for research universe")

    data_snapshot_id = compute_data_snapshot_id(close_dict, (start_date, end_date))

    # Create job (duplicate check inside)
    job = _store.create_job(
        group=group, strategies=strategies,
        universe_snapshot_id=universe_snapshot_id,
        data_snapshot_id=data_snapshot_id,
        start_date=start_date, end_date=end_date,
        force=force,
    )

    # If cached DONE, return immediately
    if job.status == "DONE":
        return job

    # Run in background thread
    def _run():
        try:
            job.status = "RUNNING"
            job.started_at = _now_iso()
            _store.update_job(job)

            results = {}
            for strat_name in strategies:
                strat_config = STRATEGY_CONFIGS.get(strat_name, {})

                # Strategy-specific missing filter
                valid_tickers, excluded = filter_universe_for_strategy(
                    strat_name, tickers, close_dict
                )
                logger.info(
                    f"[LAB] {strat_name}: {len(valid_tickers)} valid, "
                    f"{len(excluded)} excluded"
                )

                # Filter ohlcv_dict to valid tickers only
                filtered_ohlcv = {
                    s: df for s, df in ohlcv_dict.items() if s in set(valid_tickers)
                }

                # Load strategy
                strategy = _load_strategy(strat_name, strat_config)
                if not strategy:
                    logger.error(f"[LAB] Strategy not found: {strat_name}")
                    continue

                # Run simulation
                state = run_simulation(
                    strategy=strategy,
                    ohlcv_dict=filtered_ohlcv,
                    start_date=start_date,
                    end_date=end_date,
                    config=strat_config,
                )

                # Calculate metrics
                equity_series = pd.Series(
                    [e[1] for e in state.equity_history],
                    index=[e[0] for e in state.equity_history],
                )
                metrics = calc_metrics(
                    equity_curve=equity_series,
                    trades=state.trades,
                    total_days=state.day_count,
                    total_tickers=len(tickers),
                    excluded_tickers=len(excluded),
                    positions_history=state.positions_count_history,
                )

                results[strat_name] = {
                    "metrics": metrics,
                    "trade_count": len(state.trades),
                    "final_equity": round(equity_series.iloc[-1], 2) if len(equity_series) > 0 else 0,
                    "excluded_top10": excluded[:10],
                }

            job.results = results
            job.result_meta = {
                "universe_snapshot_id": universe_snapshot_id,
                "data_snapshot_id": data_snapshot_id,
                "universe_count": len(tickers),
                "data_count": len(close_dict),
            }
            job.status = "DONE"
            job.finished_at = _now_iso()
            _store.update_job(job)

            logger.info(f"[LAB] Job {job.job_id} completed: {list(results.keys())}")

        except Exception as e:
            job.status = "FAILED"
            job.error = str(e)
            job.finished_at = _now_iso()
            _store.update_job(job)
            logger.error(f"[LAB] Job {job.job_id} failed: {e}", exc_info=True)

    threading.Thread(target=_run, daemon=True, name=f"lab-{job.job_id}").start()
    return job


def _load_strategy(name: str, config: dict):
    """Dynamically load strategy class."""
    try:
        module = __import__(f"lab.strategies.{name}", fromlist=[name])
        # Convention: class name = CamelCase of strategy name
        class_name = "".join(w.capitalize() for w in name.split("_")) + "Strategy"
        cls = getattr(module, class_name, None)
        if cls:
            instance = cls()
            instance.name = name
            instance.config = config
            return instance
    except ImportError:
        pass
    except Exception as e:
        logger.error(f"[LAB] Failed to load strategy {name}: {e}")
    return None
