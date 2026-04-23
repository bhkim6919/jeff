"""
factor_ranker.py — Stock selection via LowVol + Momentum ranking
=================================================================
1. Filter to low-volatility universe (bottom VOL_PERCENTILE)
2. Keep only positive momentum stocks
3. Rank by momentum descending, take top N_STOCKS

Matches backtest_gen4_core.py lines 191-206 exactly.
"""
from __future__ import annotations
import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from strategy.scoring import score_universe

logger = logging.getLogger("gen4.ranker")


def select_top_n(scores_df: pd.DataFrame,
                 vol_percentile: float = 0.30,
                 n_stocks: int = 20) -> List[str]:
    """
    Select top N stocks from scored universe.

    Args:
        scores_df: DataFrame with columns [ticker, vol_12m, mom_12_1]
        vol_percentile: Low-volatility cutoff (bottom N%).
        n_stocks: Number of stocks to select.

    Returns:
        List of ticker codes, ordered by momentum (highest first).
    """
    df = scores_df.dropna(subset=["vol_12m", "mom_12_1"]).copy()
    if df.empty:
        logger.warning("No valid scores — empty selection")
        return []

    # Low-vol filter: bottom percentile
    vol_thresh = df["vol_12m"].quantile(vol_percentile)
    low_vol = df[df["vol_12m"] <= vol_thresh]

    # Positive momentum only
    candidates = low_vol[low_vol["mom_12_1"] > 0]

    if candidates.empty:
        logger.warning("No positive-momentum stocks in low-vol universe")
        return []

    # Rank by momentum descending, take top N
    ranked = candidates.sort_values("mom_12_1", ascending=False)
    selected = ranked["ticker"].head(n_stocks).tolist()

    logger.info(f"Selected {len(selected)}/{n_stocks} stocks "
                f"(vol_thresh={vol_thresh:.4f}, candidates={len(candidates)})")
    return selected


def build_target_portfolio(close_dict: Dict[str, pd.Series],
                           config,
                           target_date: Optional[date] = None) -> dict:
    """
    Build target portfolio for a given date (batch mode output).

    Returns:
        {
            "date": "YYYYMMDD",
            "target_tickers": [...],
            "scores": {ticker: {"vol_12m": ..., "mom_12_1": ...}, ...},
            "vol_threshold": float,
            "universe_size": int,
        }
    """
    scores_df = score_universe(
        close_dict,
        vol_lookback=config.VOL_LOOKBACK,
        mom_lookback=config.MOM_LOOKBACK,
        mom_skip=config.MOM_SKIP,
    )

    target = select_top_n(scores_df, config.VOL_PERCENTILE, config.N_STOCKS)

    # Build score lookup for selected
    score_lookup = {}
    for _, row in scores_df.iterrows():
        if row["ticker"] in target:
            score_lookup[row["ticker"]] = {
                "vol_12m": round(row["vol_12m"], 6),
                "mom_12_1": round(row["mom_12_1"], 6),
            }

    valid = scores_df.dropna(subset=["vol_12m", "mom_12_1"])
    vol_thresh = float(valid["vol_12m"].quantile(config.VOL_PERCENTILE)) if not valid.empty else 0

    # Use actual last trading date from data, not today() (avoids weekend/holiday mislabel)
    if target_date:
        dt = target_date
    else:
        last_dates = [s.index.max() for s in close_dict.values()
                      if hasattr(s.index, 'max') and len(s) > 0]
        if last_dates:
            latest = max(last_dates)
            dt = latest.date() if hasattr(latest, 'date') else date.today()
        else:
            dt = date.today()
    return {
        "date": dt.strftime("%Y%m%d"),
        "target_tickers": target,
        "scores": score_lookup,
        "vol_threshold": round(vol_thresh, 6),
        "universe_size": len(valid),
    }


def save_target_portfolio(target: dict, signals_dir: Path) -> Path:
    """Save target portfolio JSON to signals directory."""
    signals_dir.mkdir(parents=True, exist_ok=True)
    path = signals_dir / f"target_portfolio_{target['date']}.json"
    path.write_text(json.dumps(target, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Saved target portfolio: {path}")
    return path


def load_target_portfolio(signals_dir: Path,
                          target_date: Optional[str] = None) -> Optional[dict]:
    """Load most recent target portfolio.

    Hot-patch (2026-04-22): PG fallback when JSON is missing. Temporary measure
    until Phase 6 (Storage Retirement) formally migrates reads to PG. Rationale:
    signals/ dir was silently cleared 2026-04-21 → dashboard preview broken
    despite PG audit being intact. PG fallback unblocks dashboard without
    regenerating the JSON file (which would reinforce JSON dependence).
    """
    if target_date:
        path = signals_dir / f"target_portfolio_{target_date}.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    else:
        files = sorted(signals_dir.glob("target_portfolio_*.json"), reverse=True)
        if files:
            return json.loads(files[0].read_text(encoding="utf-8"))

    try:
        from data.db_provider import DbProvider
        pg_date = ""
        if target_date:
            pg_date = (f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
                       if len(target_date) == 8 else target_date)
        target = DbProvider().get_target_portfolio(pg_date)
        if target:
            logger.warning(
                "[TARGET_SOURCE_PG_FALLBACK] JSON missing — loaded from PG "
                "date=%s tickers=%d (Phase 6 TODO: migrate to PG-first)",
                target.get("date", "?"), len(target.get("target_tickers", [])))
            return target
    except Exception as e:
        logger.warning("[TARGET_PG_FALLBACK_FAIL] %s", e, exc_info=True)
    return None
