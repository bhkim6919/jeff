"""
runner.py — Lab 메인 시뮬레이션 루프
======================================
그룹별 실행, atomic status write, heartbeat.
"""
from __future__ import annotations
import copy
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from lab.lab_config import (
    LabConfig, FillTiming, STRATEGY_GROUPS,
    DISABLE_CROSS_GROUP_COMPARISON, EXPECTED_EXPOSURE,
)
from lab.lab_errors import LabDataError, LabStrategyError
from lab.snapshot import build_snapshot, DailySnapshot
from lab.universe import build_universe
from lab.engine import (
    StrategyState, process_pending_fills, process_exit_policy,
    process_sell_signals, process_buy_signals, record_equity,
    _close_position,
)
from lab.strategies.base import BaseStrategy

logger = logging.getLogger("lab.runner")


# ── Atomic write (P0-1) ─────────────────────────────────────────
def atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic JSON write: tmp → fsync → replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    # backup
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak")
        try:
            if bak.exists():
                bak.unlink()
            path.rename(bak)
        except OSError:
            pass
    os.replace(str(tmp), str(path))


def safe_read_json(path: Path) -> Optional[dict]:
    """JSON 읽기: parse 실패 → .bak fallback."""
    for p in [path, path.with_suffix(path.suffix + ".bak")]:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
    return None


# ── Data Loading (reuse backtester patterns) ─────────────────────
def _is_valid_common_stock(code: str) -> bool:
    return len(code) == 6 and code.isdigit() and code[-1] == '0'


def load_ohlcv(ohlcv_dir: Path, min_history: int = 60) -> dict:
    """Load per-stock OHLCV CSVs. Common stocks only."""
    data = {}
    for f in sorted(ohlcv_dir.glob("*.csv")):
        code = f.stem
        if not _is_valid_common_stock(code):
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
    """Build aligned price matrices."""
    d = {tk: df.set_index("date") for tk, df in all_data.items()}
    close = pd.DataFrame({tk: v["close"] for tk, v in d.items()}, index=dates).ffill()
    opn = pd.DataFrame({tk: v["open"] for tk, v in d.items()}, index=dates)
    high = pd.DataFrame({tk: v["high"] for tk, v in d.items()}, index=dates)
    low = pd.DataFrame({tk: v["low"] for tk, v in d.items()}, index=dates)
    vol = pd.DataFrame({tk: v["volume"] for tk, v in d.items()}, index=dates).fillna(0)
    return close, opn, high, low, vol


def load_sector_map(path: Path) -> dict:
    """Load sector_map.json."""
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def load_fundamental(fund_dir: Path, target_date: pd.Timestamp) -> Optional[pd.DataFrame]:
    """Load most recent fundamental snapshot <= target_date."""
    if not fund_dir.exists():
        return None
    files = sorted(fund_dir.glob("fundamental_*.csv"))
    best = None
    for f in files:
        try:
            dt_str = f.stem.split("_")[1]
            dt = pd.Timestamp(dt_str)
            if dt <= target_date:
                best = f
        except (IndexError, ValueError):
            continue
    if best:
        try:
            return pd.read_csv(best, encoding="utf-8-sig")
        except Exception:
            pass
    return None


# ── Strategy Factory ─────────────────────────────────────────────
def create_strategy(name: str) -> BaseStrategy:
    """전략 이름으로 인스턴스 생성."""
    if name == "momentum_base":
        from lab.strategies.momentum_base import MomentumBase
        return MomentumBase()
    elif name == "lowvol_momentum":
        from lab.strategies.lowvol_momentum import LowVolMomentum
        return LowVolMomentum()
    elif name == "breakout_trend":
        from lab.strategies.breakout_trend import BreakoutTrend
        return BreakoutTrend()
    elif name == "mean_reversion":
        from lab.strategies.mean_reversion import MeanReversionStrategy
        return MeanReversionStrategy()
    elif name == "quality_factor":
        from lab.strategies.quality_factor import QualityFactor
        return QualityFactor()
    elif name == "sector_rotation":
        from lab.strategies.sector_rotation import SectorRotation
        return SectorRotation()
    elif name == "vol_regime":
        from lab.strategies.vol_regime import VolRegime
        return VolRegime()
    elif name == "liquidity_signal":
        from lab.strategies.liquidity_signal import LiquiditySignalStrategy
        return LiquiditySignalStrategy()
    elif name == "hybrid_qscore":
        from lab.strategies.hybrid_qscore import HybridQScore
        return HybridQScore()
    # ── B군 HA 전략 ─────────────────────────────────────────
    elif name == "momentum_base_ha":
        from lab.strategies.momentum_base_ha import MomentumBaseHA
        return MomentumBaseHA()
    elif name == "lowvol_momentum_ha":
        from lab.strategies.lowvol_momentum_ha import LowVolMomentumHA
        return LowVolMomentumHA()
    elif name == "quality_factor_ha":
        from lab.strategies.quality_factor_ha import QualityFactorHA
        return QualityFactorHA()
    elif name == "hybrid_qscore_ha":
        from lab.strategies.hybrid_qscore_ha import HybridQScoreHA
        return HybridQScoreHA()
    elif name == "breakout_trend_ha":
        from lab.strategies.breakout_trend_ha import BreakoutTrendHA
        return BreakoutTrendHA()
    elif name == "mean_reversion_ha":
        from lab.strategies.mean_reversion_ha import MeanReversionHA
        return MeanReversionHA()
    elif name == "liquidity_signal_ha":
        from lab.strategies.liquidity_signal_ha import LiquiditySignalHA
        return LiquiditySignalHA()
    elif name == "sector_rotation_ha":
        from lab.strategies.sector_rotation_ha import SectorRotationHA
        return SectorRotationHA()
    elif name == "vol_regime_ha":
        from lab.strategies.vol_regime_ha import VolRegimeHA
        return VolRegimeHA()
    else:
        raise ValueError(f"Unknown strategy: {name}")


# ── Main Runner ──────────────────────────────────────────────────
def run_lab(config: LabConfig) -> Dict[str, Dict]:
    """메인 Lab 실행. 그룹별 순차 실행.

    Returns:
        {strategy_name: {"equity": pd.Series, "trades": list, "state": StrategyState}}
    """
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = config.OUTPUT_DIR / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"

    logger.info(f"[LAB_START] run_id={run_id}")
    logger.info(f"[LAB_MODE] {config.LAB_MODE}")

    # Status init
    status = {
        "run_id": run_id,
        "state": "running",
        "mode": config.LAB_MODE,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "progress_pct": 0.0,
        "current_day": "",
        "current_strategy": "",
        "error": None,
    }
    atomic_write_json(status_path, status)

    # ── 1. Load data (DB first, CSV fallback) ──────────────────
    t0 = time.time()
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        print(f"[LAB] Loading from PostgreSQL...")
        close, opn, high, low, vol, dates = db.build_matrices()
        idx_df = db.get_kospi_index()
        dates = idx_df["date"]
        idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
        sector_map = db.get_sector_map()
        print(f"  {len(close.columns)} stocks, {len(dates)} dates ({time.time()-t0:.1f}s) [DB]")
    except Exception as e:
        logger.warning(f"[LAB] DB failed ({e}), falling back to CSV")
        print(f"[LAB] Loading OHLCV from {config.OHLCV_DIR}...")
        all_data = load_ohlcv(config.OHLCV_DIR, config.UNIV_MIN_HISTORY)
        idx_df = pd.read_csv(config.INDEX_FILE)
        date_col = "index" if "index" in idx_df.columns else "date"
        rename = {date_col: "date"}
        for s, d_ in [("Open", "open"), ("High", "high"), ("Low", "low"),
                       ("Close", "close"), ("Volume", "volume")]:
            if s in idx_df.columns:
                rename[s] = d_
        idx_df = idx_df.rename(columns=rename)
        idx_df["date"] = pd.to_datetime(idx_df["date"], errors="coerce")
        idx_df = idx_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        for c in ["open", "high", "low", "close", "volume"]:
            if c in idx_df.columns:
                idx_df[c] = pd.to_numeric(idx_df[c], errors="coerce").fillna(0)
        dates = idx_df["date"]
        close, opn, high, low, vol = build_matrices(all_data, dates)
        idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
        sector_map = load_sector_map(config.SECTOR_MAP_FILE)
        print(f"  {len(all_data)} stocks, {len(dates)} dates ({time.time()-t0:.1f}s) [CSV]")

    # ── 2. Date range ────────────────────────────────────────────
    sd = pd.Timestamp(config.START_DATE) if config.START_DATE else dates.iloc[config.LOOKBACK_DAYS]
    ed = pd.Timestamp(config.END_DATE) if config.END_DATE else dates.iloc[-1]

    si = int((dates >= sd).values.argmax())
    ei = int(len(dates) - 1 - (dates <= ed).values[::-1].argmax())

    # Ensure warmup
    if si < config.LOOKBACK_DAYS:
        si = config.LOOKBACK_DAYS
    if ei <= si:
        raise LabDataError(f"Invalid date range: {dates[si].date()} ~ {dates[ei].date()}")

    print(f"  Simulation: {dates[si].date()} ~ {dates[ei].date()} ({ei-si+1} days)")

    # ── 3. Initialize strategies ─────────────────────────────────
    active_groups = config.get_active_groups()
    all_results = {}
    total_strats = sum(len(v) for v in active_groups.values())
    total_days = ei - si + 1
    completed_work = 0

    for group_name, strat_names in active_groups.items():
        print(f"\n[LAB_GROUP] {group_name}: {strat_names}")

        # Create states
        states: Dict[str, StrategyState] = {}
        for sname in sorted(strat_names):
            try:
                strategy = create_strategy(sname)
            except Exception as e:
                logger.error(f"[LAB_STRATEGY_INIT_ERROR] {sname}: {e}")
                continue

            states[sname] = StrategyState(
                name=sname,
                cash=float(config.INITIAL_CASH),
                strategy=strategy,
            )

        if not states:
            continue

        # Determine fill timing
        fill_timing = FillTiming.NEXT_OPEN
        if config.EXPERIMENTAL_SAME_DAY and group_name == "event":
            fill_timing = FillTiming.SAME_DAY_CLOSE
            logger.warning(f"[LAB_EXPERIMENTAL] event group using SAME_DAY_CLOSE")

        # ── 4. Daily loop ────────────────────────────────────────
        for i in range(si, ei + 1):
            dt = dates[i]

            # Universe (1회)
            univ = build_universe(close, vol, i,
                                  config.UNIV_MIN_CLOSE, config.UNIV_MIN_AMOUNT)

            # Fundamental (캐시)
            fund_df = load_fundamental(config.FUNDAMENTAL_DIR, dt)

            # Snapshot (1회, frozen)
            snapshot = build_snapshot(
                i, dates, close, opn, high, low, vol,
                univ, sector_map, idx_close, fund_df)

            # Per-strategy processing
            for sname in sorted(states.keys()):
                state = states[sname]
                try:
                    # [4] Fill pending
                    process_pending_fills(state, snapshot, config)

                    # [5] Exit policy
                    process_exit_policy(state, snapshot, config)

                    # [6] Generate signals
                    pos_copy = copy.deepcopy(state.positions)
                    signals = state.strategy.generate_signals(snapshot, pos_copy)

                    # [7] SELL signals
                    process_sell_signals(state, signals, snapshot, config)

                    # [8] BUY signals
                    process_buy_signals(state, signals, snapshot, config,
                                        fill_timing, ei)

                    # [9] Record equity
                    record_equity(state, snapshot)

                except LabDataError as e:
                    logger.error(f"[LAB_DATA_ERROR] {sname} day={dt.date()}: {e}")
                    record_equity(state, snapshot)
                except LabStrategyError as e:
                    logger.error(f"[LAB_STRATEGY_ERROR] {sname} day={dt.date()}: {e}")
                    record_equity(state, snapshot)
                except Exception as e:
                    logger.error(f"[LAB_UNKNOWN_ERROR] {sname} day={dt.date()}: {e}")
                    record_equity(state, snapshot)

            # Progress update (heartbeat)
            completed_work += 1
            progress = completed_work / (total_days * len(active_groups)) * 100
            if i % 5 == 0 or i == ei:
                status["progress_pct"] = round(progress, 1)
                status["current_day"] = str(dt.date())
                status["current_strategy"] = group_name
                atomic_write_json(status_path, status)

        # ── 5. Force close remaining ─────────────────────────────
        for sname, state in states.items():
            final_snapshot = build_snapshot(
                ei, dates, close, opn, high, low, vol,
                frozenset(), sector_map, idx_close)
            for tk in list(state.positions.keys()):
                _close_position(state, tk, final_snapshot, config, "EOD")
            record_equity(state, final_snapshot)

        # Collect results
        for sname, state in states.items():
            eq = pd.Series(state.equity_history).sort_index()
            all_results[sname] = {
                "equity": eq,
                "trades": state.trades,
                "state": state,
                "group": group_name,
            }

    # ── 6. Finalize ──────────────────────────────────────────────
    elapsed = time.time() - t0
    status["state"] = "completed"
    status["finished_at"] = datetime.now().isoformat()
    status["progress_pct"] = 100.0
    atomic_write_json(status_path, status)

    print(f"\n[LAB_COMPLETE] {len(all_results)} strategies, {elapsed:.1f}s")
    print(f"  Output: {output_dir}")

    # Exposure warnings (final state cash vs final equity)
    for sname, result in all_results.items():
        eq = result["equity"]
        state = result["state"]
        group = result["group"]
        if len(eq) > 1:
            final_eq = eq.iloc[-1]
            if final_eq > 0:
                band = EXPECTED_EXPOSURE.get(group, (0, 1))
                # exposure approximated from turnover
                avg_eq = eq.mean()
                if avg_eq > 0 and state.total_buy_amount > 0:
                    approx_exp = state.total_buy_amount / (avg_eq * len(eq))
                    if approx_exp < band[0] * 0.1 or approx_exp > band[1] * 2:
                        logger.warning(
                            f"[LAB_EXPOSURE_WARN] {sname}: approx_exposure "
                            f"outside expected range")

    return {"run_id": run_id, "output_dir": str(output_dir),
            "results": all_results, "config": config}
