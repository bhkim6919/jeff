# -*- coding: utf-8 -*-
"""
forward.py — US Forward Trading (Append-Only Simulator)
=========================================================
- Versioned state + HEAD pointer (atomic commit)
- Date-level lock with stale recovery
- Same-day close fill, no pending buys
- Same-day re-entry prohibited
- Per-strategy independent state
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("qtron.us.lab.forward")

STATE_DIR = Path(__file__).resolve().parent / "state" / "forward"
VERSIONS_DIR = STATE_DIR / "versions"
RUNS_DIR = STATE_DIR / "runs"
LOCKS_DIR = STATE_DIR / "locks"
ARCHIVE_DIR = STATE_DIR / "archive"

for d in [STATE_DIR, VERSIONS_DIR, RUNS_DIR, LOCKS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_write(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(str(tmp), str(path))


def _load_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── Forward Strategy State ──────────────────────────────

@dataclass
class ForwardStrategyState:
    strategy_name: str
    cash: float = 100_000
    positions: Dict[str, dict] = field(default_factory=dict)
    trades: List[dict] = field(default_factory=list)
    equity_history: List[list] = field(default_factory=list)  # [[date, equity], ...]

    # Reproducibility
    universe_snapshot_id: str = ""
    data_snapshot_id: str = ""
    last_eod_date: str = ""
    last_signal_date: str = ""
    day_count: int = 0
    cost_model_version: str = "US-1.0"
    excluded_tickers: List[str] = field(default_factory=list)
    missing_data_ratio: float = 0.0
    last_run_id: str = ""
    last_run_status: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ForwardStrategyState:
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)

    def get_equity(self, close_dict: dict = None) -> float:
        eq = self.cash
        for sym, pos in self.positions.items():
            price = (close_dict or {}).get(sym, pos.get("avg_price", 0))
            eq += pos.get("quantity", 0) * price
        return eq


# ── Lock Management ─────────────────────────────────────

def _acquire_lock(eod_date: str, run_id: str) -> bool:
    lock_path = LOCKS_DIR / f"{eod_date}.lock"

    # Check stale lock
    if lock_path.exists():
        lock_data = _load_json(lock_path)
        if lock_data:
            pid = lock_data.get("pid", 0)
            started = lock_data.get("started_at", "")

            # PID check
            try:
                os.kill(pid, 0)  # check if process exists
                pid_alive = True
            except (OSError, ProcessLookupError):
                pid_alive = False

            # Time check (30 min stale)
            stale_time = False
            if started:
                try:
                    dt = datetime.fromisoformat(started)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - dt).total_seconds()
                    stale_time = age > 1800  # 30 minutes
                except Exception:
                    stale_time = True

            if not pid_alive or stale_time:
                logger.warning(f"[FORWARD] Stale lock detected for {eod_date}, clearing")
                lock_path.unlink(missing_ok=True)
                # Mark run as FAILED
                run_path = RUNS_DIR / f"{eod_date}.json"
                if run_path.exists():
                    run_data = _load_json(run_path) or {}
                    if run_data.get("status") == "RUNNING":
                        run_data["status"] = "FAILED"
                        run_data["error"] = "stale_lock_recovered"
                        run_data["finished_at"] = _now_iso()
                        _atomic_write(run_path, run_data)
            else:
                return False  # Lock held by active process

    # Acquire
    _atomic_write(lock_path, {
        "pid": os.getpid(),
        "started_at": _now_iso(),
        "run_id": run_id,
    })
    return True


def _release_lock(eod_date: str):
    lock_path = LOCKS_DIR / f"{eod_date}.lock"
    lock_path.unlink(missing_ok=True)


# ── Meta Management ─────────────────────────────────────

def _load_meta() -> dict:
    return _load_json(STATE_DIR / "meta.json") or {
        "status": "NOT_INITIALIZED",
        "last_committed_run_id": "",
        "current_run_id": "",
        "current_eod_date": "",
        "last_successful_eod_date": "",
        "started_at": "",
        "day_count": 0,
        "schema_version": "1.0",
    }


def _save_meta(meta: dict):
    _atomic_write(STATE_DIR / "meta.json", meta)


# ── Forward Trader ──────────────────────────────────────

class ForwardTrader:

    def initialize(self) -> dict:
        """Initialize 10 strategies with $100K each."""
        from .lab_config import STRATEGY_CONFIGS

        meta = _load_meta()
        if meta["status"] not in ("NOT_INITIALIZED", "IDLE"):
            # Already initialized — return current state
            return meta

        run_id = str(uuid.uuid4())[:8]
        version_dir = VERSIONS_DIR / run_id
        version_dir.mkdir(parents=True, exist_ok=True)

        for name in STRATEGY_CONFIGS:
            state = ForwardStrategyState(strategy_name=name)
            _atomic_write(version_dir / f"{name}.json", state.to_dict())

        meta = {
            "status": "IDLE",
            "last_committed_run_id": run_id,
            "current_run_id": "",
            "current_eod_date": "",
            "last_successful_eod_date": "",
            "started_at": _now_iso(),
            "day_count": 0,
            "schema_version": "1.0",
        }
        _save_meta(meta)

        logger.info(f"[FORWARD] Initialized {len(STRATEGY_CONFIGS)} strategies (run_id={run_id})")
        return meta

    def run_eod(self, eod_date: str, provider=None, force: bool = False) -> dict:
        """Run EOD for a specific date. Atomic commit via versioned state."""
        with _lock:
            return self._run_eod_locked(eod_date, provider, force)

    def _run_eod_locked(self, eod_date: str, provider, force: bool) -> dict:
        from .lab_config import STRATEGY_CONFIGS, MISSING_THRESHOLDS, BUY_COST, SELL_COST
        from .runner import _load_strategy, filter_universe_for_strategy
        from .engine import DailySnapshot, safe_close_series
        from data.universe_builder import load_universe_snapshot, get_universe_snapshot_id

        meta = _load_meta()
        if meta["status"] == "NOT_INITIALIZED":
            # Auto-initialize
            ForwardTrader().initialize()
            meta = _load_meta()

        # Check existing run
        run_path = RUNS_DIR / f"{eod_date}.json"
        existing_run = _load_json(run_path)
        if existing_run:
            if existing_run["status"] == "DONE" and not force:
                return {"error": f"EOD already DONE for {eod_date}", "run": existing_run}
            if existing_run["status"] == "RUNNING":
                return {"error": f"EOD RUNNING for {eod_date}. Wait or check stale lock."}

        run_id = str(uuid.uuid4())[:8]

        # Acquire lock
        if not _acquire_lock(eod_date, run_id):
            return {"error": f"Lock held for {eod_date}. Another EOD in progress."}

        try:
            # Record RUNNING
            run_record = {
                "eod_date": eod_date,
                "run_id": run_id,
                "status": "RUNNING",
                "started_at": _now_iso(),
                "finished_at": "",
                "strategies_processed": [],
                "snapshots": {},
                "error": "",
            }
            _atomic_write(run_path, run_record)
            meta["current_run_id"] = run_id
            meta["current_eod_date"] = eod_date
            meta["status"] = "RUNNING"
            _save_meta(meta)

            # Load previous committed state
            prev_run_id = meta["last_committed_run_id"]
            prev_dir = VERSIONS_DIR / prev_run_id

            # Fetch market data from Alpaca
            from regime.collector import fetch_alpaca_snapshots
            tickers_r1000 = load_universe_snapshot("RESEARCH_R1000")

            snapshots_data = {}
            if provider:
                snapshots_data = fetch_alpaca_snapshots(tickers_r1000[:500], provider)
                snapshots_data.update(fetch_alpaca_snapshots(tickers_r1000[500:], provider))
            else:
                # Fallback: load from DB
                from data.db_provider import DbProviderUS
                db = DbProviderUS()
                close_dict_db = db.load_close_dict_research(min_history=1, symbols=tickers_r1000)
                for sym, series in close_dict_db.items():
                    if len(series) > 0:
                        snapshots_data[sym] = {
                            "price": float(series.iloc[-1]),
                            "prev_close": float(series.iloc[-2]) if len(series) > 1 else float(series.iloc[-1]),
                            "change_pct": 0,
                        }

            close_dict = {sym: d["price"] for sym, d in snapshots_data.items() if d.get("price", 0) > 0}

            # Also load historical for signal generation
            from data.db_provider import DbProviderUS
            db = DbProviderUS()
            ohlcv_dict = db.load_ohlcv_dict_research(min_history=20, symbols=list(close_dict.keys()))
            full_close_dict = db.load_close_dict_research(min_history=20, symbols=list(close_dict.keys()))

            # Create new version directory
            version_dir = VERSIONS_DIR / run_id
            version_dir.mkdir(parents=True, exist_ok=True)

            processed = []
            snapshot_bundle = {}

            for strat_name, strat_config in STRATEGY_CONFIGS.items():
                try:
                    # Load previous state
                    prev_state_path = prev_dir / f"{strat_name}.json"
                    prev_data = _load_json(prev_state_path)
                    if not prev_data:
                        state = ForwardStrategyState(strategy_name=strat_name)
                    else:
                        state = ForwardStrategyState.from_dict(prev_data)

                    # Universe filter
                    universe_name = strat_config.get("universe", "RESEARCH_R1000")
                    uni_tickers = load_universe_snapshot(universe_name) if "R3000" in universe_name else tickers_r1000
                    valid, excluded = filter_universe_for_strategy(strat_name, uni_tickers, full_close_dict)

                    uni_snap_id = get_universe_snapshot_id(universe_name)
                    state.universe_snapshot_id = uni_snap_id
                    state.excluded_tickers = [e[0] for e in excluded[:20]]
                    state.missing_data_ratio = round(len(excluded) / len(uni_tickers) * 100, 1) if uni_tickers else 0

                    # Build snapshot for this day
                    snapshot = DailySnapshot(
                        date=eod_date,
                        day_idx=state.day_count,
                        close_dict={s: close_dict[s] for s in valid if s in close_dict},
                        open_dict={s: close_dict.get(s, 0) for s in valid},
                        high_dict={s: close_dict.get(s, 0) for s in valid},
                        low_dict={s: close_dict.get(s, 0) for s in valid},
                        volume_dict={},
                    )

                    # Build matrices for signal generation
                    filtered_ohlcv = {s: df for s, df in ohlcv_dict.items() if s in set(valid)}
                    from .engine import _build_matrices
                    all_dates = set()
                    for df in filtered_ohlcv.values():
                        if "date" in df.columns:
                            all_dates.update(df["date"].astype(str).tolist())
                    dates = sorted(all_dates)
                    if eod_date not in dates:
                        dates.append(eod_date)
                        dates.sort()
                    matrices = _build_matrices(filtered_ohlcv, dates)
                    # Add today's close to matrices
                    for sym in close_dict:
                        if sym in matrices["close"]:
                            matrices["close"][sym][eod_date] = close_dict[sym]
                    matrices["dates"] = dates

                    day_idx = len(dates) - 1

                    # ── Processing Order (fixed) ────────────
                    trail_pct = strat_config.get("trail_pct", 0.12)
                    max_pos = strat_config.get("max_positions", 20)
                    sold_today: Set[str] = set()

                    # 1. Trail stop evaluation
                    trail_sells = []
                    for sym, pos in list(state.positions.items()):
                        price = close_dict.get(sym, 0)
                        if price <= 0:
                            continue
                        hwm = max(pos.get("high_watermark", price), price)
                        pos["high_watermark"] = hwm
                        trigger = hwm * (1 - trail_pct)
                        if price <= trigger:
                            trail_sells.append(sym)

                    # 2. Strategy signal generation
                    strategy = _load_strategy(strat_name, strat_config)
                    from .engine import StrategyState
                    engine_state = StrategyState(name=strat_name)
                    engine_state.day_count = state.day_count  # forward counter, not matrix index
                    # Convert positions to engine format
                    from .engine import SimPosition
                    for sym, pos in state.positions.items():
                        engine_state.positions[sym] = SimPosition(
                            symbol=sym,
                            quantity=pos.get("quantity", 0),
                            avg_price=pos.get("avg_price", 0),
                            entry_date=pos.get("entry_date", ""),
                            high_watermark=pos.get("high_watermark", 0),
                            entry_day_idx=pos.get("entry_day_idx", 0),
                        )

                    buys, sells = [], []
                    if strategy:
                        snapshot_for_signal = DailySnapshot(
                            date=eod_date, day_idx=day_idx,
                            close_dict=snapshot.close_dict,
                            open_dict=snapshot.open_dict,
                            high_dict=snapshot.high_dict,
                            low_dict=snapshot.low_dict,
                            volume_dict=snapshot.volume_dict,
                        )
                        try:
                            buys, sells = strategy.generate_signals(
                                snapshot_for_signal, engine_state, matrices
                            )
                        except Exception as e:
                            logger.warning(f"[FORWARD] {strat_name} signal error: {e}")

                    # 3. SELL execution (trail + signal)
                    all_sells = set(trail_sells + [s["symbol"] for s in sells])
                    for sym in all_sells:
                        if sym not in state.positions:
                            continue
                        pos = state.positions.pop(sym)
                        price = close_dict.get(sym, pos.get("avg_price", 0))
                        proceeds = pos["quantity"] * price * (1 - SELL_COST)
                        state.cash += proceeds
                        pnl = proceeds - pos["quantity"] * pos["avg_price"]
                        reason = "trail_stop" if sym in trail_sells else "signal"
                        state.trades.append({
                            "symbol": sym, "side": "SELL", "qty": pos["quantity"],
                            "price": round(price, 2), "date": eod_date,
                            "pnl": round(pnl, 2), "exit_reason": reason,
                        })
                        sold_today.add(sym)

                    # 4+5. BUY execution (same-day re-entry prohibited)
                    available_slots = max_pos - len(state.positions)
                    for buy in buys[:available_slots]:
                        sym = buy.get("symbol", "")
                        if sym in sold_today:
                            continue  # same-day re-entry prohibited
                        if sym in state.positions:
                            continue
                        price = close_dict.get(sym, 0)
                        if price <= 0:
                            continue

                        n_buys = min(available_slots, len(buys))
                        allocation = state.cash * 0.95 / max(n_buys, 1)
                        cost_price = price * (1 + BUY_COST)
                        qty = int(allocation / cost_price)
                        if qty <= 0 or qty * cost_price > state.cash:
                            continue

                        state.cash -= qty * cost_price
                        state.positions[sym] = {
                            "symbol": sym, "quantity": qty,
                            "avg_price": round(price, 2),
                            "entry_date": eod_date,
                            "high_watermark": round(price, 2),
                            "entry_day_idx": day_idx,
                        }
                        state.trades.append({
                            "symbol": sym, "side": "BUY", "qty": qty,
                            "price": round(price, 2), "date": eod_date,
                            "reason": buy.get("reason", "signal"),
                        })
                        available_slots -= 1

                    # 6. Equity append
                    equity = state.get_equity(close_dict)
                    state.equity_history.append([eod_date, round(equity, 2)])
                    state.day_count += 1
                    state.last_eod_date = eod_date
                    state.last_run_id = run_id
                    state.last_run_status = "DONE"

                    # Save to version directory
                    _atomic_write(version_dir / f"{strat_name}.json", state.to_dict())
                    processed.append(strat_name)

                    snapshot_bundle[strat_name] = {
                        "universe": uni_snap_id,
                        "positions": len(state.positions),
                        "equity": round(equity, 2),
                    }

                except Exception as e:
                    logger.error(f"[FORWARD] {strat_name} error: {e}", exc_info=True)
                    # Copy previous state to new version (no change)
                    prev_file = prev_dir / f"{strat_name}.json"
                    if prev_file.exists():
                        shutil.copy2(prev_file, version_dir / f"{strat_name}.json")

            # 7. Commit: update HEAD pointer
            meta["last_committed_run_id"] = run_id
            meta["last_successful_eod_date"] = eod_date
            meta["current_run_id"] = ""
            meta["current_eod_date"] = ""
            meta["status"] = "IDLE"
            meta["day_count"] = meta.get("day_count", 0) + 1
            _save_meta(meta)

            # 8. Run record → DONE
            run_record["status"] = "DONE"
            run_record["finished_at"] = _now_iso()
            run_record["strategies_processed"] = processed
            run_record["snapshots"] = snapshot_bundle
            _atomic_write(run_path, run_record)

            logger.info(f"[FORWARD] EOD {eod_date} done: {len(processed)} strategies")
            return run_record

        except Exception as e:
            # Mark FAILED
            run_record = _load_json(run_path) or {}
            run_record["status"] = "FAILED"
            run_record["error"] = str(e)
            run_record["finished_at"] = _now_iso()
            _atomic_write(run_path, run_record)

            meta = _load_meta()
            meta["status"] = "IDLE"
            meta["current_run_id"] = ""
            _save_meta(meta)

            logger.error(f"[FORWARD] EOD {eod_date} failed: {e}", exc_info=True)
            return {"error": str(e)}

        finally:
            _release_lock(eod_date)

    def get_state(self) -> dict:
        """Get current state for dashboard (HEAD pointer based)."""
        meta = _load_meta()
        run_id = meta.get("last_committed_run_id", "")
        if not run_id:
            return {"meta": meta, "strategies": {}}

        version_dir = VERSIONS_DIR / run_id
        strategies = {}
        if version_dir.exists():
            for f in sorted(version_dir.glob("*.json")):
                data = _load_json(f)
                if data:
                    name = data.get("strategy_name", f.stem)
                    eq_hist = data.get("equity_history", [])
                    last_eq = eq_hist[-1][1] if eq_hist else 100_000
                    first_eq = eq_hist[0][1] if eq_hist else 100_000
                    pnl_pct = (last_eq / first_eq - 1) * 100 if first_eq > 0 else 0

                    # Simple MDD
                    mdd = 0
                    if eq_hist:
                        peak = 0
                        for _, eq in eq_hist:
                            peak = max(peak, eq)
                            dd = (eq - peak) / peak * 100 if peak > 0 else 0
                            mdd = min(mdd, dd)

                    strategies[name] = {
                        "equity": round(last_eq, 2),
                        "pnl_pct": round(pnl_pct, 2),
                        "mdd": round(mdd, 2),
                        "positions": len(data.get("positions", {})),
                        "trades": len(data.get("trades", [])),
                        "day_count": data.get("day_count", 0),
                        "last_eod_date": data.get("last_eod_date", ""),
                    }

        return {"meta": meta, "strategies": strategies}

    def get_runs(self) -> List[dict]:
        """List all EOD run records."""
        runs = []
        for f in sorted(RUNS_DIR.glob("*.json"), reverse=True):
            data = _load_json(f)
            if data:
                runs.append(data)
        return runs[:30]

    def reset(self, strategy_name: str = None) -> dict:
        """Reset all or single strategy. Archive existing state."""
        meta = _load_meta()
        if meta["status"] == "RUNNING":
            return {"error": "Cannot reset while RUNNING"}

        # Archive
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = ARCHIVE_DIR / ts
        archive_path.mkdir(parents=True, exist_ok=True)

        run_id = meta.get("last_committed_run_id", "")
        if run_id:
            src = VERSIONS_DIR / run_id
            if src.exists():
                shutil.copytree(src, archive_path / run_id, dirs_exist_ok=True)

        # Copy meta
        meta_src = STATE_DIR / "meta.json"
        if meta_src.exists():
            shutil.copy2(meta_src, archive_path / "meta.json")

        logger.info(f"[FORWARD] Archived to {archive_path}")

        # Re-initialize
        if strategy_name:
            # Single strategy reset
            if run_id:
                version_dir = VERSIONS_DIR / run_id
                state = ForwardStrategyState(strategy_name=strategy_name)
                _atomic_write(version_dir / f"{strategy_name}.json", state.to_dict())
            return {"status": "reset", "strategy": strategy_name}
        else:
            # Full reset
            meta = {
                "status": "NOT_INITIALIZED",
                "last_committed_run_id": "",
                "current_run_id": "",
                "current_eod_date": "",
                "last_successful_eod_date": "",
                "started_at": "",
                "day_count": 0,
                "schema_version": "1.0",
            }
            _save_meta(meta)
            return {"status": "reset_all", "archive": str(archive_path)}
