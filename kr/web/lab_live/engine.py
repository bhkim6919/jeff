"""
engine.py -- Lab Live Simulator (9-Strategy Forward Paper Trading)
===================================================================
Surge 패턴 기반. 9개 전략을 실시간 forward paper trading.
Phase 1: EOD daily run. Phase 2: live P&L. Phase 3: intraday.
"""
from __future__ import annotations
import copy
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import hashlib
import os

from web.lab_live.config import LabLiveConfig
from web.lab_live.state_store import (
    save_state, load_state, save_trades, load_trades, append_equity,
    atomic_write_json, safe_read_json,
    save_state_v2, load_state_v2, archive_state_v2,
)


# ── Date Lock (file-based, stale recovery) ──────────────

def _acquire_date_lock(lock_dir: Path, eod_date: str, stale_sec: float = 1800) -> bool:
    """Acquire file lock for date. Returns False if held by active process."""
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{eod_date}.lock"

    if lock_path.exists():
        try:
            lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
            pid = lock_data.get("pid", 0)
            started = lock_data.get("started_at", "")

            # PID alive check
            pid_alive = True
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                pid_alive = False

            # Stale check (30 min)
            stale = False
            if started:
                try:
                    dt = datetime.fromisoformat(started)
                    age = (datetime.now() - dt).total_seconds()
                    stale = age > stale_sec
                except Exception:
                    stale = True

            if pid_alive and not stale:
                return False  # Lock held

            logging.getLogger("lab.live").warning(
                f"[LAB_LOCK] Stale lock for {eod_date}, clearing"
            )
        except Exception:
            pass
        lock_path.unlink(missing_ok=True)

    # Acquire
    atomic_write_json(lock_path, {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(),
        "eod_date": eod_date,
    })
    return True


def _release_date_lock(lock_dir: Path, eod_date: str):
    lock_path = lock_dir / f"{eod_date}.lock"
    lock_path.unlink(missing_ok=True)


# ── Snapshot ID (reproducibility) ───────────────────────

def _compute_data_snapshot_id(close_df: pd.DataFrame, dates: pd.Series) -> str:
    """Content-based hash of OHLCV data for reproducibility."""
    h = hashlib.sha256()
    h.update(str(len(close_df.columns)).encode())
    h.update(str(len(dates)).encode())
    if len(dates) > 0:
        h.update(str(dates.iloc[0]).encode())
        h.update(str(dates.iloc[-1]).encode())
    # Sample first/last column values
    for col in list(close_df.columns)[:10]:
        vals = close_df[col].dropna()
        if len(vals) > 0:
            h.update(f"{vals.iloc[0]:.2f}".encode())
            h.update(f"{vals.iloc[-1]:.2f}".encode())
    return h.hexdigest()[:16]

logger = logging.getLogger("lab_live.engine")

# Add parent for lab imports
_gen04_rest = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_gen04_rest))


@dataclass
class LabLivePosition:
    code: str
    name: str
    qty: int
    entry_price: float
    entry_date: str
    high_wm: float
    buy_cost_total: float
    current_price: float = 0.0
    entry_day_idx: int = 0


@dataclass
class LabLiveLane:
    name: str
    group: str
    cash: float
    positions: Dict[str, LabLivePosition] = field(default_factory=dict)
    pending_buys: List[dict] = field(default_factory=list)
    trades: List[dict] = field(default_factory=list)
    equity_history: List[dict] = field(default_factory=list)
    last_rebal_idx: int = -999
    strategy: Any = None  # BaseStrategy instance


class LabLiveSimulator:
    """9-Strategy Forward Paper Trading Engine."""

    def __init__(self, config: LabLiveConfig = None):
        self.config = config or LabLiveConfig()
        self._lock = threading.Lock()
        self._lanes: Dict[str, LabLiveLane] = {}
        self._initialized = False
        self._running = False
        self._last_run_date = ""
        self._sector_map = {}
        self._start_time = None
        self._data_snapshot_id = ""
        self._missing_data_ratio = 0.0

    # ── Initialization ───────────────────────────────────────

    def initialize(self, reset: bool = False) -> dict:
        """9개 전략 초기화. reset=True면 archive 후 1억 재시작."""
        from lab.lab_config import STRATEGY_CONFIGS
        from lab.runner import create_strategy, load_sector_map

        with self._lock:
            # Archive before reset (audit trail 보존)
            if reset:
                self._archive_state()

            # Load sector map
            self._sector_map = load_sector_map(self.config.sector_map_file)

            # Try restore from saved state (v2: per-strategy + HEAD)
            saved = None
            _corrupted = False
            if not reset:
                saved = load_state_v2(self.config)
                if saved and saved.get("status") == "CORRUPTED":
                    logger.error(f"[LAB_LIVE] State CORRUPTED: {saved.get('message')}")
                    _corrupted = True
                    saved = None  # fall through to fresh start

            strategy_names = list(STRATEGY_CONFIGS.keys())

            for sname in strategy_names:
                scfg = STRATEGY_CONFIGS[sname]
                try:
                    strategy = create_strategy(sname)
                except Exception as e:
                    logger.error(f"[LAB_LIVE] Failed to create {sname}: {e}")
                    continue

                # Restore or fresh start
                if saved and sname in saved.get("lanes", {}):
                    lane_data = saved["lanes"][sname]
                    lane = LabLiveLane(
                        name=sname,
                        group=scfg.group,
                        cash=lane_data["cash"],
                        last_rebal_idx=lane_data.get("last_rebal_idx", -999),
                        equity_history=lane_data.get("equity_history", []),
                        strategy=strategy,
                    )
                    # Restore positions
                    for code, pdata in lane_data.get("positions", {}).items():
                        lane.positions[code] = LabLivePosition(
                            code=code,
                            name=pdata.get("name", code),
                            qty=pdata["qty"],
                            entry_price=pdata["entry_price"],
                            entry_date=pdata.get("entry_date", ""),
                            high_wm=pdata.get("high_wm", pdata["entry_price"]),
                            buy_cost_total=pdata.get("buy_cost_total", 0),
                            current_price=pdata.get("current_price", pdata["entry_price"]),
                            entry_day_idx=pdata.get("entry_day_idx", 0),
                        )
                    # Restore pending buys
                    lane.pending_buys = lane_data.get("pending_buys", [])
                    logger.info(f"[LAB_LIVE] Restored {sname}: "
                                f"cash={lane.cash:,.0f}, positions={len(lane.positions)}")
                else:
                    lane = LabLiveLane(
                        name=sname,
                        group=scfg.group,
                        cash=float(self.config.initial_cash),
                        strategy=strategy,
                    )

                # Restore strategy internal state
                if strategy and hasattr(strategy, '_last_rebal_idx'):
                    strategy._last_rebal_idx = lane.last_rebal_idx

                self._lanes[sname] = lane

            if saved:
                self._last_run_date = saved.get("last_run_date", "")
                # v2 includes trades; fallback to file load
                self._all_trades = saved.get("trades", [])
                if not self._all_trades:
                    self._all_trades = load_trades(self.config.trades_file)
                self._equity_rows = saved.get("equity_rows", [])
            else:
                self._all_trades = []
                self._equity_rows = []

            self._initialized = True
            self._running = True
            self._start_time = datetime.now()

            result = {
                "ok": True,
                "lanes": len(self._lanes),
                "restored": saved is not None and not reset,
                "last_run_date": self._last_run_date,
            }
            if _corrupted:
                result["warning"] = "CORRUPTED"
                result["message"] = "State was corrupted — started fresh"
            if saved and saved.get("recovered_from"):
                result["recovered_from"] = saved["recovered_from"]
            return result

    # ── Daily Run (Phase 1 핵심) ─────────────────────────────

    def run_daily(self) -> dict:
        """EOD 신호 생성 + 가상 체결. 하루 한 번 실행."""
        from lab.snapshot import build_snapshot, safe_slice
        from lab.universe import build_universe

        with self._lock:
            if not self._initialized:
                return {"error": "Not initialized"}

            t0 = time.time()
            today_str = datetime.now().strftime("%Y-%m-%d")
            lock_dir = self.config.state_dir / "locks"

            # Date lock — prevent concurrent/duplicate EOD
            if not _acquire_date_lock(lock_dir, today_str):
                return {"error": f"Date lock held for {today_str}. Another run in progress."}

            try:
                return self._run_daily_locked(t0, lock_dir, today_str)
            finally:
                _release_date_lock(lock_dir, today_str)

    def _run_daily_locked(self, t0, lock_dir, lock_date) -> dict:
        from lab.snapshot import build_snapshot, safe_slice
        from lab.universe import build_universe

        with self._lock:
            logger.info("[LAB_LIVE] Daily run starting...")

            # Load from DB (CSV fallback)
            try:
                from data.db_provider import DbProvider
                db = DbProvider()
                close, opn, high, low, vol, dates = db.build_matrices()
                idx_df = db.get_kospi_index()
                dates = idx_df["date"]
                idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
                logger.info(f"[LAB_LIVE] DB load: {len(close.columns)} stocks")
            except Exception as e:
                logger.warning(f"[LAB_LIVE] DB failed ({e}), CSV fallback")
                from lab.runner import load_ohlcv, build_matrices
                all_data = load_ohlcv(self.config.ohlcv_dir, self.config.univ_min_history)
                idx_df = pd.read_csv(self.config.index_file)
                date_col = "index" if "index" in idx_df.columns else "date"
                rename_map = {date_col: "date"}
                for s, d_ in [("Open", "open"), ("High", "high"), ("Low", "low"),
                               ("Close", "close"), ("Volume", "volume")]:
                    if s in idx_df.columns:
                        rename_map[s] = d_
                idx_df = idx_df.rename(columns=rename_map)
                idx_df["date"] = pd.to_datetime(idx_df["date"], errors="coerce")
                idx_df = idx_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                for c in ["open", "high", "low", "close", "volume"]:
                    if c in idx_df.columns:
                        idx_df[c] = pd.to_numeric(idx_df[c], errors="coerce").fillna(0)
                dates = idx_df["date"]
                close, opn, high, low, vol = build_matrices(all_data, dates)
                idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()

            # Today = latest date
            today_idx = len(dates) - 1
            today_date = str(dates.iloc[today_idx].date())

            if today_date == self._last_run_date:
                logger.info(f"[LAB_LIVE] Already ran for {today_date}, skipping")
                return {"skipped": True, "date": today_date}

            # Universe
            univ = build_universe(close, vol, today_idx,
                                  self.config.univ_min_close, self.config.univ_min_amount)

            # Missing data filter — exclude tickers with >20% NaN in lookback
            _min_hist = 252
            _max_missing = 0.20
            _filtered_univ = []
            _excluded_count = 0
            for tk in univ:
                if tk not in close.columns:
                    _excluded_count += 1
                    continue
                series = close[tk].iloc[max(0, today_idx - _min_hist):today_idx + 1]
                actual = series.notna().sum()
                expected = min(_min_hist, today_idx + 1)
                if expected > 0 and (1 - actual / expected) > _max_missing:
                    _excluded_count += 1
                    continue
                _filtered_univ.append(tk)
            if _filtered_univ:
                univ = _filtered_univ
            self._missing_data_ratio = round(
                _excluded_count / (len(univ) + _excluded_count) * 100, 1
            ) if (len(univ) + _excluded_count) > 0 else 0

            # Fundamental
            fund_df = load_fundamental(self.config.fundamental_dir, dates.iloc[today_idx])

            # Snapshot
            snapshot = build_snapshot(
                today_idx, dates, close, opn, high, low, vol,
                univ, self._sector_map, idx_close, fund_df,
            )

            # Name lookup
            name_lookup = {}
            for code, info in self._sector_map.items():
                if isinstance(info, dict):
                    name_lookup[code] = info.get("name", code)

            new_trades = []
            equity_row = {"date": today_date}

            for sname, lane in sorted(self._lanes.items()):
                try:
                    strategy = lane.strategy
                    if not strategy:
                        continue

                    # Sync strategy rebal idx
                    strategy._last_rebal_idx = lane.last_rebal_idx

                    # 1. Fill pending buys at today's open
                    filled = []
                    for pb in lane.pending_buys:
                        tk = pb["ticker"]
                        if tk in lane.positions:
                            filled.append(pb)
                            continue
                        entry_price = float(opn[tk].iloc[today_idx]) if tk in opn.columns and not pd.isna(opn[tk].iloc[today_idx]) else 0
                        if entry_price <= 0:
                            filled.append(pb)
                            continue

                        max_pos = strategy.config.max_positions
                        if len(lane.positions) >= max_pos:
                            filled.append(pb)
                            continue

                        buy_cost = entry_price * (1 + self.config.buy_cost)
                        per_pos = pb.get("per_pos", lane.cash * self.config.cash_buffer / max(1, max_pos - len(lane.positions)))
                        available = min(per_pos, lane.cash * self.config.cash_buffer)
                        qty = int(available / buy_cost)

                        if qty <= 0 or qty * buy_cost > lane.cash:
                            filled.append(pb)
                            continue

                        lane.cash -= qty * buy_cost
                        lane.positions[tk] = LabLivePosition(
                            code=tk,
                            name=name_lookup.get(tk, tk),
                            qty=qty,
                            entry_price=entry_price,
                            entry_date=today_date,
                            high_wm=entry_price,
                            buy_cost_total=qty * entry_price * self.config.buy_cost,
                            current_price=float(close[tk].iloc[today_idx]) if tk in close.columns else entry_price,
                            entry_day_idx=today_idx,
                        )
                        filled.append(pb)

                    for pb in filled:
                        if pb in lane.pending_buys:
                            lane.pending_buys.remove(pb)
                    lane.pending_buys = []  # clear stale

                    # 2. Exit policy — track sold tickers for same-day re-entry block
                    _exit_sold = set()
                    for tk in list(lane.positions.keys()):
                        pos = lane.positions[tk]
                        pos_dict = {
                            "ticker": tk, "qty": pos.qty,
                            "entry_price": pos.entry_price,
                            "entry_idx": pos.entry_day_idx,
                            "high_wm": pos.high_wm,
                            "buy_cost_total": pos.buy_cost_total,
                        }
                        reason = strategy.exit_policy.check_exit(snapshot, pos_dict, strategy._state)
                        pos.high_wm = pos_dict.get("high_wm", pos.high_wm)

                        if reason:
                            trade = self._close_position(lane, tk, snapshot, reason, today_date)
                            if trade:
                                new_trades.append(trade)
                                _exit_sold.add(tk)

                    # 3. Generate signals
                    pos_copy = {tk: {
                        "qty": p.qty, "entry_price": p.entry_price,
                        "entry_idx": p.entry_day_idx, "high_wm": p.high_wm,
                        "buy_cost_total": p.buy_cost_total, "ticker": tk,
                    } for tk, p in lane.positions.items()}

                    signals = strategy.generate_signals(snapshot, copy.deepcopy(pos_copy))

                    # 4. SELL signals — combine with exit-sold for same-day re-entry block
                    _sold_today = set(_exit_sold)
                    for sig in signals:
                        if sig.direction == "SELL" and sig.ticker in lane.positions:
                            trade = self._close_position(lane, sig.ticker, snapshot, sig.reason, today_date)
                            if trade:
                                new_trades.append(trade)
                                _sold_today.add(sig.ticker)

                    # 5. BUY signals → pending for T+1 (same-day re-entry 금지)
                    max_pos = strategy.config.max_positions
                    current = len(lane.positions) + len(lane.pending_buys)
                    pv = lane.cash + sum(
                        p.qty * float(close[p.code].iloc[today_idx])
                        for p in lane.positions.values()
                        if p.code in close.columns and float(close[p.code].iloc[today_idx]) > 0
                    )
                    per_pos = pv / max_pos if max_pos > 0 else 0

                    buy_sigs = sorted(
                        [s for s in signals if s.direction == "BUY"],
                        key=lambda s: -s.priority
                    )
                    for sig in buy_sigs:
                        if current >= max_pos:
                            break
                        if sig.ticker in lane.positions:
                            continue
                        if sig.ticker in _sold_today:
                            continue  # same-day re-entry 금지
                        if any(pb["ticker"] == sig.ticker for pb in lane.pending_buys):
                            continue
                        lane.pending_buys.append({
                            "ticker": sig.ticker,
                            "per_pos": per_pos,
                            "signal_date": today_date,
                        })
                        current += 1

                    # Update rebal idx
                    lane.last_rebal_idx = strategy._last_rebal_idx

                    # 6. Update current prices + record equity
                    equity = lane.cash
                    for tk, pos in lane.positions.items():
                        c = float(close[tk].iloc[today_idx]) if tk in close.columns else pos.entry_price
                        if c > 0 and not pd.isna(c):
                            pos.current_price = c
                            equity += pos.qty * c

                    lane.equity_history.append({
                        "date": today_date, "equity": equity,
                        "n_positions": len(lane.positions),
                    })
                    equity_row[sname] = equity

                except Exception as e:
                    logger.error(f"[LAB_LIVE] {sname} error: {e}")
                    # Record equity even on error
                    equity = lane.cash + sum(
                        p.qty * p.current_price for p in lane.positions.values()
                    )
                    equity_row[sname] = equity

            # Accumulate trades + equity, then save all via v2 committed write
            self._last_run_date = today_date
            if new_trades:
                self._all_trades.extend(new_trades)
            self._equity_rows.append(equity_row)
            self._save_state()

            elapsed = time.time() - t0
            logger.info(f"[LAB_LIVE] Daily run complete: {today_date} "
                        f"({len(new_trades)} trades, {elapsed:.1f}s)")

            # Compute data snapshot ID for reproducibility
            data_snap_id = _compute_data_snapshot_id(close, dates)
            self._data_snapshot_id = data_snap_id

            # ── Meta Layer Phase 0: collect & store ──
            try:
                from web.lab_live.meta_collector import collect_meta
                collect_meta(
                    today_date=today_date,
                    today_idx=today_idx,
                    close=close, high=high, vol=vol,
                    universe=univ,
                    sector_map=self._sector_map,
                    index_series=idx_close,
                    fundamental=fund_df,
                    lanes=self._lanes,
                    new_trades=new_trades,
                    data_snapshot_id=data_snap_id,
                    config=self.config,
                )
            except Exception as e:
                logger.warning(f"[META] Collection failed (non-fatal): {e}")

            return {
                "ok": True,
                "date": today_date,
                "trades": len(new_trades),
                "elapsed": round(elapsed, 1),
                "data_snapshot_id": data_snap_id,
                "universe_count": len(close.columns),
            }

    # ── State Access ─────────────────────────────────────────

    def get_state(self) -> dict:
        """Dashboard용 전체 스냅샷."""
        with self._lock:
            lanes = []
            for sname in sorted(self._lanes.keys()):
                lane = self._lanes[sname]
                equity = lane.cash
                positions = []
                for tk, pos in lane.positions.items():
                    pnl_pct = (pos.current_price / pos.entry_price - 1) * 100 if pos.entry_price > 0 else 0
                    equity += pos.qty * pos.current_price
                    positions.append({
                        "code": pos.code,
                        "name": pos.name,
                        "qty": pos.qty,
                        "entry_price": pos.entry_price,
                        "current_price": pos.current_price,
                        "entry_date": pos.entry_date,
                        "pnl_pct": round(pnl_pct, 2),
                        "pnl_amount": round((pos.current_price - pos.entry_price) * pos.qty, 0),
                    })

                # Performance
                init_eq = self.config.initial_cash
                total_return = (equity / init_eq - 1) * 100 if init_eq > 0 else 0

                # MDD from equity history
                mdd = 0
                if lane.equity_history:
                    eqs = [e["equity"] for e in lane.equity_history]
                    peak = eqs[0]
                    for eq in eqs:
                        peak = max(peak, eq)
                        dd = (eq - peak) / peak * 100
                        mdd = min(mdd, dd)

                lanes.append({
                    "name": sname,
                    "group": lane.group,
                    "cash": round(lane.cash),
                    "equity": round(equity),
                    "total_return": round(total_return, 2),
                    "mdd": round(mdd, 2),
                    "n_positions": len(lane.positions),
                    "n_trades": len([t for t in self._all_trades if t.get("strategy") == sname]),
                    "n_pending": len(lane.pending_buys),
                    "positions": positions,
                })

            return {
                "initialized": self._initialized,
                "running": self._running,
                "last_run_date": self._last_run_date,
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "n_lanes": len(self._lanes),
                "lanes": lanes,
            }

    def get_trades(self, limit: int = 50) -> list:
        with self._lock:
            return self._all_trades[-limit:]

    def get_equity_history(self) -> dict:
        """전략별 equity history."""
        with self._lock:
            result = {}
            for sname, lane in self._lanes.items():
                result[sname] = lane.equity_history
            return result

    # ── Internal ─────────────────────────────────────────────

    def _close_position(self, lane: LabLiveLane, ticker: str,
                        snapshot, reason: str, today_date: str) -> Optional[dict]:
        if ticker not in lane.positions:
            return None
        pos = lane.positions[ticker]
        p = float(snapshot.close.get(ticker, 0))
        if p <= 0 or pd.isna(p):
            p = pos.entry_price

        net = pos.qty * p * (1 - self.config.sell_cost)
        invested = pos.qty * pos.entry_price + pos.buy_cost_total
        pnl = (net - invested) / invested if invested > 0 else 0

        lane.cash += net

        # hold_days 계산
        try:
            from datetime import datetime as _dt
            hd = (_dt.strptime(today_date, "%Y-%m-%d") - _dt.strptime(pos.entry_date, "%Y-%m-%d")).days
        except Exception:
            hd = 0

        trade = {
            "strategy": lane.name,
            "ticker": ticker,
            "name": pos.name,
            "entry_date": pos.entry_date,
            "exit_date": today_date,
            "entry_price": pos.entry_price,
            "exit_price": p,
            "qty": pos.qty,
            "pnl_pct": round(pnl * 100, 2),
            "pnl_amount": round(net - invested),
            "exit_reason": reason,
            "hold_days": hd,
        }
        del lane.positions[ticker]
        return trade

    def _archive_state(self):
        """Archive current committed state (head + states + trades + equity)."""
        dest = archive_state_v2(self.config)
        if dest:
            logger.info(f"[LAB_LIVE] State archived → {dest}")
        else:
            logger.warning("[LAB_LIVE] Archive failed")

    def _save_state(self):
        """현재 상태를 v2 committed version으로 저장."""
        lanes_data = {}
        for sname, lane in self._lanes.items():
            lanes_data[sname] = {
                "cash": lane.cash,
                "positions": {
                    tk: {
                        "code": p.code, "name": p.name, "qty": p.qty,
                        "entry_price": p.entry_price, "entry_date": p.entry_date,
                        "high_wm": p.high_wm, "buy_cost_total": p.buy_cost_total,
                        "current_price": p.current_price,
                        "entry_day_idx": p.entry_day_idx,
                    } for tk, p in lane.positions.items()
                },
                "pending_buys": lane.pending_buys,
                "last_rebal_idx": lane.last_rebal_idx,
                "equity_history": lane.equity_history[-60:],  # keep last 60 days
            }
        save_state_v2(lanes_data, self._all_trades, self._equity_rows, self.config)
