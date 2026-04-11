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

from web.lab_live.config import LabLiveConfig
from web.lab_live.state_store import (
    save_state, load_state, save_trades, load_trades, append_equity,
)

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

            # Try restore from saved state
            saved = None
            if not reset:
                saved = load_state(self.config.state_file)

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
                # Restore trades
                self._all_trades = load_trades(self.config.trades_file)
            else:
                self._all_trades = []

            self._initialized = True
            self._running = True
            self._start_time = datetime.now()

            return {
                "ok": True,
                "lanes": len(self._lanes),
                "restored": saved is not None and not reset,
                "last_run_date": self._last_run_date,
            }

    # ── Daily Run (Phase 1 핵심) ─────────────────────────────

    def run_daily(self) -> dict:
        """EOD 신호 생성 + 가상 체결. 하루 한 번 실행."""
        from lab.snapshot import build_snapshot, safe_slice
        from lab.universe import build_universe

        with self._lock:
            if not self._initialized:
                return {"error": "Not initialized"}

            t0 = time.time()
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

            # Save state
            self._last_run_date = today_date
            self._save_state()

            # Save new trades
            if new_trades:
                self._all_trades.extend(new_trades)
                save_trades(new_trades, self.config.trades_file)

            # Append equity
            append_equity(equity_row, self.config.equity_file)

            elapsed = time.time() - t0
            logger.info(f"[LAB_LIVE] Daily run complete: {today_date} "
                        f"({len(new_trades)} trades, {elapsed:.1f}s)")

            return {
                "ok": True,
                "date": today_date,
                "trades": len(new_trades),
                "elapsed": round(elapsed, 1),
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
        """Archive current state + trades before reset (audit trail)."""
        import shutil
        from datetime import datetime
        archive_dir = self.config.state_file.parent / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = archive_dir / ts
        dest.mkdir(exist_ok=True)

        for f in [self.config.state_file,
                  self.config.state_file.parent / "trades.json",
                  self.config.state_file.parent / "equity_history.csv"]:
            if f.exists():
                try:
                    shutil.copy2(f, dest / f.name)
                except Exception as e:
                    logger.warning(f"[LAB_LIVE] Archive copy failed {f.name}: {e}")

        logger.info(f"[LAB_LIVE] State archived → {dest}")

    def _save_state(self):
        """현재 상태를 JSON으로 저장."""
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
        save_state(lanes_data, self.config.state_file)
