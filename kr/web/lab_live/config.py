"""
config.py -- Lab Live Configuration
=====================================
Forward paper trading 설정. Surge config 패턴 참조.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class LabLiveConfig:
    # Capital
    initial_cash: int = 100_000_000      # 1억

    # Costs (BASELINE_SPEC)
    buy_cost: float = 0.00115
    sell_cost: float = 0.00295
    cash_buffer: float = 0.95

    # Strategy defaults
    default_max_positions: int = 20
    default_rebal_days: int = 21
    default_trail_pct: float = 0.12

    # Scoring
    vol_lookback: int = 252
    vol_percentile: float = 0.30
    mom_lookback: int = 252
    mom_skip: int = 22

    # Universe
    univ_min_close: int = 2000
    univ_min_amount: float = 2e9
    univ_min_history: int = 260

    # Auto-run
    eod_auto_run: bool = True
    eod_run_hour: int = 16
    eod_run_minute: int = 5

    # SSE
    sse_interval: float = 2.0

    # Paths
    @property
    def base_dir(self) -> Path:
        return Path(__file__).resolve().parent.parent.parent

    @property
    def ohlcv_dir(self) -> Path:
        return self.base_dir.parent / "backtest" / "data_full" / "ohlcv"

    @property
    def index_file(self) -> Path:
        return self.base_dir.parent / "backtest" / "data_full" / "index" / "KOSPI.csv"

    @property
    def sector_map_file(self) -> Path:
        local = self.base_dir / "data" / "sector_map.json"
        if local.exists():
            return local
        return self.base_dir.parent / "backtest" / "data_full" / "sector_map.json"

    @property
    def fundamental_dir(self) -> Path:
        return self.base_dir.parent / "backtest" / "data_full" / "fundamental"

    @property
    def state_dir(self) -> Path:
        return self.base_dir / "data" / "lab_live"

    @property
    def state_file(self) -> Path:
        """Legacy monolithic state (migration source)."""
        return self.state_dir / "state.json"

    @property
    def states_dir(self) -> Path:
        """Per-strategy state files directory."""
        return self.state_dir / "states"

    @property
    def head_file(self) -> Path:
        """Committed version pointer."""
        return self.state_dir / "head.json"

    @property
    def trades_file(self) -> Path:
        return self.state_dir / "trades.json"

    @property
    def equity_file(self) -> Path:
        """Legacy CSV equity (migration source)."""
        return self.state_dir / "equity_history.csv"

    @property
    def equity_json_file(self) -> Path:
        """Versioned equity JSON."""
        return self.state_dir / "equity.json"

    @property
    def state_io_lock_file(self) -> Path:
        """File-level lock for cross-process safety."""
        return self.state_dir / ".state_io.lock"
