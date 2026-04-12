# -*- coding: utf-8 -*-
"""
Q-TRON US 1.0 Configuration
=============================
Alpaca broker, LowVol+Mom12-1 strategy for US equities.
Fully isolated from kr (KR).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class USConfig:
    # ── Market Identity ──────────────────────────────────────────
    MARKET: str = "US"
    STRATEGY_VERSION: str = "US-1.1"

    # ── Factor Scoring ───────────────────────────────────────────
    VOL_LOOKBACK: int = 252          # 12-month volatility window
    VOL_PERCENTILE: float = 1.00     # 1.0 = no filter (pure momentum_base, Lab 7yr CAGR 19.4%)
    MOM_LOOKBACK: int = 252          # 12-month price window
    MOM_SKIP: int = 22               # skip last month
    N_STOCKS: int = 20               # target portfolio size

    # ── Rebalance ────────────────────────────────────────────────
    REBAL_DAYS: int = 10             # biweekly (Lab 7yr: CAGR 27.7%, Sharpe 1.24)
    TARGET_MAX_STALE_DAYS: int = 3
    CASH_BUFFER_RATIO: float = 0.95

    # ── Trailing Stop ────────────────────────────────────────────
    TRAIL_PCT: float = 0.12          # -12% from high watermark

    # ── Costs (Alpaca zero-commission + slippage) ────────────────
    BUY_COST: float = 0.0005         # 0.05% slippage only
    SELL_COST: float = 0.0005         # no tax
    FEE: float = 0.0
    SLIPPAGE: float = 0.0005
    TAX: float = 0.0

    # ── Risk ─────────────────────────────────────────────────────
    DAILY_DD_LIMIT: float = -0.04
    MONTHLY_DD_LIMIT: float = -0.07

    DD_LEVELS: tuple = (
        (-0.25, 0.00, 0.20, "DD_SAFE_MODE"),
        (-0.20, 0.00, 0.20, "DD_SEVERE"),
        (-0.15, 0.00, 0.00, "DD_CRITICAL"),
        (-0.10, 0.50, 0.00, "DD_WARNING"),
        (-0.05, 0.70, 0.00, "DD_CAUTION"),
    )
    SAFE_MODE_RELEASE_THRESHOLD: float = -0.20

    # ── Universe Filters (USD) ───────────────────────────────────
    MARKETS: List[str] = field(default_factory=lambda: ["US"])
    UNIV_MIN_CLOSE: float = 5.0           # $5 minimum
    UNIV_MIN_AMOUNT: float = 10_000_000   # $10M daily traded value
    UNIV_MIN_HISTORY: int = 260
    UNIV_MAX_CANDIDATES: int = 300
    UNIV_MIN_COUNT: int = 100

    # ── Price Sanity ─────────────────────────────────────────────
    PRICE_MIN: float = 0.01
    PRICE_MAX: float = 100_000.0

    # ── Capital ──────────────────────────────────────────────────
    INITIAL_CASH: float = 100_000.0

    # ── Alpaca ───────────────────────────────────────────────────
    ALPACA_BASE_URL: str = "https://paper-api.alpaca.markets"
    ALPACA_DATA_URL: str = "https://data.alpaca.markets"

    # ── Market Timing ────────────────────────────────────────────
    MARKET_TZ: str = "US/Eastern"
    MARKET_OPEN: str = "09:30"
    MARKET_CLOSE: str = "16:00"

    # ── Fill / Order Safety ──────────────────────────────────────
    FILL_TIMEOUT_SEC: float = 30.0
    MAX_GHOST_AGE_SEC: float = 300.0
    GHOST_RECONCILE_INTERVAL_SEC: float = 60.0
    SNAPSHOT_MAX_STALE_HOURS: int = 24

    # ── Execution Mode ───────────────────────────────────────────
    TRADING_MODE: str = "paper"      # "paper" | "live"

    # ── Paths ────────────────────────────────────────────────────
    BASE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent)

    @property
    def STATE_DIR(self) -> Path:
        return self.BASE_DIR / "state"

    @property
    def LOG_DIR(self) -> Path:
        return self.BASE_DIR / "logs"

    @property
    def SIGNALS_DIR(self) -> Path:
        return self.BASE_DIR / "data" / "signals"

    @property
    def REPORT_DIR(self) -> Path:
        return self.BASE_DIR / "report" / "output"

    @property
    def OHLCV_DIR(self) -> Path:
        """Fallback CSV dir (primary = DB)."""
        return self.BASE_DIR / "backtest" / "ohlcv"

    @property
    def INDEX_FILE(self) -> Path:
        return self.BASE_DIR / "backtest" / "index" / "SPY.csv"

    def ensure_dirs(self):
        """Create all output directories."""
        for d in [self.STATE_DIR, self.LOG_DIR, self.SIGNALS_DIR,
                  self.REPORT_DIR, self.OHLCV_DIR]:
            d.mkdir(parents=True, exist_ok=True)
