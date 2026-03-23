"""
Gen4 Core Configuration
========================
LowVol + Momentum 12-1 monthly rebalance strategy.
All parameters in one place. Backtest/batch/live share identical values.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Gen4Config:
    # ── Strategy ─────────────────────────────────────────────────────
    STRATEGY_VERSION: str = "4.0"

    # Factor scoring
    VOL_LOOKBACK: int = 252          # 12-month annualized volatility window
    VOL_PERCENTILE: float = 0.30     # bottom 30% = low volatility
    MOM_LOOKBACK: int = 252          # 12-month price window
    MOM_SKIP: int = 22               # skip last month (22 trading days)
    N_STOCKS: int = 20               # target portfolio size

    # Rebalance
    REBAL_DAYS: int = 21             # monthly (21 trading days)
    TARGET_MAX_STALE_DAYS: int = 3   # max calendar days before target is rejected

    # Trailing stop
    TRAIL_PCT: float = 0.12          # -12% from high watermark

    # ── Costs (realistic: fee 0.015% + slippage 0.10% + tax 0.18% sell) ─
    BUY_COST: float = 0.00115        # 0.015% fee + 0.10% slippage
    SELL_COST: float = 0.00295       # 0.015% fee + 0.10% slippage + 0.18% tax
    FEE: float = 0.00015             # commission per side (for reporter)
    SLIPPAGE: float = 0.001          # slippage per side (for reporter)
    TAX: float = 0.0018              # sell tax (for reporter)

    # ── Risk (from Gen2 governor, adapted) ───────────────────────────
    DAILY_DD_LIMIT: float = -0.04    # daily DD -> block new entries
    MONTHLY_DD_LIMIT: float = -0.07  # monthly DD -> block new entries
    # NO forced liquidation. Trail stop handles exits.

    # ── Capital ──────────────────────────────────────────────────────
    INITIAL_CASH: int = 500_000_000

    # ── Universe filters ─────────────────────────────────────────────
    UNIV_MIN_CLOSE: int = 2000       # minimum close price (KRW)
    UNIV_MIN_AMOUNT: float = 2e9     # 20-day avg daily traded value (KRW)
    UNIV_MIN_HISTORY: int = 260      # minimum trading days of history
    UNIV_MIN_COUNT: int = 500        # warn if universe below this

    # ── Paths ────────────────────────────────────────────────────────
    BASE_DIR: Path = field(default_factory=lambda: Path(__file__).resolve().parent)

    @property
    def OHLCV_DIR(self) -> Path:
        """Per-stock OHLCV CSVs (949 stocks, 2019~2026)."""
        return self.BASE_DIR.parent / "backtest" / "data_full" / "ohlcv"

    @property
    def OHLCV_DIR_GEN3(self) -> Path:
        """Gen3 per-stock OHLCV (2555 stocks, 2021~). Fallback."""
        return self.BASE_DIR.parent / "Gen03-02" / "data" / "ohlcv_kospi_daily"

    @property
    def INDEX_FILE(self) -> Path:
        return self.BASE_DIR.parent / "backtest" / "data_full" / "index" / "KOSPI.csv"

    @property
    def SECTOR_MAP(self) -> Path:
        return self.BASE_DIR.parent / "Gen03-02" / "data" / "sector_map.json"

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

    # ── Kiwoom ───────────────────────────────────────────────────────
    PAPER_TRADING: bool = True
    ACCOUNT_NO: str = ""
    TR_DELAY: float = 0.5
    TR_TIMEOUT: float = 20.0

    def ensure_dirs(self):
        """Create all output directories."""
        for d in [self.STATE_DIR, self.LOG_DIR, self.SIGNALS_DIR, self.REPORT_DIR]:
            d.mkdir(parents=True, exist_ok=True)
