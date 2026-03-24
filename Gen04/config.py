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
    CASH_BUFFER_RATIO: float = 0.95  # buy allocation capped at 95% of estimated cash

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

    # ── DD Graduated Response (STEP 5) ─────────────────────────────
    # (monthly_dd_threshold, buy_scale, trim_ratio, label)
    # Evaluated top-to-bottom: first match wins (most severe first)
    DD_LEVELS: tuple = (
        (-0.25, 0.00, 0.20, "DD_SAFE_MODE"),   # -25% → SAFE MODE, trim 20%
        (-0.20, 0.00, 0.20, "DD_SEVERE"),       # -20% → block buys, trim 20%
        (-0.15, 0.00, 0.00, "DD_CRITICAL"),     # -15% → block buys
        (-0.10, 0.50, 0.00, "DD_WARNING"),       # -10% → buy 50%
        (-0.05, 0.70, 0.00, "DD_CAUTION"),       # -5%  → buy 70%
    )
    SAFE_MODE_RELEASE_THRESHOLD: float = -0.20   # DD >= -20% → SAFE MODE 해제

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

    @property
    def INTRADAY_DIR(self) -> Path:
        return self.BASE_DIR / "data" / "intraday"

    # ── Execution Mode ─────────────────────────────────────────────
    # TRADING_MODE is the operator's intended mode.
    # server_type is the broker's actual connected environment.
    # If they do not match, abort immediately.
    #   mock  = internal simulation only (no broker)
    #   paper = broker mock trading (키움 모의투자)
    #   live  = broker real trading (실거래)
    TRADING_MODE: str = "paper"      # "mock" | "paper" | "live"

    # Deprecated — use TRADING_MODE instead.
    # Kept for backward compatibility; ignored if TRADING_MODE is set explicitly.
    PAPER_TRADING: bool = True

    # ── Kiwoom ───────────────────────────────────────────────────────
    ACCOUNT_NO: str = ""
    TR_DELAY: float = 0.5
    TR_TIMEOUT: float = 20.0

    def ensure_dirs(self):
        """Create all output directories."""
        for d in [self.STATE_DIR, self.LOG_DIR, self.SIGNALS_DIR,
                  self.REPORT_DIR, self.INTRADAY_DIR]:
            d.mkdir(parents=True, exist_ok=True)
