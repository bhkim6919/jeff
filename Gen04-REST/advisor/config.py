"""Advisor configuration — rules, paths, thresholds."""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # Gen04/
ADVISOR_DIR = Path(__file__).resolve().parent       # Gen04/advisor/

# ── Access Rules ──
ADVISOR_RULES = {
    "no_order_execution": True,
    "no_state_write": True,
    "no_broker_access": True,
    "no_reconcile_modify": True,
    "no_auto_param_apply": True,
    "no_auto_code_modify": True,

    "allowed_read_paths": [
        str(BASE_DIR / "state"),
        str(BASE_DIR / "report" / "output"),
        str(BASE_DIR / "report" / "output_test"),
        str(BASE_DIR / "data" / "signals"),
        str(BASE_DIR / "data" / "intraday"),
        str(BASE_DIR / "logs"),
        str(BASE_DIR / "config.py"),
    ],
    "allowed_write_paths": [
        str(ADVISOR_DIR / "output"),
        str(ADVISOR_DIR / "cache"),
        str(ADVISOR_DIR / "metrics"),
    ],
}

# ── Paths ──
STATE_DIR = BASE_DIR / "state"
REPORT_DIR = BASE_DIR / "report" / "output"
REPORT_DIR_TEST = BASE_DIR / "report" / "output_test"
SIGNALS_DIR = BASE_DIR / "data" / "signals"
SIGNALS_DIR_TEST = BASE_DIR / "data" / "signals" / "test"
LOG_DIR = BASE_DIR / "logs"
INTRADAY_DIR = BASE_DIR / "data" / "intraday"
OUTPUT_DIR = ADVISOR_DIR / "output"
CACHE_DIR = ADVISOR_DIR / "cache"
METRICS_DIR = ADVISOR_DIR / "metrics"

# ── Thresholds ──
MIN_WINDOW_COVERAGE = 0.80     # 80% valid days required
MAX_SNAPSHOT_AGE_HOURS = 24    # dd_blocked_buys stale threshold
MAX_HIGH_RECOMMENDATIONS = 3
MAX_MEDIUM_RECOMMENDATIONS = 5
MAX_ACTIVE_OVERRIDES = 3

# ── Operational tags (auto-exclude from strategy analysis) ──
OPERATIONAL_TAGS = frozenset({
    "RECON_UNRELIABLE", "DIRTY_EXIT", "SAFE_MODE",
    "RECON_SAFETY", "BROKER_STATE_UNRELIABLE",
    "MODE_MISMATCH_ABORT", "GHOST_FILL_ERROR",
    "RECON_SAFE_MODE_DETECTED",
})

# ── Log tags to parse ──
IMPORTANT_LOG_TAGS = frozenset({
    "RECON", "DD_GUARD", "GHOST_FILL", "GHOST_FILL_FINALIZED",
    "SELL_STATUS_UPGRADED", "FAST_REENTRY", "PENDING_BUY",
    "TRAIL", "EOD_TRAIL", "REBALANCE", "RISK_ACTION",
    "DIRTY_EXIT", "SAFE_MODE", "BUY_PERMISSION",
    "TIMEOUT_UNCERTAIN", "PENDING_EXTERNAL",
})
