"""
Lifecycle utility functions extracted from main.py.
File hashing, logging setup, trading day/hours checks,
trading mode validation, state save, and regime snapshot.
"""
from __future__ import annotations
import hashlib
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


def _file_hash(path: Path) -> str:
    """SHA256 hash of a file, or empty string if file doesn't exist."""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


# ── Logging Setup ────────────────────────────────────────────────────────────
def setup_logging(log_dir: Path, mode: str):
    log_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    log_file = log_dir / f"gen4_{mode}_{today}.log"
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # R20 (2026-04-23): prevent UnicodeEncodeError on Windows cp949 consoles.
    # Reconfigures stdout to UTF-8 with `errors='replace'` so em-dash (—),
    # bullet (•), emoji (✅) etc. don't raise when logged. File handler is
    # already UTF-8 — only stream handler was the culprit.
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    logging.basicConfig(
        level=logging.INFO, format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Auto-cleanup old logs (30 days retention)
    try:
        from data.rest_logger import _cleanup_old_logs
        _cleanup_old_logs(log_dir)
    except Exception:
        pass


def is_weekday() -> bool:
    return date.today().weekday() < 5

def is_trading_day() -> bool:
    """평일이면 거래일. 공휴일은 pykrx로 제외."""
    today_dt = date.today()
    if today_dt.weekday() >= 5:
        return False
    try:
        from pykrx import stock as pykrx_stock
        today = today_dt.strftime("%Y%m%d")
        # 당월 거래일 목록 조회 (과거 확정분만 포함)
        month_start = today_dt.replace(day=1).strftime("%Y%m%d")
        df = pykrx_stock.get_market_ohlcv_by_date(month_start, today, "005930")
        if df is None or df.empty:
            return True  # 데이터 없으면 평일 신뢰
        last_trading = df.index[-1].strftime("%Y%m%d")
        if last_trading == today:
            return True  # 당일 데이터 있음 = 거래일
        # 당일 데이터 없음 = 장 전이거나 공휴일
        # 장 전이면 어제가 마지막 → 오늘은 아직 미확정 → 평일이므로 거래일
        # 공휴일이면 어제도 거래일이었을 것 → 동일하게 평일 신뢰
        # 진짜 구분: 내일 데이터가 나와야 알 수 있음
        # → 평일이면 거래일로 간주 (공휴일 오탐은 09:01에 빈 호가로 감지)
        return True
    except Exception:
        return True

def is_market_hours() -> bool:
    now = datetime.now()
    if now.hour < 9:
        return False
    if now.hour >= 15 and now.minute >= 20:
        return False
    return 9 <= now.hour <= 15


def _resolve_trading_mode(config) -> str:
    """Resolve TRADING_MODE from config, with PAPER_TRADING backward compat."""
    mode = getattr(config, "TRADING_MODE", None)
    if mode and mode in ("mock", "paper", "paper_test", "shadow_test", "live"):
        return mode
    # Fallback: derive from deprecated PAPER_TRADING
    if getattr(config, "PAPER_TRADING", True):
        logging.getLogger("gen4").warning(
            "[DEPRECATED_CONFIG] PAPER_TRADING is deprecated; use TRADING_MODE")
        return "paper"
    return "live"


def validate_trading_mode(trading_mode: str, server_type: str,
                          broker_connected: bool = True) -> None:
    """
    Hard gate: abort if trading_mode and server_type mismatch.

    TRADING_MODE is the operator's intended mode.
    server_type is the broker's actual connected environment.
    If they do not match, abort immediately.
      mock  = internal simulation only
      paper = broker mock trading
      live  = broker real trading

    Raises RuntimeError on mismatch.
    """
    _logger = logging.getLogger("gen4.live")

    if trading_mode == "mock":
        if broker_connected:
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=mock but broker is connected "
                f"(server_type={server_type}). Mock mode must not use broker.")
        return  # mock + no broker = OK

    if trading_mode == "shadow_test":
        if server_type not in ("MOCK", "REAL"):
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=shadow_test server_type={server_type}. "
                f"Shadow test requires MOCK or REAL server.")
        return  # shadow_test + any server = OK (read-only, no orders)

    if trading_mode in ("paper", "paper_test"):
        if server_type != "MOCK":
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode={trading_mode} server_type={server_type}. "
                f"Paper mode requires MOCK server (모의투자).")
        return  # paper/paper_test + MOCK = OK

    if trading_mode == "live":
        if server_type != "REAL":
            raise RuntimeError(
                f"[MODE_MISMATCH_ABORT] trading_mode=live server_type={server_type}. "
                f"Live mode requires REAL server.")
        return  # live + REAL = OK

    raise RuntimeError(f"[MODE_MISMATCH_ABORT] Unknown trading_mode={trading_mode!r}")


def _safe_save(state_mgr, portfolio, context: str = "",
               max_retries: int = 3, retry_delay: float = 0.5) -> bool:
    """Save portfolio state with retry and logging."""
    _logger = logging.getLogger("gen4.live")
    for attempt in range(1, max_retries + 1):
        saved = state_mgr.save_portfolio(portfolio.to_dict())
        if saved:
            _logger.info(f"[STATE_SAVE_OK] {context}")
            return True
        if attempt < max_retries:
            _logger.warning(f"[STATE_SAVE_RETRY] {context} — attempt {attempt}/{max_retries}")
            time.sleep(retry_delay)
    _logger.error(f"[STATE_SAVE_FAIL] {context} — {max_retries} attempts exhausted!")
    return False


def _save_test_reentry_meta(state_mgr, config, today_str, logger):
    """Save paper_test fast reentry metadata to runtime state."""
    try:
        delay = config.PAPER_TEST_REENTRY_DELAY_SEC
        ready_at = (datetime.now() + timedelta(seconds=delay)).isoformat()
        cycle_id = f"{today_str}_force"
        rt = state_mgr.load_runtime()
        rt["test_reentry_ready_at"] = ready_at
        rt["test_cycle_id"] = cycle_id
        rt["test_reentry_generated_at"] = datetime.now().isoformat()
        state_mgr.save_runtime(rt)
        logger.info(f"[PAPER_TEST_FAST_REENTRY] ready_at={ready_at} "
                    f"delay={delay}s cycle={cycle_id}")
    except Exception as e:
        logger.warning(f"[PAPER_TEST_REENTRY_SAVE_ERROR] {e}")


def _count_trading_days(start_date, end_date, config) -> int:
    """Count trading days between two dates.
    Priority: pykrx calendar (authoritative) → KOSPI.csv → weekday approx."""
    logger = logging.getLogger("gen4.live")
    s_str = start_date.strftime("%Y%m%d")
    e_str = end_date.strftime("%Y%m%d")

    # Method 1: pykrx — authoritative KRX trading calendar via 005930
    try:
        from pykrx import stock as pykrx_stock
        df = pykrx_stock.get_market_ohlcv(s_str, e_str, "005930")
        if len(df) > 0:
            count = sum(1 for d in df.index if d.strftime("%Y%m%d") > s_str)
            logger.info(f"[TRADING_DAYS] pykrx: {count} days ({s_str}~{e_str})")
            return count
    except Exception as e:
        logger.warning(f"[TRADING_DAYS] pykrx failed: {e}")

    # Method 2: KOSPI.csv file (may be stale)
    try:
        from report.kospi_utils import load_kospi_close
        if hasattr(config, "INDEX_FILE") and config.INDEX_FILE.exists():
            kospi = load_kospi_close(config.INDEX_FILE)
            if not kospi.empty:
                s = start_date.strftime("%Y-%m-%d")
                e = end_date.strftime("%Y-%m-%d")
                trading = [d for d in kospi.index if s < d <= e]
                logger.info(f"[TRADING_DAYS] KOSPI.csv: {len(trading)} days "
                            f"(last_date={kospi.index[-1]})")
                return len(trading)
    except Exception as e:
        logger.warning(f"[TRADING_DAYS] KOSPI.csv failed: {e}")

    # Method 3: Weekday approximation (last resort)
    cal_days = (end_date - start_date).days
    approx = int(cal_days * 5 / 7)
    logger.warning(f"[TRADING_DAYS] Fallback weekday approx: {approx} days")
    return approx


# ── Regime Observation (no trading logic impact) ─────────────────────────────
def _compute_regime_snapshot(config) -> tuple:
    """Compute market regime as observation only. NOT used for trading decisions.

    Uses INDEX_FILE (KOSPI daily closes) to calculate:
      - regime: "BULL" / "SIDE" / "BEAR"
      - kospi_ma200: 200-day moving average
      - breadth: fixed 0.5 (proper breadth requires batch-time universe scan)

    Returns (regime, kospi_ma200, breadth). Safe default on error.
    """
    _logger = logging.getLogger("gen4.live")
    try:
        import pandas as pd
        from strategy.regime_detector import calc_regime

        idx_df = pd.read_csv(config.INDEX_FILE)
        date_col = "index" if "index" in idx_df.columns else "date"
        idx_df = idx_df.rename(columns={date_col: "date"})
        close_col = "Close" if "Close" in idx_df.columns else "close"
        idx_df[close_col] = pd.to_numeric(idx_df[close_col], errors="coerce")
        closes = idx_df[close_col].dropna()

        if len(closes) < 200:
            _logger.warning("[REGIME_SNAPSHOT] Insufficient KOSPI data (%d < 200)", len(closes))
            return ("SIDE", 0.0, 0.5)

        kospi_close = float(closes.iloc[-1])
        kospi_ma200 = float(closes.iloc[-200:].mean())

        # R11 (2026-04-23): sanity check for mixed-scale KOSPI.csv corruption.
        # Root cause: pre-2026 rows had wrong values (~70000-120000 scale from
        # unknown ticker), 2026 rows appended at real KOSPI scale (~6400).
        # MA200 dominated by 198 bad historical + 2 good → ratio 0.086 → false BEAR.
        # If ratio falls outside plausible range [0.5, 2.0], treat KOSPI.csv as
        # corrupted and fall back to SIDE (neutral) instead of emitting BEAR/BULL
        # based on garbage MA200.
        if kospi_ma200 > 0:
            ratio = kospi_close / kospi_ma200
            if ratio < 0.5 or ratio > 2.0:
                _logger.error(
                    "[REGIME_SNAPSHOT] KOSPI.csv corruption detected — "
                    "kospi_close=%.0f / ma200=%.0f ratio=%.3f outside [0.5, 2.0]. "
                    "Falling back to SIDE (advisory). "
                    "Likely cause: mixed historical data; regenerate KOSPI.csv.",
                    kospi_close, kospi_ma200, ratio,
                )
                return ("SIDE", 0.0, 0.5)

        breadth = 0.5  # placeholder — proper breadth from batch-time universe
        regime = calc_regime(kospi_close, kospi_ma200, breadth)

        _logger.info("[REGIME_SNAPSHOT] %s  KOSPI=%.0f  MA200=%.0f  ratio=%.3f",
                     regime, kospi_close, kospi_ma200,
                     kospi_close / kospi_ma200 if kospi_ma200 > 0 else 0)
        return (regime, kospi_ma200, breadth)
    except Exception as e:
        _logger.warning("[REGIME_SNAPSHOT] Failed: %s — defaulting to SIDE", e)
        return ("SIDE", 0.0, 0.5)
