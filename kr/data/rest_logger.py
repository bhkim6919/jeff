"""
rest_logger.py -- REST API logging with daily rotation + auto-cleanup
=====================================================================
- Daily log file: rest_api_YYYYMMDD.log
- Auto-delete files older than RETENTION_DAYS (default 30)
- gen4.rest.* namespace (kr-legacy LIVE log와 분리)
- gen4.live / gen4.state 등 전체 로거에도 동일 rotation 적용
"""
from __future__ import annotations

import glob
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_initialized = False

# Log retention: 30 days
RETENTION_DAYS = 30


class DailyFileHandler(logging.FileHandler):
    """Daily log file handler with midnight rollover detection.

    Unlike TimedRotatingFileHandler, this uses a simple date-check approach
    that works correctly across process restarts (no need for continuous run).
    """

    def __init__(self, log_dir: Path, prefix: str = "rest_api",
                 encoding: str = "utf-8"):
        self._log_dir = log_dir
        self._prefix = prefix
        self._current_date = date.today().strftime("%Y%m%d")
        log_file = log_dir / f"{prefix}_{self._current_date}.log"
        super().__init__(str(log_file), encoding=encoding, mode="a")

    def emit(self, record):
        today = date.today().strftime("%Y%m%d")
        if today != self._current_date:
            self._rollover(today)
        super().emit(record)

    def _rollover(self, new_date: str):
        """Close current file and open new date's file."""
        self.close()
        self._current_date = new_date
        new_file = self._log_dir / f"{self._prefix}_{new_date}.log"
        self.baseFilename = str(new_file)
        self.stream = self._open()


def setup_rest_logging(log_dir: str = None, level: int = logging.INFO) -> None:
    """REST API + engine logging initialization. Once only.

    Sets up:
      - gen4.rest.*  (REST API / WebSocket)
      - gen4.live    (engine lifecycle)
      - gen4.state   (state manager)
      - gen4.crosscheck (cross validator)
    """
    global _initialized
    if _initialized:
        return
    _initialized = True

    if log_dir is None:
        log_dir = str(Path(__file__).resolve().parent.parent / "data" / "logs")

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    # Daily rotating file handler
    fh = DailyFileHandler(log_path, prefix="rest_api")
    fh.setLevel(level)
    fh.setFormatter(formatter)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    # Apply to all gen4.* loggers
    for name in ("gen4.rest", "gen4.live", "gen4.state", "gen4.crosscheck",
                 "gen4.dual_read"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.addHandler(fh)
        lg.addHandler(ch)

    today = date.today().strftime("%Y%m%d")
    log_file = log_path / f"rest_api_{today}.log"
    logging.getLogger("gen4.rest").info(f"[REST_LOG] Initialized: {log_file}")

    # Auto-cleanup old logs
    _cleanup_old_logs(log_path)


def _cleanup_old_logs(log_dir: Path, retention_days: int = RETENTION_DAYS) -> int:
    """Delete log files older than retention_days. Returns count deleted."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0

    for pattern in ("rest_api_*.log", "gen4_*.log"):
        for f in log_dir.glob(pattern):
            try:
                # Extract date from filename: rest_api_YYYYMMDD.log
                stem = f.stem  # rest_api_20260411
                date_part = stem.split("_")[-1]
                if len(date_part) == 8 and date_part.isdigit():
                    file_date = datetime.strptime(date_part, "%Y%m%d")
                    if file_date < cutoff:
                        f.unlink()
                        deleted += 1
            except (ValueError, OSError):
                continue

    if deleted > 0:
        logging.getLogger("gen4.rest").info(
            f"[LOG_CLEANUP] Deleted {deleted} log files older than {retention_days} days"
        )
    return deleted
