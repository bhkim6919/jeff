"""
rest_logger.py — REST API 전용 로깅 설정
=========================================
모든 REST/WebSocket 호출을 파일 + 콘솔에 기록.
Gen04 LIVE 로그와 완전 분리.
"""
from __future__ import annotations

import logging
import sys
from datetime import date
from pathlib import Path

_initialized = False


def setup_rest_logging(log_dir: str = None, level: int = logging.INFO) -> None:
    """REST API 전용 로깅 초기화. 1회만 실행."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    if log_dir is None:
        log_dir = str(Path(__file__).resolve().parent.parent / "data" / "logs")

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    today = date.today().strftime("%Y%m%d")
    log_file = log_path / f"rest_api_{today}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # Root logger for gen4.rest.* namespace
    rest_root = logging.getLogger("gen4.rest")
    rest_root.setLevel(level)

    # File handler (UTF-8, append)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    rest_root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    rest_root.addHandler(ch)

    rest_root.info(f"[REST_LOG] Initialized: {log_file}")
