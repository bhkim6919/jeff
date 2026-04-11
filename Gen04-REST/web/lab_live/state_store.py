"""
state_store.py -- Persistent state for Lab Live
==================================================
Atomic JSON write. 서버 재시작 후 포지션/equity 복원.
"""
from __future__ import annotations
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("lab_live.state")


def atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic JSON write: tmp -> fsync -> replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    # backup
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak")
        try:
            if bak.exists():
                bak.unlink()
            path.rename(bak)
        except OSError:
            pass
    os.replace(str(tmp), str(path))


def safe_read_json(path: Path) -> Optional[dict]:
    """JSON read with .bak fallback."""
    for p in [path, path.with_suffix(path.suffix + ".bak")]:
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
    return None


def save_state(lanes: dict, state_file: Path) -> None:
    """전체 Lab Live 상태 저장."""
    state = {
        "last_run_date": datetime.now().strftime("%Y-%m-%d"),
        "last_run_ts": datetime.now().isoformat(),
        "lanes": {},
    }
    for name, lane in lanes.items():
        state["lanes"][name] = {
            "cash": lane["cash"],
            "positions": lane["positions"],
            "pending_buys": lane.get("pending_buys", []),
            "last_rebal_idx": lane.get("last_rebal_idx", -999),
            "equity_history": lane.get("equity_history", []),
        }
    atomic_write_json(state_file, state)
    logger.info(f"[LAB_LIVE] State saved: {len(lanes)} lanes")


def load_state(state_file: Path) -> Optional[dict]:
    """저장된 상태 복원."""
    data = safe_read_json(state_file)
    if data:
        logger.info(f"[LAB_LIVE] State loaded: last_run={data.get('last_run_date')}")
    return data


def save_trades(trades: list, trades_file: Path) -> None:
    """거래 이력 저장 (append-friendly)."""
    existing = []
    if trades_file.exists():
        try:
            existing = json.loads(trades_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    existing.extend(trades)
    atomic_write_json(trades_file, existing)


def load_trades(trades_file: Path) -> list:
    """거래 이력 로드."""
    if not trades_file.exists():
        return []
    try:
        return json.loads(trades_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def append_equity(equity_row: dict, equity_file: Path) -> None:
    """Equity history CSV에 한 줄 추가."""
    import csv
    equity_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not equity_file.exists()
    with open(equity_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date"] + sorted(
            [k for k in equity_row.keys() if k != "date"]))
        if write_header:
            writer.writeheader()
        writer.writerow(equity_row)
