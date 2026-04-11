# -*- coding: utf-8 -*-
"""
state_manager.py — Atomic State Persistence for Q-TRON US
==========================================================
- Paired save: portfolio + runtime share saved_at + version_seq
- Atomic write: tmp → read-back verify → bak → rename
- Dirty exit detection via runtime started_at / shutdown_at
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("qtron.us.state")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class StateManagerUS:
    """
    Manages portfolio_state_us_{mode}.json and runtime_state_us_{mode}.json.

    All saves go through save_all() to keep both files at the same
    saved_at + version_seq — no partial snapshots.
    """

    def __init__(self, state_dir: str | Path, trading_mode: str = "paper"):
        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._mode = trading_mode

        self._portfolio_path = self._state_dir / f"portfolio_state_us_{trading_mode}.json"
        self._runtime_path = self._state_dir / f"runtime_state_us_{trading_mode}.json"

        self._lock = threading.RLock()
        self._version_seq = 0

        # Load existing seq if present
        rt = self._load_json(self._runtime_path)
        if rt:
            self._version_seq = rt.get("version_seq", 0)

    # ── Atomic Write ────────────────────────────────────────

    def _load_json(self, path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[STATE] Load failed {path.name}: {e}")
            return None

    def _atomic_write(self, path: Path, data: dict) -> bool:
        """tmp → read-back verify → bak → rename"""
        tmp_path = path.with_suffix(".tmp")
        bak_path = path.with_suffix(".bak")

        try:
            # 1. Write to tmp
            content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)

            # 2. Read-back verify
            with open(tmp_path, "r", encoding="utf-8") as f:
                verify = json.load(f)
            if verify.get("version_seq") != data.get("version_seq"):
                logger.error(f"[STATE] Verify failed: seq mismatch in {path.name}")
                return False

            # 3. Backup existing
            if path.exists():
                shutil.copy2(path, bak_path)

            # 4. Rename (atomic on most OS)
            os.replace(str(tmp_path), str(path))
            return True

        except Exception as e:
            logger.error(f"[STATE] Atomic write failed {path.name}: {e}")
            # Cleanup tmp
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    # ── Paired Save ─────────────────────────────────────────

    def _next_seq(self) -> int:
        self._version_seq += 1
        return self._version_seq

    def save_all(self, portfolio_data: dict, runtime_data: dict) -> bool:
        """Save portfolio + runtime with shared saved_at and version_seq."""
        with self._lock:
            ts = _now_iso()
            seq = self._next_seq()

            portfolio_data["saved_at"] = ts
            portfolio_data["version_seq"] = seq
            runtime_data["saved_at"] = ts
            runtime_data["version_seq"] = seq

            ok_p = self._atomic_write(self._portfolio_path, portfolio_data)
            ok_r = self._atomic_write(self._runtime_path, runtime_data)

            if ok_p and ok_r:
                logger.debug(f"[STATE] save_all seq={seq}")
            else:
                logger.error(f"[STATE] save_all partial failure: portfolio={ok_p} runtime={ok_r}")

            return ok_p and ok_r

    # ── Load ────────────────────────────────────────────────

    def load_portfolio(self) -> Optional[dict]:
        with self._lock:
            return self._load_json(self._portfolio_path)

    def load_runtime(self) -> Optional[dict]:
        with self._lock:
            return self._load_json(self._runtime_path)

    # ── Rebalance Date ──────────────────────────────────────

    def get_last_rebalance_date(self) -> str:
        rt = self.load_runtime()
        if rt:
            return rt.get("last_rebalance_date", "")
        return ""

    def set_last_rebalance_date(self, date_str: str) -> None:
        # Will be saved via save_all in the next cycle
        pass  # Caller updates runtime_data dict directly

    # ── Lifecycle ───────────────────────────────────────────

    def mark_startup(self) -> dict:
        """Record startup in runtime. Returns runtime_data for save_all."""
        return {
            "started_at": _now_iso(),
            "pid": os.getpid(),
            "shutdown_at": "",
            "shutdown_reason": "",
            "mode": self._mode,
        }

    def mark_shutdown(self, reason: str = "normal") -> dict:
        """Record shutdown in runtime. Returns runtime_data for save_all."""
        rt = self.load_runtime() or {}
        rt["shutdown_at"] = _now_iso()
        rt["shutdown_reason"] = reason
        return rt

    def was_dirty_exit(self) -> bool:
        """True if started_at exists but shutdown_at is missing."""
        rt = self.load_runtime()
        if not rt:
            return False
        started = rt.get("started_at", "")
        shutdown = rt.get("shutdown_at", "")
        return bool(started and not shutdown)

    # ── Paths (for external reference) ──────────────────────

    @property
    def portfolio_path(self) -> Path:
        return self._portfolio_path

    @property
    def runtime_path(self) -> Path:
        return self._runtime_path
