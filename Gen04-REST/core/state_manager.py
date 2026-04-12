"""
state_manager.py — Atomic state persistence
=============================================
Adapted from Gen3 (simplified for monthly rebalance).

Saves/loads:
  state/portfolio_state.json — positions, cash, equity, rebalance tracking
  state/runtime_state.json   — last run info

Atomic write: tmp -> verify -> backup -> rename
"""
from __future__ import annotations
import json
import logging
import os
import shutil
import time
import threading  # Phase 1-A: RLock for reentrant safety (JUG CONDITIONAL → resolved)
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.state")


class StateManager:
    """Atomic state persistence with mode-separated files.

    State files are separated by trading_mode to prevent cross-contamination:
      portfolio_state_mock.json   — mock mode only
      portfolio_state_paper.json  — paper mode only
      portfolio_state_live.json   — live mode only (no suffix = legacy live)

    Every write includes:
      write_origin: "engine" — identifies writer (REST write attempt → reject)
      version_seq: monotonic counter — detects out-of-order writes
    """

    # Valid trading modes
    VALID_MODES = ("mock", "paper", "paper_test", "shadow_test", "live")

    def __init__(self, state_dir: Path, trading_mode: str = "paper",
                 paper: bool = None):
        """
        Args:
            state_dir: directory for state files
            trading_mode: "mock" | "paper" | "live"
            paper: DEPRECATED — use trading_mode instead.
                   paper=True → trading_mode="paper", paper=False → trading_mode="live"
        """
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)

        # Backward compatibility: paper= → trading_mode
        if paper is not None:
            logger.warning("[DEPRECATED_PARAM] StateManager.paper is deprecated; "
                           "use trading_mode='mock'|'paper'|'live'")
            if trading_mode == "paper":  # default not explicitly overridden
                trading_mode = "paper" if paper else "live"

        if trading_mode not in self.VALID_MODES:
            raise ValueError(f"Invalid trading_mode={trading_mode!r}, "
                             f"must be one of {self.VALID_MODES}")

        self.trading_mode = trading_mode
        self._lock = threading.RLock()  # Phase 1-A: reentrant lock (JUG 권고)
        self._lock_contention_threshold = 0.1  # 100ms
        self._version_seq = 0  # monotonic write counter (세션 내)
        suffix = f"_{trading_mode}"
        self._portfolio_file = self.state_dir / f"portfolio_state{suffix}.json"
        self._runtime_file = self.state_dir / f"runtime_state{suffix}.json"

        # Migration: if new file doesn't exist, try legacy file
        self._migrate_legacy_state()

        logger.info(f"StateManager: mode={trading_mode.upper()}, "
                     f"file={self._portfolio_file.name}")

        # Restore version_seq from last saved state (세션 간 연속성)
        self._restore_version_seq()

    def _restore_version_seq(self) -> None:
        """기존 state 파일에서 version_seq 복원."""
        try:
            for path in (self._portfolio_file, self._runtime_file):
                if path.exists():
                    raw = json.loads(path.read_text(encoding="utf-8"))
                    seq = raw.get("_version_seq", 0)
                    if seq > self._version_seq:
                        self._version_seq = seq
            if self._version_seq > 0:
                logger.info(f"[VERSION_SEQ] Restored: {self._version_seq}")
        except Exception as e:
            logger.warning(f"[VERSION_SEQ] Restore failed: {e}")

    def _migrate_legacy_state(self) -> None:
        """Migrate from legacy file naming if new mode-specific file doesn't exist."""
        if self._portfolio_file.exists():
            return  # already has mode-specific file

        # Legacy mapping: paper=True → _paper, paper=False → no suffix
        legacy_map = {
            "mock":  "_paper",   # mock previously shared paper's file
            "paper": "_paper",   # paper was _paper
            "live":  "",         # live had no suffix
        }
        legacy_suffix = legacy_map.get(self.trading_mode, "")
        legacy_portfolio = self.state_dir / f"portfolio_state{legacy_suffix}.json"
        legacy_runtime = self.state_dir / f"runtime_state{legacy_suffix}.json"

        if legacy_portfolio.exists():
            import shutil
            shutil.copy2(legacy_portfolio, self._portfolio_file)
            logger.info(f"[STATE_MIGRATION] {legacy_portfolio.name} → "
                        f"{self._portfolio_file.name}")
        if legacy_runtime.exists() and not self._runtime_file.exists():
            import shutil
            shutil.copy2(legacy_runtime, self._runtime_file)
            logger.info(f"[STATE_MIGRATION] {legacy_runtime.name} → "
                        f"{self._runtime_file.name}")

    @contextmanager
    def _timed_lock(self):
        """Lock with contention measurement."""
        t0 = time.monotonic()
        self._lock.acquire()
        wait = time.monotonic() - t0
        if wait > self._lock_contention_threshold:
            logger.warning(f"[LOCK_CONTENTION] wait={wait:.3f}s")
        try:
            yield
        finally:
            self._lock.release()

    # ── Portfolio State ──────────────────────────────────────────────

    def save_portfolio(self, portfolio_data: dict) -> bool:
        """
        Atomically save portfolio state.
        portfolio_data must contain: cash, positions, peak_equity, etc.

        Positions are serialized using Position.to_dict() via
        PortfolioManager.to_dict(), so all fields (including current_price,
        entry_rank, score_mom) are preserved.  The legacy field-by-field
        extraction was removed (C1 fix) to prevent field-list drift.
        """
        with self._timed_lock():
            data = {
                "timestamp": datetime.now().isoformat(),
                "version": "4.1",
                **portfolio_data,
            }

            # Positions should already be dicts (from PortfolioManager.to_dict()).
            # If a caller passes raw Position objects, convert them.
            if "positions" in data:
                pos_dict = {}
                for code, pos in data["positions"].items():
                    if isinstance(pos, dict):
                        # Already serialized by Position.to_dict() — use as-is
                        pos_dict[code] = pos
                    else:
                        # Fallback: raw Position object (shouldn't happen)
                        pos_dict[code] = pos.to_dict() if hasattr(pos, "to_dict") else pos
                data["positions"] = pos_dict

            return self._atomic_write(self._portfolio_file, data)

    def load_portfolio(self) -> Optional[dict]:
        """Load portfolio state. Returns None if not found."""
        with self._timed_lock():
            data = self._atomic_read(self._portfolio_file)
            if data is None:
                return None

            logger.info(f"Loaded portfolio: {len(data.get('positions', {}))} positions, "
                         f"cash={data.get('cash', 0):,.0f}")
            return data

    # ── Runtime State ────────────────────────────────────────────────

    def save_runtime(self, state: dict) -> bool:
        """Save runtime metadata."""
        with self._timed_lock():
            data = {
                "timestamp": datetime.now().isoformat(),
                **state,
            }
            return self._atomic_write(self._runtime_file, data)

    def load_runtime(self) -> dict:
        """Load runtime state. Returns empty dict if not found."""
        with self._timed_lock():
            return self._atomic_read(self._runtime_file) or {}

    # ── Rebalance Tracking ───────────────────────────────────────────

    def get_last_rebalance_date(self) -> Optional[str]:
        """Get last rebalance date from runtime state."""
        rt = self.load_runtime()
        return rt.get("last_rebalance_date")

    def set_last_rebalance_date(self, dt_str: str) -> None:
        """Update last rebalance date."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["last_rebalance_date"] = dt_str
            rt["rebalance_count"] = rt.get("rebalance_count", 0) + 1
            self._atomic_write(self._runtime_file,
                               {"timestamp": datetime.now().isoformat(), **rt})

    # ── Pending Buys (T+1 model) ────────────────────────────────────

    def save_pending_buys(self, buys: list, sell_status: str = "COMPLETE") -> bool:
        """Save pending buy orders and rebalance sell status to runtime state."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["pending_buys"] = buys
            rt["rebal_sell_status"] = sell_status
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    def load_pending_buys(self) -> tuple:
        """Load pending buys and sell status. Returns (list, str)."""
        rt = self.load_runtime()
        return rt.get("pending_buys", []), rt.get("rebal_sell_status", "")

    def clear_pending_buys(self) -> bool:
        """Clear pending buys after execution or expiry."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["pending_buys"] = []
            rt["rebal_sell_status"] = ""
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    # ── Pending External Orders ──────────────────────────────────────

    def save_pending_external(self, orders: list) -> bool:
        """Save PENDING_EXTERNAL order info for reconcile after restart."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["pending_external_orders"] = orders
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    def load_pending_external(self) -> list:
        """Load PENDING_EXTERNAL orders. Returns empty list if none."""
        rt = self.load_runtime()
        return rt.get("pending_external_orders", [])

    def clear_pending_external(self) -> bool:
        """Clear PENDING_EXTERNAL orders after settlement."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["pending_external_orders"] = []
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    # ── Recovery State (exposure_guard 영속화) ──────────────────

    def save_guard_state(self, guard_state: dict) -> bool:
        """ExposureGuard recovery state를 runtime에 영속화."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["guard_state"] = {
                "timestamp": datetime.now().isoformat(),
                **guard_state,
            }
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    def load_guard_state(self) -> dict:
        """ExposureGuard recovery state 복원. 없으면 빈 dict."""
        rt = self.load_runtime()
        return rt.get("guard_state", {})

    # ── Shutdown Reason (dirty exit detection) ──────────────────

    def mark_startup(self) -> bool:
        """Mark session as running (dirty). Call at startup BEFORE trading."""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["shutdown_reason"] = "running"
            rt["session_start"] = datetime.now().isoformat()
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    def mark_shutdown(self, reason: str = "normal") -> bool:
        """Mark clean shutdown. reason: 'normal' | 'sigint' | 'eod_complete'"""
        with self._timed_lock():
            rt = self._atomic_read(self._runtime_file) or {}
            rt["shutdown_reason"] = reason
            rt["session_end"] = datetime.now().isoformat()
            return self._atomic_write(self._runtime_file,
                                      {"timestamp": datetime.now().isoformat(), **rt})

    def get_last_shutdown_reason(self) -> str:
        """Get last shutdown reason. 'running' = dirty exit (crash/power loss)."""
        rt = self.load_runtime()
        return rt.get("shutdown_reason", "unknown")

    def was_dirty_exit(self) -> bool:
        """True if last session didn't shut down cleanly."""
        reason = self.get_last_shutdown_reason()
        return reason in ("running", "unknown")

    # ── Atomic I/O ───────────────────────────────────────────────────

    def _atomic_write(self, path: Path, data: dict) -> bool:
        """
        Atomic write: tmp -> verify -> backup -> rename.
        Prevents corruption on crash/power loss.

        Automatically injects write_origin="engine" and version_seq.
        """
        tmp = path.with_suffix(".tmp")
        bak = path.with_suffix(".bak")

        try:
            # Provenance: every write is stamped
            self._version_seq += 1
            data["_write_origin"] = "engine"
            data["_version_seq"] = self._version_seq
            data["_write_ts"] = datetime.now().isoformat()

            # 1. Write to temp
            content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
            tmp.write_text(content, encoding="utf-8")

            # 2. Verify temp is valid JSON
            verify = json.loads(tmp.read_text(encoding="utf-8"))
            if not isinstance(verify, dict):
                raise ValueError("Verification failed: not a dict")

            # 3. Backup existing
            if path.exists():
                shutil.copy2(path, bak)

            # 4. Atomic rename (os.replace is atomic on all platforms,
            #    including Windows where it overwrites existing file)
            os.replace(str(tmp), str(path))

            return True

        except Exception as e:
            logger.error(f"Atomic write failed for {path.name}: {e}")
            # Cleanup temp
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            return False

    def _atomic_read(self, path: Path) -> Optional[dict]:
        """Read with backup fallback."""
        primary_failed = False
        for p in [path, path.with_suffix(".bak")]:
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        if primary_failed:
                            logger.warning(
                                f"[STATE_BACKUP_USED] Primary {path.name} failed, "
                                f"loaded from {p.name}")
                        return data
                except Exception as e:
                    logger.warning(f"Failed to read {p.name}: {e}")
                    primary_failed = True
            else:
                primary_failed = True
        return None
