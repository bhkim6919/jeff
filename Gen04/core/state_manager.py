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
    """

    # Valid trading modes
    VALID_MODES = ("mock", "paper", "live")

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
        suffix = f"_{trading_mode}"
        self._portfolio_file = self.state_dir / f"portfolio_state{suffix}.json"
        self._runtime_file = self.state_dir / f"runtime_state{suffix}.json"

        # Migration: if new file doesn't exist, try legacy file
        self._migrate_legacy_state()

        logger.info(f"StateManager: mode={trading_mode.upper()}, "
                     f"file={self._portfolio_file.name}")

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

    # ── Portfolio State ──────────────────────────────────────────────

    def save_portfolio(self, portfolio_data: dict) -> bool:
        """
        Atomically save portfolio state.
        portfolio_data must contain: cash, positions, peak_equity, etc.
        """
        data = {
            "timestamp": datetime.now().isoformat(),
            "version": "4.0",
            **portfolio_data,
        }

        # Serialize positions
        if "positions" in data:
            pos_dict = {}
            for code, pos in data["positions"].items():
                pos_dict[code] = {
                    "code": code,
                    "quantity": pos.get("quantity", pos.get("qty", 0)),
                    "avg_price": pos.get("avg_price", pos.get("entry_price", 0)),
                    "entry_date": str(pos.get("entry_date", "")),
                    "high_watermark": pos.get("high_watermark", pos.get("high_wm", 0)),
                    "trail_stop_price": pos.get("trail_stop_price", 0),
                    "sector": pos.get("sector", ""),
                }
            data["positions"] = pos_dict

        return self._atomic_write(self._portfolio_file, data)

    def load_portfolio(self) -> Optional[dict]:
        """Load portfolio state. Returns None if not found."""
        data = self._atomic_read(self._portfolio_file)
        if data is None:
            return None

        logger.info(f"Loaded portfolio: {len(data.get('positions', {}))} positions, "
                     f"cash={data.get('cash', 0):,.0f}")
        return data

    # ── Runtime State ────────────────────────────────────────────────

    def save_runtime(self, state: dict) -> bool:
        """Save runtime metadata."""
        data = {
            "timestamp": datetime.now().isoformat(),
            **state,
        }
        return self._atomic_write(self._runtime_file, data)

    def load_runtime(self) -> dict:
        """Load runtime state. Returns empty dict if not found."""
        return self._atomic_read(self._runtime_file) or {}

    # ── Rebalance Tracking ───────────────────────────────────────────

    def get_last_rebalance_date(self) -> Optional[str]:
        """Get last rebalance date from runtime state."""
        rt = self.load_runtime()
        return rt.get("last_rebalance_date")

    def set_last_rebalance_date(self, dt_str: str) -> None:
        """Update last rebalance date."""
        rt = self.load_runtime()
        rt["last_rebalance_date"] = dt_str
        rt["rebalance_count"] = rt.get("rebalance_count", 0) + 1
        self.save_runtime(rt)

    # ── Atomic I/O ───────────────────────────────────────────────────

    def _atomic_write(self, path: Path, data: dict) -> bool:
        """
        Atomic write: tmp -> verify -> backup -> rename.
        Prevents corruption on crash/power loss.
        """
        tmp = path.with_suffix(".tmp")
        bak = path.with_suffix(".bak")

        try:
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
        for p in [path, path.with_suffix(".bak")]:
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        return data
                except Exception as e:
                    logger.warning(f"Failed to read {p.name}: {e}")
        return None
