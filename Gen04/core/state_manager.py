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

    def __init__(self, state_dir: Path, paper: bool = True):
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)

        suffix = "_paper" if paper else ""
        self._portfolio_file = self.state_dir / f"portfolio_state{suffix}.json"
        self._runtime_file = self.state_dir / f"runtime_state{suffix}.json"

        logger.info(f"StateManager: {'PAPER' if paper else 'LIVE'}, "
                     f"file={self._portfolio_file.name}")

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

            # 4. Atomic rename
            if os.name == "nt":
                # Windows: can't rename over existing, so remove first
                if path.exists():
                    path.unlink()
            os.rename(str(tmp), str(path))

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
