"""LifecycleHarness — minimal scaffolding for RG5 lifecycle invariant tests.

Phase 1 scope (intentionally narrow per JUG directive):
  - Real StateManager pointed at tmp_path (no live state contamination)
  - shutdown_reason transitions: startup / normal / sigint / eod_complete / crash
  - restart() simulates fresh process (re-instantiate state_mgr from disk)
  - Mock provider/executor (no live API)
  - Batch config helpers (for duplicate batch tests with PR 3 lock helpers)

NOT in scope yet (deferred to later RG5 phases):
  - Running monitor / rebalance / EOD phases end-to-end
  - Order executor fill flow simulation
  - Full RECON phase simulation
  - Preview snapshot binding tests (depends on the next-PR feature itself)

Filename note: this is `conftest_lifecycle.py` (not `conftest.py`) per JUG
spec — pytest does NOT auto-discover this. Test files import the class
explicitly via `from conftest_lifecycle import LifecycleHarness`.

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_lifecycle_*.py -v
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

KR_ROOT = Path(__file__).resolve().parent.parent
if str(KR_ROOT) not in sys.path:
    sys.path.insert(0, str(KR_ROOT))


class LifecycleHarness:
    """In-memory, tmp_path-isolated lifecycle simulator.

    Constructed with a pytest tmp_path fixture. Builds a real
    StateManager (so we exercise actual state file format + atomic write
    + version_seq logic) but writes only into the tmp dir, never the
    live state path. provider/executor are MagicMocks — never touch the
    real broker.

    Typical usage::

        def test_something(tmp_path):
            h = LifecycleHarness(tmp_path)
            h.mark_startup()
            # ... simulate work ...
            h.normal_shutdown()
            assert h.state_mgr.was_dirty_exit() is False
    """

    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.state_dir = tmp_path / "state"
        self.state_dir.mkdir()
        self.ohlcv_dir = tmp_path / "ohlcv"
        self.ohlcv_dir.mkdir()
        self.config = SimpleNamespace(
            OHLCV_DIR=self.ohlcv_dir,
            INITIAL_CASH=100_000_000,
            DAILY_DD_LIMIT=-0.04,
            MONTHLY_DD_LIMIT=-0.07,
            N_STOCKS=20,
            BUY_COST=0.00115,
            SELL_COST=0.00295,
            CASH_BUFFER_RATIO=0.01,
        )
        self.state_mgr = self._make_state_mgr()
        self.provider = MagicMock()
        self.executor = MagicMock()
        self.logger = logging.getLogger("test.lifecycle_harness")

    def _make_state_mgr(self):
        """Build a fresh StateManager pointed at tmp state_dir.

        backup_dirs=[] disables off-disk mirroring — important for test
        isolation; otherwise StateManager would try to mirror writes to
        the configured external path (env QTRON_STATE_BACKUP_DIRS).
        """
        from core.state_manager import StateManager
        return StateManager(
            state_dir=self.state_dir,
            trading_mode="paper_test",
            backup_dirs=[],
        )

    # ── Shutdown reason simulators ────────────────────────────────────

    def mark_startup(self) -> bool:
        """Simulate session start. Sets shutdown_reason='running'."""
        return self.state_mgr.mark_startup()

    def normal_shutdown(self) -> bool:
        """Simulate clean shutdown."""
        return self.state_mgr.mark_shutdown("normal")

    def sigint_shutdown(self) -> bool:
        """Simulate SIGINT (Ctrl+C) clean exit."""
        return self.state_mgr.mark_shutdown("sigint")

    def eod_complete_shutdown(self) -> bool:
        """Simulate EOD-complete clean shutdown."""
        return self.state_mgr.mark_shutdown("eod_complete")

    def crash(self) -> None:
        """Simulate dirty exit: reason stays 'running' (no clean shutdown).

        Real-world equivalents:
          - SIGKILL / kill -9
          - power loss
          - segfault before signal handler
          - hard reboot

        Implementation: we explicitly write shutdown_reason='running'
        (mimics the state mark_startup leaves) and clear session_end so
        was_dirty_exit() will return True on next load.
        """
        rt = self.state_mgr.load_runtime() or {}
        rt["shutdown_reason"] = "running"
        rt["session_end"] = ""
        self.state_mgr.save_runtime(rt)

    def restart(self):
        """Simulate fresh process — re-instantiate StateManager from disk.

        Returns the new StateManager so callers can inspect was_dirty_exit
        immediately. Old self.state_mgr is replaced.
        """
        self.state_mgr = self._make_state_mgr()
        return self.state_mgr

    # ── RECON outcome simulator (Phase 1 — monitor_only persistence) ──

    def run_recon(self, *, status: str = "OK", reason: str = "") -> dict:
        """Simulate a RECON phase outcome by writing runtime fields.

        Does NOT call the real reconcile pathway — that's broker/state
        coupling beyond Phase 1 scope. Instead writes the runtime fields
        the engine would set so subsequent gate checks (web rebal,
        rebalance_phase) see the intended state.

        Args:
            status:
              'OK'      — clear monitor_only_reason + recon_unreliable
              'PARTIAL' — set monitor_only_reason='holdings_unreliable',
                          recon_unreliable=True (mimics RECON PARTIAL
                          snapshot from kt00018 paginated query)
              'ERROR'   — set monitor_only_reason=reason or 'recon_error',
                          recon_unreliable=True (broker query exception path)
            reason:    custom reason string (used with status='ERROR' or
                       to override the default for 'PARTIAL')

        Returns the post-RECON runtime dict for inspection.

        IMPORTANT: monitor_only_reason is SESSION-SCOPED per JUG decision —
        only restart clears it. This simulator does the write; whether the
        next session sees it cleared depends on whether RECON('OK') is run
        AFTER restart, not before.
        """
        from datetime import datetime as _dt
        if status not in ("OK", "PARTIAL", "ERROR"):
            raise ValueError(f"Unknown RECON status: {status!r}")

        rt = self.state_mgr.load_runtime() or {}
        if status == "OK":
            rt.pop("monitor_only_reason", None)
            rt.pop("recon_unreliable", None)
            rt.pop("monitor_only_set_at", None)
        elif status == "PARTIAL":
            rt["monitor_only_reason"] = reason or "holdings_unreliable"
            rt["recon_unreliable"] = True
            rt["monitor_only_set_at"] = _dt.now().isoformat(timespec="seconds")
        elif status == "ERROR":
            rt["monitor_only_reason"] = reason or "recon_error"
            rt["recon_unreliable"] = True
            rt["monitor_only_set_at"] = _dt.now().isoformat(timespec="seconds")
        self.state_mgr.save_runtime(rt)
        return rt

    def force_monitor_only(self, reason: str) -> dict:
        """Set monitor_only_reason directly (for tests that don't need the
        full RECON-status framing). Lower-level than run_recon — exposes
        the raw runtime field write for entry-condition tests."""
        from datetime import datetime as _dt
        rt = self.state_mgr.load_runtime() or {}
        rt["monitor_only_reason"] = reason
        rt["monitor_only_set_at"] = _dt.now().isoformat(timespec="seconds")
        self.state_mgr.save_runtime(rt)
        return rt

    # ── Batch lock fixtures (PR 3 integration) ────────────────────────

    def batch_config(self) -> SimpleNamespace:
        """Config object compatible with PR 3 batch lock helpers.

        Returns the same SimpleNamespace as self.config — sufficient
        because the lock helpers only use config.OHLCV_DIR.
        """
        return self.config

    def lock_path(self) -> Path:
        """Convenience: return the path PR 3 helpers will use for the lock file."""
        return Path(self.config.OHLCV_DIR).parent / "batch.lock"

    def checkpoint_path(self) -> Path:
        """Convenience: return the path PR 3 / batch.py uses for the checkpoint."""
        return Path(self.config.OHLCV_DIR).parent / "batch_checkpoint.json"
