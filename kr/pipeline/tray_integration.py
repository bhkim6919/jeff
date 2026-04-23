# -*- coding: utf-8 -*-
"""kr/pipeline/tray_integration.py — Tray ↔ Orchestrator glue (Phase 3).

Keeps tray_server.py changes to two lines: one import, one per-tick call.
The heavy lifting (state/backoff/mode/steps/mirror) stays in the pipeline
package so tray can remain focused on tray concerns.

Toggle semantics via env var `QTRON_PIPELINE`:
    unset | "0"  → fully disabled, zero side effects (default; safe)
    "1"          → SHADOW mode — orchestrator ticks alongside legacy
                   auto-triggers. Writes pipeline state JSON + PG mirror
                   but does NOT prevent tray's legacy scheduling. Use
                   for 1-week observation per plan §3.1.
    "2"          → PRIMARY mode — orchestrator owns scheduling. Legacy
                   tray auto-triggers should check `is_primary()` and
                   skip. Reserved for Phase 5 cutover; not wired in Phase 3.

The tray never constructs an Orchestrator directly — it calls
`tick_if_enabled(holder)` which lazy-builds on first use.

Implementation notes:
    - `bootstrap_env()` runs exactly once per process on first tick; its
      DONE status persists in today's state file.
    - Step list is intentionally ordered for logging readability; actual
      execution order is DAG-driven via preconditions.
    - `default_data_dir()` resolves to `<repo>/kr/data/pipeline/` matching
      the plan. Created lazily if missing.
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Optional

from .bootstrap import BootstrapError, bootstrap_env
from .mode import detect_mode
from .orchestrator import Orchestrator
from .schema import STATUS_DONE
from .state import PipelineState
from .steps import (
    BackupStep,
    BatchStep,
    GateObserverStep,
    LabEodKrStep,
    LabEodUsStep,
)

_log = logging.getLogger("gen4.pipeline.tray_integration")

# Env var drives the toggle — see module docstring.
ENV_TOGGLE = "QTRON_PIPELINE"

# Module-level lock so a naive double-call from tray (rare but possible
# during shutdown) can't race on lazy construction.
_build_lock = threading.Lock()


def is_enabled() -> bool:
    """True if `QTRON_PIPELINE` is set to a non-"0" non-empty value."""
    v = os.environ.get(ENV_TOGGLE, "").strip()
    return bool(v) and v != "0"


def is_primary() -> bool:
    """True when the toggle is '2' or higher — orchestrator is authoritative.

    Phase 3 ships SHADOW only; tray code that wants to gate its legacy
    auto-triggers under PRIMARY may call this, but the orchestrator side
    works regardless.
    """
    v = os.environ.get(ENV_TOGGLE, "").strip()
    try:
        return int(v) >= 2
    except (TypeError, ValueError):
        return False


def default_data_dir(repo_root: Optional[Path] = None) -> Path:
    """Resolve `<repo>/kr/data/pipeline/`, creating it if absent."""
    if repo_root is None:
        # This file lives at <repo>/kr/pipeline/tray_integration.py
        # parents[0]=pipeline, parents[1]=kr, parents[2]=<repo>
        kr_root = Path(__file__).resolve().parents[1]
    else:
        kr_root = Path(repo_root) / "kr"
    data_dir = kr_root / "data" / "pipeline"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def default_steps():
    """Return the standard step list in logical execution order.

    The orchestrator's DAG enforcement is via per-step `preconditions`;
    this order is what gets evaluated first in each tick — useful for
    log locality but not correctness.
    """
    return [
        BatchStep(),
        LabEodKrStep(),
        LabEodUsStep(),
        GateObserverStep(),
        BackupStep(),
    ]


class OrchestratorHolder:
    """Lazy one-shot construction wrapper used by tray_server.

    Tray holds one instance for the process lifetime; first `tick()` call
    builds the Orchestrator. Thread-safe (double-checked + module lock).
    """

    def __init__(self):
        self._orch: Optional[Orchestrator] = None
        self._last_tick_summary: Optional[dict] = None

    def _build(self) -> Orchestrator:
        data_dir = default_data_dir()
        mode = detect_mode()
        return Orchestrator(
            data_dir=data_dir,
            steps=default_steps(),
            mode=mode,
        )

    def get(self) -> Optional[Orchestrator]:
        """Build on first call; return None if env toggle is off."""
        if not is_enabled():
            return None
        if self._orch is not None:
            return self._orch
        with _build_lock:
            if self._orch is None:
                try:
                    self._orch = self._build()
                    _log.info(
                        "[PIPELINE_TRAY_BUILD] data_dir=%s steps=%s mode=%s",
                        self._orch._data_dir,
                        [s.name for s in self._orch._steps],
                        self._orch._mode,
                    )
                except Exception as e:  # noqa: BLE001
                    _log.exception(
                        "[PIPELINE_TRAY_BUILD_FAIL] %s — orchestrator disabled",
                        e,
                    )
                    return None
        return self._orch

    def _record_bootstrap(self, orch: Orchestrator) -> None:
        """Record bootstrap_env outcome idempotently per trading day.

        Checks today's state file first; if bootstrap_env is already DONE,
        skip. Otherwise run bootstrap_env and record outcome. This replaces
        the prior per-process flag which did not reset on date rollover
        and silently blocked all downstream steps (see incident 2026-04-22:
        lab_eod_us missed its 60-second window because tray never restarted
        across midnight KST).

        Uses strict=False so a local env issue doesn't kill the tray —
        instead the orchestrator records FAILED and downstream preconditions
        block as designed (with fail_count driving backoff retries).
        """
        try:
            state = PipelineState.load_or_create_today(
                data_dir=orch._data_dir,
                mode=orch._mode,
            )
            existing = state.steps.get("bootstrap_env")
            if existing is not None and existing.status == STATUS_DONE:
                return
            checks = bootstrap_env(data_dir=orch._data_dir, strict=False)
            if all(checks.values()):
                state.mark_done("bootstrap_env", details=checks)
            else:
                state.mark_failed(
                    "bootstrap_env",
                    f"env checks failed: {checks}",
                )
            state.save()
            try:
                from . import pg_mirror
                pg_mirror.mirror_step(state, "bootstrap_env")
            except Exception as e:  # noqa: BLE001
                _log.warning(
                    "[PIPELINE_PG_MIRROR_SKIP] step=bootstrap_env err=%s",
                    e,
                )
            _log.info(
                "[PIPELINE_TRAY_BOOTSTRAP] trade_date=%s checks=%s",
                state.trade_date, checks,
            )
        except BootstrapError as be:
            _log.error("[PIPELINE_TRAY_BOOTSTRAP_FAIL] %s", be, exc_info=True)
        except Exception as e:  # noqa: BLE001
            _log.exception("[PIPELINE_TRAY_BOOTSTRAP_ERR] %s", e)

    def tick(self) -> Optional[dict]:
        """One scheduling pass. Returns summary dict or None if disabled."""
        orch = self.get()
        if orch is None:
            return None
        self._record_bootstrap(orch)
        summary = orch.tick()
        self._last_tick_summary = summary
        return summary

    @property
    def last_summary(self) -> Optional[dict]:
        return self._last_tick_summary


# Module-level singleton for tray_server's convenience. Tray can import
# and call `HOLDER.tick()` once per 30s scheduler iteration.
HOLDER = OrchestratorHolder()


def tick_if_enabled() -> Optional[dict]:
    """Convenience wrapper: call `HOLDER.tick()`.

    Tray integration is literally one line:
        from pipeline.tray_integration import tick_if_enabled
        tick_if_enabled()
    """
    try:
        return HOLDER.tick()
    except Exception as e:  # noqa: BLE001 — MUST NOT break tray loop
        _log.exception("[PIPELINE_TICK_WRAP_CRASH] %s", e)
        return None


def notify_if_enabled() -> list[str]:
    """Dispatch Telegram notifications for newly-terminal steps.

    Re-reads today's state from disk (cheap — JSON) and feeds it to the
    module-level PipelineNotifier, which dedupes via an internal set so
    repeated ticks don't re-emit.

    Returns list of step names for which a message was sent this call.
    Safe to call every tick; returns [] when toggle is off or on any
    error (never raises into the tray loop).
    """
    if not is_enabled():
        return []
    try:
        orch = HOLDER.get()
        if orch is None:
            return []
        state = PipelineState.load_or_create_today(
            data_dir=orch._data_dir,
            mode=orch._mode,
        )
        from .notify import get_notifier
        return get_notifier().notify_transitions(state)
    except Exception as e:  # noqa: BLE001 — MUST NOT break tray loop
        _log.warning("[PIPELINE_NOTIFY_WRAP_CRASH] %s", e)
        return []
