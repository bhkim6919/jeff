# -*- coding: utf-8 -*-
"""kr/pipeline/orchestrator.py — Pipeline tick-loop orchestrator (Phase 3).

Single event-driven scheduler that replaces the scattered time-window
triggers in tray_server (R-2). Each `tick()` call:

    1. Loads-or-creates today's PipelineState (atomic JSON).
    2. Walks registered steps in declared order.
    3. For each step: evaluates precondition → backoff gate → spawns
       run in a daemon thread (non-blocking). A step already running
       (status=PENDING) is never re-entered.
    4. Mirrors every transition to `pipeline_state_history` (best-effort).

The tick is idempotent and cheap — safe to call every 30s. All "has today
already finished?" logic is encoded in StepBase (already_done short-
circuit) so the orchestrator stays dumb.

Threading model:
    - Only one step per step-name can run at a time (guarded by
      `_running` dict + PENDING status marker in state).
    - Steps run in daemon threads; if the process exits the step is
      abandoned (next tick will see PENDING stale and re-arm after
      the orchestrator restart-sweep in `tick()`).
    - The stale-PENDING sweep re-classifies a PENDING step whose
      started_at is older than STALE_PENDING_SEC as FAILED so it can
      re-enter the backoff gate rather than wedge forever.

Design doc: kr/docs/PIPELINE_ORCHESTRATOR.md §3 Phase 3.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

from . import pg_mirror as _pg_mirror
from .schema import (
    MODE_PAPER_FORWARD,
    STATUS_PENDING,
)
from .state import PipelineState
from .steps.base import StepBase, StepRunResult

_log = logging.getLogger("gen4.pipeline.orchestrator")

# A step that has been PENDING for longer than this is assumed to have
# crashed or been killed during a prior run (daemon thread abandoned on
# process exit). Re-classify as FAILED so backoff can gate the retry.
STALE_PENDING_SEC = 30 * 60  # 30 minutes


class Orchestrator:
    """Event-driven pipeline runner. One tick = one scheduling pass."""

    def __init__(
        self,
        *,
        data_dir: Path,
        steps: list[StepBase],
        mode: str = MODE_PAPER_FORWARD,
        tz: str = "Asia/Seoul",
        clock: Optional[Callable[[], datetime]] = None,
        pg_mirror_fn: Optional[Callable[[PipelineState, str], bool]] = None,
        spawn_threads: bool = True,
    ):
        """Create an Orchestrator.

        Parameters
        ----------
        data_dir : Path
            Directory for `state_YYYYMMDD.json` files.
        steps : list[StepBase]
            Ordered list of step instances. Execution order follows this
            list, but precondition DAG still gates each entry.
        mode : str
            Pipeline mode tag (MODE_*). Used for state.mode.
        tz : str
            Timezone string for state.tz. Default Asia/Seoul.
        clock : Callable, optional
            Return current datetime. Default datetime.now (naive).
        pg_mirror_fn : Callable, optional
            Override for `pg_mirror.mirror_step`. Tests inject a no-op.
        spawn_threads : bool
            If False, run steps synchronously in tick() — useful for
            deterministic tests. Default True (daemon-thread spawn).
        """
        self._data_dir = Path(data_dir)
        self._steps = list(steps)
        self._mode = mode
        self._tz = tz
        self._clock = clock or datetime.now
        self._pg_mirror = pg_mirror_fn or _pg_mirror.mirror_step
        self._spawn_threads = bool(spawn_threads)
        self._lock = threading.Lock()
        # Tracks step-names currently running in a daemon thread so we
        # never double-spawn within a single process.
        self._running: set[str] = set()
        # Step name → instance for mirror-after-run dispatch
        self._by_name: dict[str, StepBase] = {s.name: s for s in self._steps}

    # ---------- public entry ----------

    def tick(self) -> dict:
        """One scheduling pass. Returns a summary dict for logging.

        Never raises. Every exception is caught and logged; the tick
        always completes with a result dict so callers can log the
        outcome deterministically.
        """
        summary: dict = {
            "trade_date": None,
            "mode": self._mode,
            "evaluated": [],
            "spawned": [],
            "skipped": [],
            "stale_swept": [],
            "errors": [],
        }
        try:
            state = PipelineState.load_or_create_today(
                data_dir=self._data_dir,
                mode=self._mode,
                tz=self._tz,
                clock=self._clock,
            )
            summary["trade_date"] = state.trade_date.strftime("%Y-%m-%d")

            # Sweep stale PENDING before making scheduling decisions
            swept = self._sweep_stale_pending(state)
            summary["stale_swept"] = swept
            if swept:
                state.save()

            for step in self._steps:
                summary["evaluated"].append(step.name)
                decision = self._evaluate_and_maybe_run(step, state)
                if decision == "spawned":
                    summary["spawned"].append(step.name)
                else:
                    summary["skipped"].append((step.name, decision))
        except Exception as e:  # noqa: BLE001
            _log.exception("[PIPELINE_TICK_CRASH] %s", e)
            summary["errors"].append(repr(e))

        _log.info(
            "[PIPELINE_TICK] date=%s spawned=%s skipped=%s swept=%s",
            summary["trade_date"],
            [s for s in summary["spawned"]],
            [n for n, _ in summary["skipped"]],
            summary["stale_swept"],
        )
        return summary

    # ---------- internals ----------

    def _sweep_stale_pending(self, state: PipelineState) -> list[str]:
        """Re-classify too-old PENDING entries as FAILED so retry gates work.

        A step is stale-PENDING if:
          - status == PENDING
          - started_at is older than STALE_PENDING_SEC
          - it is NOT currently in self._running (this process crashed
            before registering, or it's a different tray instance)

        Returns the list of swept step names.
        """
        swept: list[str] = []
        now = self._clock()
        for step in self._steps:
            st = state.steps.get(step.name)
            if st is None or st.status != STATUS_PENDING:
                continue
            if step.name in self._running:
                continue
            if st.started_at is None:
                # Safety: treat as stale immediately
                state.mark_failed(step.name, "stale_pending_no_started_at")
                swept.append(step.name)
                self._mirror_safe(state, step.name)
                continue
            age = (now - st.started_at).total_seconds()
            if age > STALE_PENDING_SEC:
                state.mark_failed(
                    step.name,
                    f"stale_pending_{int(age)}s",
                )
                swept.append(step.name)
                self._mirror_safe(state, step.name)
        return swept

    def _evaluate_and_maybe_run(self, step: StepBase, state: PipelineState) -> str:
        """Decide whether to spawn `step`. Returns one of:
            'spawned', 'already_running', 'already_done', 'precondition',
            'backoff', 'abandoned', 'unknown'
        """
        # Cheap guard: already in-flight in this process
        with self._lock:
            if step.name in self._running:
                return "already_running"

        # Already terminal today?
        st = state.step(step.name)
        if st.status in ("DONE", "SKIPPED"):
            return "already_done"
        # Another process may have marked PENDING; respect that
        if st.status == STATUS_PENDING:
            return "already_running"

        # Precondition chain
        ok, reason = step.precondition_met(state)
        if not ok:
            return f"precondition:{reason}"

        # Backoff + abandon gate (centralized in step's tracker)
        # We can't cheaply pre-check without importing BackoffTracker;
        # StepBase.run() re-checks these, so a false-positive here just
        # wastes a tick thread. Prefer to spawn and let StepBase decide.

        if self._spawn_threads:
            t = threading.Thread(
                target=self._run_and_mirror,
                args=(step,),
                name=f"pipeline-step-{step.name}",
                daemon=True,
            )
            with self._lock:
                self._running.add(step.name)
            t.start()
            return "spawned"

        # Synchronous mode (tests)
        self._run_and_mirror(step)
        return "spawned"

    def _run_and_mirror(self, step: StepBase) -> None:
        """Thread entry: reload fresh state, run step, mirror result."""
        try:
            # Reload state inside the worker — another tick/thread may
            # have updated it between dispatch and execution.
            state = PipelineState.load_or_create_today(
                data_dir=self._data_dir,
                mode=self._mode,
                tz=self._tz,
                clock=self._clock,
            )
            result = step.run(state)
            self._mirror_safe(state, step.name)
            _log.info(
                "[PIPELINE_STEP_OUTCOME] step=%s ok=%s skipped=%s err=%s",
                step.name, result.ok, result.skipped, result.error,
            )
        except Exception as e:  # noqa: BLE001 — defense in depth
            _log.exception("[PIPELINE_RUN_AND_MIRROR_CRASH] step=%s", step.name)
            # Mirror crash is best-effort; we can't persist without state
        finally:
            with self._lock:
                self._running.discard(step.name)

    def _mirror_safe(self, state: PipelineState, step_name: str) -> None:
        """Call PG mirror without propagating failures."""
        try:
            self._pg_mirror(state, step_name)
        except Exception as e:  # noqa: BLE001
            _log.warning(
                "[PIPELINE_PG_MIRROR_WRAP_FAIL] step=%s err=%s",
                step_name, e,
            )
