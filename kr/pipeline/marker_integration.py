# -*- coding: utf-8 -*-
"""kr/pipeline/marker_integration.py — StepBase ↔ completion marker glue.

Connects pipeline step execution to the canonical completion marker.
Invoked from StepBase.run() at two points:

    record_start(state, step_name)
        — called right after state.mark_started(). Transitions the
          marker from MISSING→RUNNING (or keeps RUNNING if the step
          is retrying within the same attempt).

    record_result(state, step_name, result)
        — called from a finally block that covers every exit path
          after mark_started (crash / bad return / skipped / ok / fail).
          Translates the StepRunResult to a terminal marker status via
          step_run_type_registry.derive_status() and transitions.

Jeff A2 constraints enforced here:
    1. Any marker write failure is swallowed — step return value is NEVER
       altered by marker integration. Step exceptions never swap with
       marker exceptions.
    2. Unmapped steps (bootstrap_env etc.) are silent no-ops.
    3. Status comes from the registry's derive_status() alone.
    4. incident_writer is triggered here ONLY on marker terminal-failure
       transitions, not on raw step results (ties incident lifecycle to
       the marker's state machine).

Data_dir discovery:
    The step receives a PipelineState whose .data_dir is the pipeline
    JSON directory. The marker lives in the SAME directory. This avoids
    any new config.

Re-entry note:
    A step in BackoffTracker 'abandoned' state does not reach this module
    — StepBase short-circuits before mark_started. So we never attempt
    an invalid transition on top of an abandoned marker.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from .completion_marker import (
    ChecksBlock,
    CompletionMarker,
    ErrorBlock,
    MarkerInvariantError,
    MetricsBlock,
)
from .completion_schema import (
    ALL_STATUSES,
    STATUS_FAILED,
    STATUS_MISSING,
    STATUS_PARTIAL,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    TERMINAL_STATUSES,
)
from .state import PipelineState
from .step_run_type_registry import derive_status, resolve_run_type

_log = logging.getLogger("gen4.pipeline.marker_integration")

# Statuses that trigger incident writing. Kept as a named set so tests
# and incident_writer can import the same definition.
from .completion_schema import (
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
)

INCIDENT_TRIGGER_STATUSES = frozenset({
    STATUS_FAILED,
    STATUS_PARTIAL,
    # Phase B adds preflight-originated blocks
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
})


def record_start(state: PipelineState, step_name: str) -> None:
    """Called after state.mark_started(). Transition marker to RUNNING.

    Idempotent for same-attempt retries (FAILED → RUNNING is same_attempt).
    Silent no-op for steps not in STEP_TO_RUN_TYPE.
    """
    run_type = resolve_run_type(step_name)
    if run_type is None:
        return
    try:
        marker = CompletionMarker.load_or_create_today(
            data_dir=state.data_dir, trade_date=state.trade_date)
        current = marker.run(run_type).status
        if current == STATUS_RUNNING:
            # Concurrent second start? Unlikely (orchestrator single-flight)
            # but safe: touch last_update and return.
            marker.record_heartbeat()
            marker.save()
            return
        # MISSING, FAILED, PARTIAL, PRE_FLIGHT_* → RUNNING
        # SUCCESS → RUNNING is a new_attempt transition (same-day re-execution
        # is extremely rare in current orchestrator but marker supports it).
        marker.transition(run_type, STATUS_RUNNING)
        marker.save()
    except Exception as e:  # noqa: BLE001 — marker must never break step
        _log.critical(
            "[MARKER_RECORD_START_FAIL] step=%s run_type=%s err=%r",
            step_name, run_type, e,
        )


def record_result(
    state: PipelineState, step_name: str, result: Any,
) -> None:
    """Called from StepBase.run() finally block. Writes terminal status.

    `result` may be a StepRunResult OR None (if exception path constructed
    no result — defensive). In the None case we treat as FAILED.
    Unmapped step → no-op.
    """
    run_type = resolve_run_type(step_name)
    if run_type is None:
        return

    # Normalize result → (ok, details, error_msg)
    if result is None:
        ok, details, err_msg = False, {}, "no_result_constructed"
        skipped = False
    else:
        ok = bool(getattr(result, "ok", False))
        details = dict(getattr(result, "details", {}) or {})
        err_msg = getattr(result, "error", None)
        skipped = bool(getattr(result, "skipped", False))

    # Skipped steps (precondition/time-window/backoff gated) do NOT
    # transition marker. They never reached RUNNING in the marker because
    # record_start wasn't called — but in case a subclass returns skipped=True
    # after mark_started ran, we back out safely.
    if skipped:
        _log.debug("[MARKER_SKIP_TRANSITION] step=%s (skipped=True)", step_name)
        return

    to_status = derive_status(ok, details)
    if to_status not in ALL_STATUSES:
        _log.error(
            "[MARKER_BAD_STATUS] step=%s derived=%r — coercing to FAILED",
            step_name, to_status,
        )
        to_status = STATUS_FAILED

    try:
        marker = CompletionMarker.load_or_create_today(
            data_dir=state.data_dir, trade_date=state.trade_date)

        # Guarantee we're in RUNNING before terminal transition. If the
        # start hook failed silently, the marker may still be MISSING —
        # force MISSING→RUNNING→terminal rather than skip.
        if marker.run(run_type).status == STATUS_MISSING:
            marker.transition(run_type, STATUS_RUNNING)

        checks_block = _extract_checks(details)
        metrics_block = _extract_metrics(details)
        artifacts_block = _extract_artifacts(details)
        error_block = None
        if to_status == STATUS_FAILED:
            error_block = ErrorBlock(
                stage=step_name,
                message=str(err_msg or "unknown_failure")[:2000],
                trace_ref=details.get("trace_ref"),
            )

        marker.transition(
            run_type, to_status,
            checks=checks_block,
            metrics=metrics_block,
            artifacts=artifacts_block,
            error=error_block,
            snapshot_version=details.get("snapshot_version"),
        )
        marker.save()

        if to_status in INCIDENT_TRIGGER_STATUSES:
            _emit_incident(marker, run_type, state.data_dir)

    except MarkerInvariantError as e:
        # Schema violation in our own call — bug, log loud but don't
        # propagate (step result already recorded elsewhere).
        _log.critical(
            "[MARKER_INVARIANT_VIOLATED] step=%s run_type=%s err=%s",
            step_name, run_type, e,
        )
    except Exception as e:  # noqa: BLE001
        _log.critical(
            "[MARKER_RECORD_RESULT_FAIL] step=%s run_type=%s err=%r",
            step_name, run_type, e,
        )


# ---------- Helpers ----------

def _extract_checks(details: dict) -> Optional[ChecksBlock]:
    """Pull the `checks` sub-dict out of step result details, if present.

    Steps opt-in to fine-grained checks by putting a dict at details['checks'].
    Missing / non-dict → None (marker keeps prior value).
    """
    raw = details.get("checks")
    if not isinstance(raw, dict):
        return None
    return ChecksBlock(
        imports_ok=raw.get("imports_ok"),
        db_upsert_ok=raw.get("db_upsert_ok"),
        kospi_parse_ok=raw.get("kospi_parse_ok"),
        report_ok=raw.get("report_ok"),
        head_updated=raw.get("head_updated"),
        write_perm_ok=raw.get("write_perm_ok"),
        universe_healthy=raw.get("universe_healthy"),
    )


def _extract_metrics(details: dict) -> Optional[MetricsBlock]:
    """R6 (2026-04-24): Pull numeric metrics from step details.

    Steps opt-in by setting details['metrics'] = {universe_count: N, ...}.
    Missing / non-dict → None (marker keeps prior value).
    """
    raw = details.get("metrics")
    if not isinstance(raw, dict):
        return None
    return MetricsBlock.from_dict(raw)


def _extract_artifacts(details: dict):
    """Pull artifact paths if the step supplies them.

    Steps may set details['artifacts'] = {log_path, report_path, head_last_run_date}.
    Missing → None (marker keeps prior value).
    """
    raw = details.get("artifacts")
    if not isinstance(raw, dict):
        return None
    from .completion_marker import ArtifactsBlock
    return ArtifactsBlock(
        log_path=raw.get("log_path"),
        report_path=raw.get("report_path"),
        head_last_run_date=raw.get("head_last_run_date"),
    )


def record_stale_sweep(
    state: PipelineState, step_name: str, age_sec: int,
    *, daemon_alive: bool = False,
) -> None:
    """R15 (2026-04-23) — Sync marker with orchestrator stale sweep.

    Called from orchestrator._sweep_stale_pending when a PENDING step is
    forcefully transitioned to FAILED in state. Mirrors that change to
    the marker so:
      - worst_status_today reflects the failure
      - incident is generated
      - downstream consumers see FAILED instead of stuck-RUNNING

    For unmapped steps (bootstrap_env etc.) this is a no-op.

    Never propagates exceptions — stale sweep is a best-effort recovery
    path and must not itself crash.
    """
    run_type = resolve_run_type(step_name)
    if run_type is None:
        return

    try:
        marker = CompletionMarker.load_or_create_today(
            data_dir=state.data_dir, trade_date=state.trade_date,
        )
        entry = marker.run(run_type)
        # Only transition if currently RUNNING (don't re-mark terminals)
        if entry.status != STATUS_RUNNING:
            _log.debug(
                "[MARKER_STALE_SKIP] run_type=%s already %s", run_type, entry.status,
            )
            return

        kind = "force_sweep_daemon_hung" if daemon_alive else "stale_pending"
        error_msg = f"{kind}: {age_sec}s without completion"
        marker.transition(
            run_type, STATUS_FAILED,
            error=ErrorBlock(
                stage="stale_sweep",
                message=error_msg,
                trace_ref=f"orchestrator.daemon_alive={daemon_alive}",
            ),
        )
        marker.save()
        _log.critical(
            "[MARKER_STALE_SYNC] run_type=%s daemon_alive=%s age=%ds",
            run_type, daemon_alive, age_sec,
        )

        # Fire incident for visibility
        _emit_incident(marker, run_type, state.data_dir)

    except MarkerInvariantError as e:
        _log.critical(
            "[MARKER_STALE_INVARIANT] run_type=%s err=%s", run_type, e,
        )
    except Exception as e:  # noqa: BLE001
        _log.critical(
            "[MARKER_STALE_FAIL] run_type=%s err=%r", run_type, e,
        )


def _emit_incident(marker: CompletionMarker, run_type: str, data_dir) -> None:
    """Defer to incident_writer; swallow all errors."""
    try:
        from . import incident_writer
        incident_writer.write_if_new(marker, run_type, data_dir=data_dir)
    except Exception as e:  # noqa: BLE001
        _log.warning("[INCIDENT_EMIT_FAIL] run_type=%s err=%r", run_type, e)
