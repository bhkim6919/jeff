"""kr/pipeline/step_run_type_registry.py — Explicit step → run_type mapping.

Jeff A2 constraint 2: "step → run_type 매핑은 하드코딩보다 명시적 registry".
No inference from step names — a typo in a new step must NOT silently
bypass marker recording. Unmapped steps (bootstrap_env, ohlcv_sync,
gate_observer, backup) are orchestrator-internal and DO NOT surface to
operator-facing runs.

Also defines the canonical status derivation rule (Jeff A2 constraint 3):
    - exception / ok=False                  → FAILED
    - ok=True + details.partial=True        → PARTIAL
    - ok=True + any checks.* is False       → PARTIAL (auto-demoted; the
      marker invariant would reject SUCCESS with any_false anyway)
    - ok=True + no partial flags             → SUCCESS

This table is the ONLY place this mapping lives — marker_integration,
incident_writer, tests, and docs all import from here.

History note (2026-04-30):
    Recovered from ``.pyc`` after the source went missing alongside
    ``completion_schema.py`` and ``heartbeat.py`` (see PR #37 follow-up).
"""
from __future__ import annotations

from typing import Optional

from .completion_schema import (
    RUN_KR_BATCH,
    RUN_KR_EOD,
    RUN_US_EOD,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_SUCCESS,
)


# ── Step → run_type registry ─────────────────────────────────────────
# Operator-facing step names map to canonical RUN_* identifiers. Steps
# absent from this dict (bootstrap_env, ohlcv_sync, gate_observer,
# backup, …) are orchestrator-internal and do not produce markers.

STEP_TO_RUN_TYPE: dict[str, str] = {
    "batch":       RUN_KR_BATCH,
    "lab_eod_kr":  RUN_KR_EOD,
    "lab_eod_us":  RUN_US_EOD,
}

RUN_TYPE_TO_STEP: dict[str, str] = {v: k for k, v in STEP_TO_RUN_TYPE.items()}


def resolve_run_type(step_name: str) -> Optional[str]:
    """Return RUN_* for a step name, or None if the step is internal."""
    return STEP_TO_RUN_TYPE.get(step_name)


def derive_status(result_ok, details):
    """Canonical rule. Callers MUST use this, never ad-hoc logic.

    The PARTIAL auto-demotion ensures the marker invariant
    (``I1: SUCCESS forbids any checks.* == False``) is never violated
    by producer code attempting to pass a contradictory state.
    """
    if not result_ok:
        return STATUS_FAILED

    details = details if details else {}
    if details.get("partial") is True:
        return STATUS_PARTIAL

    checks = details.get("checks") if details else {}
    checks = checks if checks else {}
    if isinstance(checks, dict) and any(v is False for v in checks.values()):
        return STATUS_PARTIAL

    return STATUS_SUCCESS
