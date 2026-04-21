# -*- coding: utf-8 -*-
"""kr/pipeline/api.py — FastAPI router for Pipeline Orchestrator (Phase 4).

Two public endpoints:

    GET  /api/pipeline/status
        Current-day pipeline state + orchestrator flags. Used by the
        lab_live UI banner and by the advisor to replace its scattered
        "is batch done yet?" probes with a single authoritative read.

    POST /api/pipeline/record_step
        Fire-and-forget delegation hook for the live engine. The live
        engine MUST NOT import pipeline.state directly (Engine Protection
        rule — see CLAUDE.md). Instead it POSTs here when a step
        completes/fails/skips and the router writes state + mirrors PG.
        On any failure we return 200 with {"ok": false, "reason": ...}
        so a bad pipeline write can never break a live order flow.

Both endpoints degrade gracefully when the pipeline module is disabled
(QTRON_PIPELINE unset). Status returns a structured "disabled" payload;
record_step accepts the write and persists it anyway (the orchestrator
may not be ticking, but the state file + PG mirror still make sense for
observability).
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Optional

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel, Field

from .mode import detect_mode
from .schema import (
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_SKIPPED,
)
from .state import PipelineState
from .tray_integration import (
    HOLDER,
    default_data_dir,
    is_enabled,
    is_primary,
)

_log = logging.getLogger("gen4.pipeline.api")

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


# ---------- schemas ----------

class RecordStepBody(BaseModel):
    """Payload the live engine posts when a step transition happens."""
    step_name: str = Field(..., min_length=1, max_length=64)
    status: str = Field(..., description="DONE | FAILED | SKIPPED")
    details: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = Field(default=None, max_length=2000)
    trade_date: Optional[str] = Field(
        default=None,
        description="YYYY-MM-DD; defaults to today (KR trading day)",
    )


# ---------- helpers ----------

def _today_state() -> Optional[PipelineState]:
    try:
        data_dir = default_data_dir()
        mode = detect_mode()
        return PipelineState.load_or_create_today(
            data_dir=data_dir, mode=mode,
        )
    except Exception as e:  # noqa: BLE001 — API must never 500 on env issues
        _log.warning("[PIPELINE_API_LOAD_FAIL] %s", e)
        return None


def _parse_trade_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


# ---------- endpoints ----------

@router.get("/status")
async def get_status(
    include_history: bool = Query(
        default=False,
        description="If true, include the last 50 PG mirror rows",
    ),
):
    """Return today's pipeline state + orchestrator flags.

    Shape:
        {
          "enabled": bool,        # QTRON_PIPELINE set to non-zero
          "primary": bool,        # QTRON_PIPELINE >= 2
          "trade_date": "YYYY-MM-DD" | null,
          "mode": "paper_forward" | "live" | ...,
          "state": <PipelineState.to_dict()> | null,
          "last_tick": <last orchestrator tick summary> | null,
          "history": [ ... 50 most-recent PG rows ... ]   # only if requested
        }

    Never 500s. On any internal failure the offending field is null and
    an `error` key is included.
    """
    state = _today_state()
    state_dict = state.to_dict() if state is not None else None
    trade_date_str = (
        state.trade_date.strftime("%Y-%m-%d") if state is not None else None
    )
    mode_str = state.mode if state is not None else None

    payload: dict[str, Any] = {
        "enabled": is_enabled(),
        "primary": is_primary(),
        "trade_date": trade_date_str,
        "mode": mode_str,
        "state": state_dict,
        "last_tick": HOLDER.last_summary,
    }

    if include_history:
        try:
            from . import pg_mirror
            payload["history"] = pg_mirror.load_recent_history(limit=50)
        except Exception as e:  # noqa: BLE001
            _log.warning("[PIPELINE_API_HISTORY_FAIL] %s", e)
            payload["history"] = []
            payload["history_error"] = str(e)

    return payload


@router.post("/record_step")
async def record_step(body: RecordStepBody = Body(...)):
    """Accept a step transition from the live engine.

    Fire-and-forget from the caller's perspective: we always return 200.
    Response body distinguishes success (`ok: true`) from soft failure
    (`ok: false, reason: "..."`). The live engine should log the reason
    but must not retry aggressively — pipeline state is advisory for the
    order flow.
    """
    allowed = (STATUS_DONE, STATUS_FAILED, STATUS_SKIPPED)
    if body.status not in allowed:
        return {
            "ok": False,
            "reason": f"invalid_status:{body.status}",
            "allowed": list(allowed),
        }

    trade_date = _parse_trade_date(body.trade_date)
    try:
        data_dir = default_data_dir()
        mode = detect_mode()
        state = PipelineState.load_or_create_today(
            data_dir=data_dir,
            mode=mode,
            trade_date=trade_date,
        )
    except Exception as e:  # noqa: BLE001
        _log.exception("[PIPELINE_API_RECORD_LOAD_FAIL] %s", e)
        return {"ok": False, "reason": f"state_load_fail:{e}"}

    try:
        if body.status == STATUS_DONE:
            state.mark_done(body.step_name, details=body.details)
        elif body.status == STATUS_FAILED:
            state.mark_failed(
                body.step_name,
                body.error or "unspecified_error",
            )
        else:  # SKIPPED
            state.mark_skipped(
                body.step_name,
                body.error or (body.details.get("reason") if body.details else "skipped"),
            )
        state.save()
    except Exception as e:  # noqa: BLE001
        _log.exception("[PIPELINE_API_RECORD_WRITE_FAIL] %s", e)
        return {"ok": False, "reason": f"state_write_fail:{e}"}

    # PG mirror is best-effort and already swallows exceptions internally.
    try:
        from . import pg_mirror
        mirrored = pg_mirror.mirror_step(state, body.step_name)
    except Exception as e:  # noqa: BLE001 — belt and suspenders
        _log.warning("[PIPELINE_API_RECORD_MIRROR_FAIL] %s", e)
        mirrored = False

    _log.info(
        "[PIPELINE_API_RECORD_OK] step=%s status=%s mirrored=%s trade_date=%s",
        body.step_name, body.status, mirrored, state.trade_date,
    )
    return {
        "ok": True,
        "step_name": body.step_name,
        "status": body.status,
        "trade_date": state.trade_date.strftime("%Y-%m-%d"),
        "mirrored": mirrored,
    }
