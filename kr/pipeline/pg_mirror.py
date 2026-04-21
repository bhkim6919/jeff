# -*- coding: utf-8 -*-
"""kr/pipeline/pg_mirror.py — Append-only PG mirror for pipeline state.

JSON at `kr/data/pipeline/state_YYYYMMDD.json` is the primary source of
truth (local, atomic, PG-independent). PG is a mirror: on each DONE /
FAILED / SKIPPED transition the orchestrator appends one row to
`pipeline_state_history` (schema v015) so dashboards, advisors, and
30-day analytics can query history without parsing JSON files.

Design decisions (open issue #1, Jeff-approved 2026-04-21):
  - Append-only: every step transition is its own row. Never UPDATE.
  - Non-blocking: mirror failures must never block orchestrator progress.
    `mirror_step(...)` catches every exception and returns False.
  - Lazy import: shared.db.pg_base is imported inside the function so the
    pipeline module can be imported in environments without psycopg2
    (e.g. pytest smoke, offline dev).

Usage:
    from pipeline.pg_mirror import mirror_step
    mirror_step(state, "batch")   # after state.mark_done / mark_failed
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from .state import PipelineState

_log = logging.getLogger("gen4.pipeline.pg_mirror")


def mirror_step(state: PipelineState, step_name: str) -> bool:
    """Append one row to `pipeline_state_history`.

    Parameters
    ----------
    state : PipelineState
        The in-memory state AFTER the transition. `state.step(step_name)`
        must reflect the new status.
    step_name : str
        The step whose transition we're recording.

    Returns
    -------
    bool
        True on successful insert, False on any failure (lazy-import
        error, PG down, SQL error, payload unserializable …). The caller
        MUST NOT treat False as a pipeline error — this is a mirror.
    """
    try:
        from shared.db.pg_base import connection  # noqa: WPS433 — lazy
    except Exception as e:  # psycopg2 missing, env vars missing, …
        _log.warning("[PIPELINE_PG_MIRROR_IMPORT_FAIL] %s", e)
        return False

    try:
        step = state.step(step_name)
        details_json = json.dumps(step.details or {}, ensure_ascii=False)
    except Exception as e:
        _log.warning("[PIPELINE_PG_MIRROR_SERIALIZE_FAIL] step=%s err=%s",
                     step_name, e)
        return False

    sql = """
        INSERT INTO pipeline_state_history (
            trade_date, mode, tz, step_name, status, fail_count,
            started_at, finished_at, last_error, details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    params = (
        state.trade_date,
        state.mode,
        state.tz,
        step_name,
        step.status,
        int(step.fail_count),
        step.started_at,
        step.finished_at,
        step.last_error,
        details_json,
    )

    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            cur.close()
        _log.debug("[PIPELINE_PG_MIRROR_OK] step=%s status=%s",
                   step_name, step.status)
        return True
    except Exception as e:
        _log.warning(
            "[PIPELINE_PG_MIRROR_INSERT_FAIL] step=%s err=%s",
            step_name, e,
        )
        return False


def load_recent_history(
    trade_date_from: "Optional" = None,
    trade_date_to: "Optional" = None,
    *,
    limit: int = 500,
) -> list[dict]:
    """Read-only helper for advisor / dashboard. Returns rows as dicts.

    Not used by the orchestrator itself — consumers only. Kept here so
    the mirror schema and query shape stay co-located.
    """
    try:
        from shared.db.pg_base import connection  # noqa: WPS433 — lazy
    except Exception as e:
        _log.warning("[PIPELINE_PG_MIRROR_IMPORT_FAIL] %s", e)
        return []

    where = []
    params: list = []
    if trade_date_from is not None:
        where.append("trade_date >= %s")
        params.append(trade_date_from)
    if trade_date_to is not None:
        where.append("trade_date <= %s")
        params.append(trade_date_to)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    params.append(int(limit))

    sql = f"""
        SELECT trade_date, mode, step_name, status, fail_count,
               started_at, finished_at, last_error, details, recorded_at
        FROM pipeline_state_history
        {where_sql}
        ORDER BY recorded_at DESC
        LIMIT %s
    """
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
        return rows
    except Exception as e:
        _log.warning("[PIPELINE_PG_MIRROR_READ_FAIL] %s", e)
        return []
