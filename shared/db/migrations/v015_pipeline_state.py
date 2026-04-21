# -*- coding: utf-8 -*-
"""v015: Pipeline state history mirror (Pipeline Orchestrator Phase 2).

Background (Jeff-approved open issue #1, 2026-04-21):
  Pipeline Orchestrator uses `kr/data/pipeline/state_YYYYMMDD.json` as the
  primary source of truth (atomic, crash-safe, independent of PG). PG is a
  mirror for historical queries (30-day backtests, dashboard rollups,
  advisor gate checks).

Write pattern:
  On each step transition (DONE / FAILED / SKIPPED) the orchestrator
  appends one row. No UPDATE — every event is append-only so we can
  reconstruct the full timeline of a day's pipeline.

Query patterns:
  - Most recent step status per day:
      SELECT DISTINCT ON (trade_date, step_name) *
      FROM pipeline_state_history
      WHERE trade_date BETWEEN ... ORDER BY trade_date DESC, step_name,
            recorded_at DESC
  - Step duration analytics:
      SELECT step_name, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
             EXTRACT(EPOCH FROM (finished_at - started_at)))
      FROM pipeline_state_history WHERE status='DONE'
      GROUP BY step_name
  - Abandoned-step detection:
      SELECT trade_date, step_name FROM pipeline_state_history
      WHERE fail_count >= 3 GROUP BY trade_date, step_name

Design doc: kr/docs/PIPELINE_ORCHESTRATOR.md
Impl plan:  kr/docs/PIPELINE_ORCHESTRATOR_PLAN.md (§2.1, §3 Phase 2)
"""
VERSION = 15
DESCRIPTION = "Pipeline state history mirror (orchestrator Phase 2)"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_state_history (
        id            BIGSERIAL PRIMARY KEY,
        trade_date    DATE NOT NULL,
        mode          TEXT NOT NULL
            CHECK (mode IN ('live','paper_forward','lab','backtest')),
        tz            TEXT NOT NULL DEFAULT 'Asia/Seoul',
        step_name     TEXT NOT NULL,
        status        TEXT NOT NULL
            CHECK (status IN (
                'NOT_STARTED','PENDING','DONE','FAILED','SKIPPED'
            )),
        fail_count    INTEGER NOT NULL DEFAULT 0,
        started_at    TIMESTAMPTZ,
        finished_at   TIMESTAMPTZ,
        last_error    TEXT,
        details       JSONB NOT NULL DEFAULT '{}'::jsonb,
        recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)

    # Common query: "what's today's pipeline state?"
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pipeline_state_history_date_step
        ON pipeline_state_history(trade_date DESC, step_name, recorded_at DESC)
    """)
    # Common query: "which steps failed recently?"
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pipeline_state_history_status
        ON pipeline_state_history(status, recorded_at DESC)
        WHERE status IN ('FAILED','SKIPPED')
    """)
    # JSONB details search (snapshot_version, step details)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_pipeline_state_history_details
        ON pipeline_state_history USING GIN (details)
    """)

    cur.close()
